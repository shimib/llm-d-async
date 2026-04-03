package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/config"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {

	var configFile string
	flag.StringVar(&configFile, "config", "", "Path to the configuration file")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Printf("failed to load config: %v\n", err)
		os.Exit(1)
	}

	logging.InitLogging(&opts, cfg.LogLevel)
	defer logging.Sync() // nolint:errcheck

	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("Logger initialized")

	printAllFlags(setupLog)
	// Create Gate Factory for per-queue gate instantiation
	gateFactory := flowcontrol.NewGateFactory(cfg)

	var policy api.RequestMergePolicy
	switch cfg.RequestMergePolicy {
	case "random-robin":
		policy = async.NewRandomRobinPolicy()
	default:
		setupLog.Error(fmt.Errorf("unknown request merge policy: %s", cfg.RequestMergePolicy), "Unknown request merge policy", "request-merge-policy",
			cfg.RequestMergePolicy)
		os.Exit(1)
	}
	var impl api.Flow
	switch cfg.MessageQueueImpl {
	case "redis-pubsub":
		impl = redis.NewRedisMQFlow(cfg.Redis)
	case "redis-sortedset":
		impl = redis.NewRedisSortedSetFlow(cfg.RedisSortedSet, redis.WithGateFactory(gateFactory))
		setupLog.Info("Using Redis sorted-set flow with per-queue gating")
	case "gcp-pubsub":
		impl = pubsub.NewGCPPubSubMQFlow(cfg.PubSub)
	case "gcp-pubsub-gated":
		impl = pubsub.NewGCPPubSubMQFlow(cfg.PubSub, pubsub.WithGateFactory(gateFactory))
		setupLog.Info("Using GCP PubSub flow with per-queue gating")
	default:
		setupLog.Error(fmt.Errorf("unknown message queue implementation: %s", cfg.MessageQueueImpl), "Unknown message queue implementation",
			"message-queue-impl", cfg.MessageQueueImpl)
		os.Exit(1)
	}

	metrics.Register(metrics.GetAsyncProcessorCollectors(impl.Characteristics().SupportsMessageLatency)...)

	ctx := ctrl.SetupSignalHandler()

	metricsServerOptions := metricsserver.Options{
		BindAddress: fmt.Sprintf(":%d", cfg.MetricsPort),
		FilterProvider: func() func(c *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
			if cfg.MetricsEndpointAuth {
				return filters.WithAuthenticationAndAuthorization
			}

			return nil
		}(),
	}
	restConfig := ctrl.GetConfigOrDie()
	httpClient := http.DefaultClient

	msrv, _ := metricsserver.NewServer(metricsServerOptions, restConfig, httpClient)
	go msrv.Start(ctx) // nolint:errcheck

	// Create inference client
	inferenceClient := api.NewHTTPInferenceClient(httpClient)

	requestChannel := policy.MergeRequestChannels(impl.RequestChannels()).Channel
	for w := 1; w <= cfg.Concurrency; w++ {

		go api.Worker(ctx, impl.Characteristics(), inferenceClient, requestChannel, impl.RetryChannel(), impl.ResultChannel())
	}

	impl.Start(ctx)
	<-ctx.Done()
}

func printAllFlags(setupLog logr.Logger) {
	flags := make(map[string]any)
	flag.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f.Value
	})
	setupLog.Info("Flags processed", "flags", flags)
}
