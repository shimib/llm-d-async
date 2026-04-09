package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/llm-d-incubation/llm-d-async/pkg/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	var loggerVerbosity int

	var metricsPort int
	var metricsAuthToken string

	var concurrency int
	var requestMergePolicy string
	var messageQueueImpl string

	flag.IntVar(&loggerVerbosity, "v", logging.DEFAULT, "number for the log level verbosity")

	flag.IntVar(&metricsPort, "metrics-port", 9090, "The metrics port")
	flag.StringVar(&metricsAuthToken, "metrics-auth-token", "", "The Bearer token for metrics endpoint authentication (disabled if empty)")

	flag.IntVar(&concurrency, "concurrency", 8, "number of concurrent workers")

	flag.StringVar(&requestMergePolicy, "request-merge-policy", "random-robin", "The request merge policy to use. Supported policies: random-robin")
	flag.StringVar(&messageQueueImpl, "message-queue-impl", "redis-pubsub", "The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, gcp-pubsub, gcp-pubsub-gated")

	var prometheusURL = flag.String("prometheus-url", "", "Prometheus server URL for metric-based gates (e.g., http://localhost:9090)")

	opts := logging.Options{
		Development: true,
	}

	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logging.InitLogging(&opts, loggerVerbosity)
	defer logging.Sync() // nolint:errcheck

	setupLog := logging.Log.WithName("setup")
	setupLog.Info("Logger initialized")

	setupLog.Info("Async Processor starting", "version", version.Version, "commit", version.Commit, "buildDate", version.BuildDate)

	printAllFlags(setupLog)
	// Create Gate Factory for per-queue gate instantiation
	gateFactory := flowcontrol.NewGateFactory(*prometheusURL)

	var policy api.RequestMergePolicy
	switch requestMergePolicy {
	case "random-robin":
		policy = async.NewRandomRobinPolicy()
	default:
		setupLog.Error(fmt.Errorf("unknown request merge policy: %s", requestMergePolicy), "Unknown request merge policy", "request-merge-policy",
			requestMergePolicy)
		os.Exit(1)
	}
	var impl api.Flow
	switch messageQueueImpl {
	case "redis-pubsub":
		impl = redis.NewRedisMQFlow()
	case "redis-sortedset":
		impl = redis.NewRedisSortedSetFlow(redis.WithGateFactory(gateFactory))
		setupLog.Info("Using Redis sorted-set flow with per-queue gating")
	case "gcp-pubsub":
		impl = pubsub.NewGCPPubSubMQFlow()
	case "gcp-pubsub-gated":
		impl = pubsub.NewGCPPubSubMQFlow(pubsub.WithGateFactory(gateFactory))
		setupLog.Info("Using GCP PubSub flow with per-queue gating")
	default:
		setupLog.Error(fmt.Errorf("unknown message queue implementation: %s", messageQueueImpl), "Unknown message queue implementation",
			"message-queue-impl", messageQueueImpl)
		os.Exit(1)
	}

	metrics.Register(metrics.GetAsyncProcessorCollectors(impl.Characteristics().SupportsMessageLatency)...)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Register metrics handler.
	mux := http.NewServeMux()
	metricsHandler := promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{})
	if metricsAuthToken != "" {
		setupLog.Info("Metrics authentication enabled")
		metricsHandler = withAuth(metricsAuthToken, metricsHandler)
	}
	mux.Handle("/metrics", metricsHandler)
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", metricsPort),
		Handler: mux,
	}
	go func() {
		setupLog.Info("Starting metrics server", "addr", metricsServer.Addr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			setupLog.Error(err, "Failed to start metrics server")
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			setupLog.Error(err, "Failed to shutdown metrics server")
		}
	}()

	httpClient := http.DefaultClient

	// Create inference client
	inferenceClient := api.NewHTTPInferenceClient(httpClient)

	requestChannel := policy.MergeRequestChannels(impl.RequestChannels()).Channel
	for w := 1; w <= concurrency; w++ {

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

func withAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer "+token {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
