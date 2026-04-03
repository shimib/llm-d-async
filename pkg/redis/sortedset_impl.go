package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/config"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"

	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const SORTEDSET_QUEUE_NAME_KEY = "queue_name"

// parseGateParams parses a JSON-encoded string
// into a map[string]string for gate parameter configuration.
func parseGateParams(s string) map[string]string {
	m := map[string]string{}
	if s == "" || s == "{}" {
		return m
	}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

type queueConfig struct {
	QueueName          string            `json:"queue_name"`
	InferenceObjective string            `json:"inference_objective"`
	RequestPathURL     string            `json:"request_path_url"`
	IGWBaseURl         string            `json:"igw_base_url"`
	GateType           string            `json:"gate_type"`
	GateParams         map[string]string `json:"gate_params,omitempty"`
}

type requestChannelData struct {
	channel   api.RequestChannel
	queueName string
	gate      api.DispatchGate
}

type RedisSortedSetFlow struct {
	rdb             *redis.Client
	requestChannels []requestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
	cfg             config.RedisSortedSetConfig
	gate            api.DispatchGate
	gateFactory     api.GateFactory
}

// SortedSetOption is a functional option for configuring RedisSortedSetFlow
type SortedSetOption func(*RedisSortedSetFlow)

// WithGateFactory sets a GateFactory for per-queue gate instantiation.
func WithGateFactory(factory api.GateFactory) SortedSetOption {
	return func(r *RedisSortedSetFlow) {
		r.gateFactory = factory
	}
}

func NewRedisSortedSetFlow(cfg config.RedisSortedSetConfig, opts ...SortedSetOption) *RedisSortedSetFlow {
	configs := loadQueueConfigs(cfg)
	r := &RedisSortedSetFlow{
		rdb: redis.NewClient(&redis.Options{
			Addr:     cfg.Conn.Addr,
			Username: cfg.Conn.User,
			Password: cfg.Conn.Password,
		}),
		requestChannels: make([]requestChannelData, 0, len(configs)),
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
		cfg:             cfg,
	}

	// Apply functional options
	for _, opt := range opts {
		opt(r)
	}

	// Create per-queue channels with gates
	for _, qcfg := range configs {
		// Determine gate for this queue
		var gate api.DispatchGate
		if r.gateFactory != nil && qcfg.GateType != "" {
			// Use factory to create per-queue gate
			var err error
			gate, err = r.gateFactory.CreateGate(qcfg.GateType, qcfg.GateParams)
			if err != nil {
				panic(fmt.Sprintf("failed to create gate for queue %q (gate_type=%q): %v", qcfg.QueueName, qcfg.GateType, err))
			}
		} else if r.gate != nil {
			// Fall back to global gate if provided
			gate = r.gate
		} else {
			// Default to always-open gate
			gate = api.ConstOpenGate()
		}

		ch := api.RequestChannel{
			Channel:            make(chan api.RequestMessage),
			InferenceObjective: qcfg.InferenceObjective,
			RequestPathURL:     util.NormalizeURLPath(qcfg.RequestPathURL),
			IGWBaseURl:         util.NormalizeBaseURL(qcfg.IGWBaseURl),
			Gate:               gate,
		}

		r.requestChannels = append(r.requestChannels, requestChannelData{
			channel:   ch,
			queueName: qcfg.QueueName,
			gate:      gate,
		})
	}

	// Set default gate if not already set
	if r.gate == nil {
		r.gate = api.ConstOpenGate()
	}

	return r
}

func loadQueueConfigs(cfg config.RedisSortedSetConfig) []queueConfig {
	if cfg.QueuesConfigFile != "" {
		data, err := os.ReadFile(cfg.QueuesConfigFile)
		if err != nil {
			panic(fmt.Sprintf("failed to read config file: %v", err))
		}
		var configs []queueConfig
		if err := json.Unmarshal(data, &configs); err != nil {
			panic(fmt.Sprintf("failed to unmarshal config file: %v", err))
		}
		return configs
	}
	// Single-queue mode
	return []queueConfig{{
		QueueName:          cfg.RequestQueueName,
		InferenceObjective: cfg.InferenceObjective,
		RequestPathURL:     cfg.RequestPathURL,
		IGWBaseURl:         cfg.IGWBaseURL,
		GateType:           cfg.GateType,
		GateParams:         parseGateParams(cfg.GateParamsJSON),
	}}
}

func (r *RedisSortedSetFlow) Start(ctx context.Context) {
	for _, ch := range r.requestChannels {
		go r.requestWorker(ctx, ch.channel.Channel, ch.queueName)
	}
	go r.retryWorker(ctx)
	go r.resultWorker(ctx)
}

func (r *RedisSortedSetFlow) RequestChannels() []api.RequestChannel {
	channels := make([]api.RequestChannel, len(r.requestChannels))
	for i, ch := range r.requestChannels {
		channels[i] = ch.channel
	}
	return channels
}

func (r *RedisSortedSetFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *RedisSortedSetFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *RedisSortedSetFlow) Characteristics() api.Characteristics {
	return api.Characteristics{HasExternalBackoff: false, SupportsMessageLatency: false}
}

// Polls sorted set and processes messages by deadline priority (earliest first)
func (r *RedisSortedSetFlow) requestWorker(ctx context.Context, msgChannel chan api.RequestMessage, queueName string) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(time.Duration(r.cfg.PollIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	// Find the gate for this queue
	var gate api.DispatchGate
	for _, ch := range r.requestChannels {
		if ch.queueName == queueName {
			gate = ch.gate
			break
		}
	}
	if gate == nil {
		gate = r.gate
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processMessages(ctx, msgChannel, queueName, gate, logger)
		}
	}
}

func (r *RedisSortedSetFlow) processMessages(ctx context.Context, msgChannel chan api.RequestMessage, queueName string, gate api.DispatchGate, logger logr.Logger) {
	currentTime := float64(time.Now().Unix())

	budget := gate.Budget(ctx)
	batchSize := int(math.Floor(float64(r.cfg.BatchSize) * budget))

	for i := 0; i < batchSize; i++ {
		results, err := r.rdb.ZPopMin(ctx, queueName, 1).Result()
		if err == redis.Nil || len(results) == 0 {
			break
		}
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to pop from sorted set")
			break
		}

		msg, deadline, ok := r.parseMessage(results[0], logger)
		if !ok {
			continue
		}

		if deadline < currentTime {
			logger.V(logutil.DEFAULT).Info("Deadline expired", "id", msg.Id)
			continue
		}

		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string)
		}
		msg.Metadata[SORTEDSET_QUEUE_NAME_KEY] = queueName

		select {
		case msgChannel <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (r *RedisSortedSetFlow) parseMessage(z redis.Z, logger logr.Logger) (api.RequestMessage, float64, bool) {
	var msg api.RequestMessage
	if err := json.Unmarshal([]byte(z.Member.(string)), &msg); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message")
		return msg, 0, false
	}

	deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Invalid deadline", "id", msg.Id)
		return msg, 0, false
	}

	return msg, float64(deadline), true
}

// Re-queues failed messages with exponential backoff
func (r *RedisSortedSetFlow) retryWorker(ctx context.Context) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-r.retryChannel:
			queueName := msg.Metadata[SORTEDSET_QUEUE_NAME_KEY]
			if queueName == "" {
				queueName = r.cfg.RequestQueueName
			}

			bytes, err := json.Marshal(msg.RequestMessage)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to marshal retry")
				continue
			}

			retryScore := float64(time.Now().Unix()) + msg.BackoffDurationSeconds
			if err := r.rdb.ZAdd(ctx, queueName, redis.Z{Score: retryScore, Member: string(bytes)}).Err(); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to add retry")
			}
		}
	}
}

// Pushes results to Redis list (FIFO)
// Routes results to the queue specified in request metadata, or default queue if not specified
func (r *RedisSortedSetFlow) resultWorker(ctx context.Context) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-r.resultChannel:
			// Check metadata for custom result queue (set by producer)
			resultQueue := r.cfg.ResultQueueName // default queue
			if msg.Metadata != nil {
				if customQueue, ok := msg.Metadata["result_queue"]; ok && customQueue != "" {
					resultQueue = customQueue
				}
			}

			msgStr := r.marshalResult(msg)
			if err := r.rdb.LPush(ctx, resultQueue, msgStr).Err(); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to push result", "queue", resultQueue)
			} else {
				logger.V(logutil.DEBUG).Info("Pushed result to queue", "id", msg.Id, "queue", resultQueue)
			}
		}
	}
}

func (r *RedisSortedSetFlow) marshalResult(msg api.ResultMessage) string {
	if bytes, err := json.Marshal(msg); err == nil {
		return string(bytes)
	}
	fallback := map[string]string{"id": msg.Id, "payload": `{"error":"marshal failed"}`}
	fallbackBytes, _ := json.Marshal(fallback)
	return string(fallbackBytes)
}
