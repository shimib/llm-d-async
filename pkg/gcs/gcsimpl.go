package gcs

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
	"google.golang.org/api/iterator"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const GCS_PUBSUB_ID = "gcs-pubsub-id"
const GCS_OBJECT_NAME = "gcs-object-name"

var (
	gcsProjectID          = flag.String("gcs.project-id", "", "GCP project ID for GCS and PubSub")
	gcsRequestBucket      = flag.String("gcs.request-bucket", "", "GCS bucket for incoming requests")
	gcsResultBucket       = flag.String("gcs.result-bucket", "", "GCS bucket for results")
	gcsNotificationSub    = flag.String("gcs.notification-subscription", "", "PubSub subscription ID for GCS notifications")
	gcsBootstrapPrefix    = flag.String("gcs.bootstrap-prefix", "new/", "Prefix for initial bootstrap scanning")
	gcsProcessingPrefix   = flag.String("gcs.processing-prefix", "processing/", "Prefix for messages currently being processed")
	gcsIGWBaseURL         = flag.String("gcs.igw-base-url", "", "Base URL for IGW.")
	gcsRequestPathURL     = flag.String("gcs.request-path-url", "/v1/completions", "inference request path url.")
	gcsInferenceObjective = flag.String("gcs.inference-objective", "", "inference objective to use in requests.")
	gcsBatchSize          = flag.Int("gcs.batch-size", 10, "Number of inflight messages")
	gcsPollInterval       = flag.Int("gcs.poll-interval-ms", 0, "If greater than 0, enables periodic polling instead of one-time bootstrap (useful for testing)")

	gcsResultChannels sync.Map
)

// GCSNotification represents the payload sent by GCS to PubSub
type GCSNotification struct {
	Name       string `json:"name"`
	Bucket     string `json:"bucket"`
	Generation int64  `json:"generation,string"`
}

type GCSMQFlow struct {
	requestChannels []RequestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
	gate            api.DispatchGate
	gcsClient       ClientWrapper
	pubsubClient    *pubsub.Client
}

type RequestChannelData struct {
	requestChannel api.RequestChannel
	subscriberID   string
	gate           api.DispatchGate
}

func NewGCSMQFlow(ctx context.Context) *GCSMQFlow {
	gcsWrapper, err := NewClientWrapper(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to create gcs client: %v", err))
	}

	psClient, err := pubsub.NewClient(ctx, *gcsProjectID)
	if err != nil {
		panic(fmt.Sprintf("failed to create pubsub client: %v", err))
	}

	return NewGCSMQFlowWithClients(gcsWrapper, psClient)
}

func NewGCSMQFlowWithClients(gcsClient ClientWrapper, psClient *pubsub.Client) *GCSMQFlow {
	gate := api.ConstOpenGate()

	ch := make(chan api.RequestMessage)
	requestChanData := RequestChannelData{
		requestChannel: api.RequestChannel{
			Channel:            ch,
			IGWBaseURl:         util.NormalizeBaseURL(*gcsIGWBaseURL),
			InferenceObjective: *gcsInferenceObjective,
			RequestPathURL:     util.NormalizeURLPath(*gcsRequestPathURL),
			Gate:               gate,
		},
		subscriberID: *gcsNotificationSub,
		gate:         gate,
	}

	p := &GCSMQFlow{
		requestChannels: []RequestChannelData{requestChanData},
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
		gate:            gate,
		gcsClient:       gcsClient,
		pubsubClient:    psClient,
	}

	return p
}

func (r *GCSMQFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *GCSMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *GCSMQFlow) Characteristics() api.Characteristics {
	return api.Characteristics{
		HasExternalBackoff:     true,
		SupportsMessageLatency: true,
	}
}

func (r *GCSMQFlow) RequestChannels() []api.RequestChannel {
	var channels []api.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
	}
	return channels
}

func (r *GCSMQFlow) Start(ctx context.Context) {
	for _, channelData := range r.requestChannels {
		go r.notificationWorker(ctx, channelData.subscriberID, channelData.requestChannel.Channel, channelData.gate)
		// Start bootstrap scanner
		go r.bootstrapScanner(ctx, channelData.requestChannel.Channel)
	}
	go r.resultWorker(ctx)
	go r.retryQueueWorker(ctx)
}

func (r *GCSMQFlow) bootstrapScanner(ctx context.Context, ch chan api.RequestMessage) {
	logger := log.FromContext(ctx)
	
	pollDuration := time.Duration(*gcsPollInterval) * time.Millisecond
	isPolling := *gcsPollInterval > 0

	for {
		if isPolling {
			logger.V(logutil.DEBUG).Info("Running GCS polling cycle", "bucket", *gcsRequestBucket, "prefix", *gcsBootstrapPrefix)
		} else {
			logger.V(logutil.DEFAULT).Info("Starting GCS bootstrap scanner", "bucket", *gcsRequestBucket, "prefix", *gcsBootstrapPrefix)
		}

		it := r.gcsClient.ListPrefix(ctx, *gcsRequestBucket, *gcsBootstrapPrefix)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to list GCS objects")
				break
			}

			// Skip directories
			if attrs.Name == *gcsBootstrapPrefix || attrs.Name == "" {
				continue
			}

			// Process the file
			r.processGCSFile(ctx, attrs.Bucket, attrs.Name, attrs.Generation, ch, "bootstrap")
		}

		if !isPolling {
			logger.V(logutil.DEFAULT).Info("GCS bootstrap scanner finished")
			return
		}

		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("GCS polling stopped due to context cancellation")
			return
		case <-time.After(pollDuration):
			// Continue next polling cycle
		}
	}
}

func (r *GCSMQFlow) processGCSFile(ctx context.Context, bucket, name string, generation int64, ch chan api.RequestMessage, source string) {
	logger := log.FromContext(ctx)

	// 1. Atomic Move to processing folder
	processingName := *gcsProcessingPrefix + name

	err := r.gcsClient.AtomicMove(ctx, bucket, name, processingName, generation)
	if err != nil {
		logger.V(logutil.DEBUG).Info("Skipping file (already processed or moved)", "bucket", bucket, "object", name, "error", err.Error())
		return
	}
	logger.V(logutil.DEFAULT).Info("Claimed GCS object for processing", "bucket", bucket, "object", name, "processingPath", processingName, "source", source)

	// 2. Download payload from the processing location
	payloadBytes, err := r.gcsClient.Download(ctx, bucket, processingName)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to download GCS object", "bucket", bucket, "object", processingName, "source", source)
		metrics.FailedReqs.Inc()
		return
	}

	// 3. Unmarshal the request
	var msgObj api.RequestMessage
	err = json.Unmarshal(payloadBytes, &msgObj)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal request message", "object", processingName, "source", source)
		metrics.FailedReqs.Inc()
		_ = r.gcsClient.Delete(ctx, bucket, processingName)
		return
	}

	// 4. Set up results tracking
	trackerID := fmt.Sprintf("%s-%s-%d", source, name, generation)
	resultsChannel := make(chan bool, 1)
	gcsResultChannels.Store(trackerID, resultsChannel)
	defer gcsResultChannels.Delete(trackerID)

	if msgObj.Metadata == nil {
		msgObj.Metadata = make(map[string]string)
	}
	msgObj.Metadata[GCS_PUBSUB_ID] = trackerID
	msgObj.Metadata[GCS_OBJECT_NAME] = processingName

	// 5. Send to processing channel
	metrics.AsyncReqs.Inc()
	ch <- msgObj

	// 6. Wait for result
	result := <-resultsChannel
	if result {
		// Delete the request file from processing folder
		err = r.gcsClient.Delete(ctx, bucket, processingName)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to delete processed GCS object", "object", processingName)
		} else {
			logger.V(logutil.DEFAULT).Info("Successfully processed and deleted GCS object", "object", processingName)
		}
	} else {
		// Move it back for retry
		err = r.gcsClient.AtomicMove(ctx, bucket, processingName, name, 0)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to move back GCS object for retry", "object", processingName, "target", name)
		} else {
			logger.V(logutil.DEFAULT).Info("Moved GCS object back for retry", "object", processingName, "target", name)
		}
	}
}

func (r *GCSMQFlow) resultWorker(ctx context.Context) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-r.resultChannel:
			objectName := "results/" + msg.Id + ".json"
			err := r.gcsClient.Upload(ctx, *gcsResultBucket, objectName, []byte(msg.Payload))

			trackerID := msg.Metadata[GCS_PUBSUB_ID]
			value, ok := gcsResultChannels.Load(trackerID)
			if ok {
				resultChan := value.(chan bool)
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Failed to upload result to GCS", "object", objectName)
					metrics.FailedReqs.Inc()
					resultChan <- false
				} else {
					logger.V(logutil.DEFAULT).Info("Uploaded result to GCS", "object", objectName)
					metrics.SuccessfulReqs.Inc()
					resultChan <- true
				}
			}
		}
	}
}

func (r *GCSMQFlow) retryQueueWorker(ctx context.Context) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-r.retryChannel:
			trackerID := msg.RequestMessage.Metadata[GCS_PUBSUB_ID]
			value, ok := gcsResultChannels.Load(trackerID)
			if ok {
				resultChan := value.(chan bool)
				logger.V(logutil.DEFAULT).Info("Retrying GCS message", "trackerID", trackerID, "backoffSeconds", msg.BackoffDurationSeconds)
				metrics.Retries.Inc()

				// Use a goroutine to wait for backoff so we don't block other retries
				go func(delay float64, ch chan bool) {
					if delay > 0 {
						time.Sleep(time.Duration(delay * float64(time.Second)))
					}
					ch <- false
				}(msg.BackoffDurationSeconds, resultChan)
			}
		}
	}
}

func (r *GCSMQFlow) notificationWorker(ctx context.Context, subscriberID string, ch chan api.RequestMessage, gate api.DispatchGate) {
	logger := log.FromContext(ctx)
	sub := r.pubsubClient.Subscriber(subscriberID)

	for {
		receiveCtx, cancel := context.WithCancel(ctx)
		budget := gate.Budget(ctx)
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if gate.Budget(ctx) != budget {
						cancel()
						return
					}
				case <-receiveCtx.Done():
					return
				}
			}
		}()

		currBatchSize := int(math.Floor(float64(*gcsBatchSize) * budget))
		sub.ReceiveSettings.MaxOutstandingMessages = currBatchSize
		sub.ReceiveSettings.NumGoroutines = 1
		if currBatchSize <= 0 {
			<-receiveCtx.Done()
			continue
		}

		err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			var notification GCSNotification
			if err := json.Unmarshal(msg.Data, &notification); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal GCS notification")
				msg.Ack()
				return
			}

			if notification.Bucket == "" || notification.Name == "" {
				msg.Ack()
				return
			}

			r.processGCSFile(ctx, notification.Bucket, notification.Name, notification.Generation, ch, "notification")

			metrics.MessageLatencyTime.Observe(float64(time.Since(msg.PublishTime).Milliseconds()))
			msg.Ack()
		})

		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Fail to receive messages from GCS notification subscription")
		}
	}
}
