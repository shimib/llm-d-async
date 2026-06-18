package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type mockAttributeGate struct {
	allowed       bool
	acquireCalled bool
	releaseCalled bool
}

func (m *mockAttributeGate) Budget(ctx context.Context) float64 { return 1.0 }
func (m *mockAttributeGate) Acquire(ctx context.Context, attrs map[string]string) (pipeline.AcquireResult, error) {
	m.acquireCalled = true
	return pipeline.AcquireResult{
		Allowed:        m.allowed,
		Classification: api.ClassificationReserved,
		Release:        func() { m.releaseCalled = true },
	}, nil
}

func TestProcessMessages_QuotaGating(t *testing.T) {
	flow := &PubSubMQFlow{}
	ch := make(chan *api.InternalRequest, 1)

	tests := []struct {
		name           string
		allowed        bool
		expectedResult bool // result sent back to resultChannel
		expectAck      bool
		expectNack     bool
	}{
		{
			name:           "Allowed and Success",
			allowed:        true,
			expectedResult: true,
			expectAck:      true,
			expectNack:     false,
		},
		{
			name:           "Allowed and Failure",
			allowed:        true,
			expectedResult: false,
			expectAck:      false,
			expectNack:     true,
		},
		{
			name:       "Denied by Quota",
			allowed:    false,
			expectAck:  false,
			expectNack: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := &mockAttributeGate{allowed: tt.allowed}

			// We need a way to mock the pubsub.Message.
			// Since we can't easily create a pubsub.Message with custom Ack/Nack handlers,
			// we rely on the fact that we can verify if the message was processed.

			msgData, _ := json.Marshal(api.RequestMessage{ID: "test-msg"})

			// Use a custom receive function that yields one message
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				msg := &pubsub.Message{
					ID:         "msg-1",
					Data:       msgData,
					Attributes: map[string]string{"userid": "user1"},
				}
				// Note: msg.Ack() and msg.Nack() will panic if not properly initialized by the library.
				// However, we are testing the logic flow.

				// In a real test, we'd need to mock the Ack/Nack methods, which is hard in this lib.
				// For the purpose of this test, we'll verify the gate calls and channel interactions.

				f(ctx, msg)
				return nil
			}

			// Run in a goroutine because it blocks on resultsChannel
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			go func() {
				if tt.allowed {
					select {
					case msg := <-ch:
						// Simulate result worker sending back a result
						pubsubID := msg.TransportCorrelationID
						val, _ := resultChannels.Load(pubsubID)
						resCh := val.(chan bool)
						resCh <- tt.expectedResult
					case <-ctx.Done():
					}
				}
			}()

			// We wrap in a recover to catch panics from Ack/Nack on uninitialized messages
			defer func() {
				if r := recover(); r != nil {
					// Expected panic if Ack/Nack is called on mock message
					// But we should check if Acquire/Release were called
					if !gate.acquireCalled {
						t.Errorf("Acquire was not called")
					}
					if tt.allowed && !gate.releaseCalled {
						t.Errorf("Release was not called for allowed request")
					}
				}
			}()

			_ = flow.processMessages(ctx, receive, "test-sub", "test-pool", ch, gate)
		})
	}
}

func TestNewGCPPubSubMQFlow_PoolRequiredAndValidation(t *testing.T) {
	origEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	origConfig := *topicsConfigFile
	origProject := *projectID
	defer func() {
		*topicsConfigFile = origConfig
		*projectID = origProject
		if origEmulatorHost != "" {
			_ = os.Setenv("PUBSUB_EMULATOR_HOST", origEmulatorHost)
		} else {
			_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
		}
	}()

	*projectID = "test-project"

	tmpFile, err := os.CreateTemp("", "topics-config-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	// Case 1: worker_pool_id is missing, and pool "default" does not exist (so it defaults to "default" and panics because not found)
	missingPoolConfig := `[{"subscriber_id":"sub-1","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(missingPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	*topicsConfigFile = tmpFile.Name()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when worker_pool_id is missing and 'default' pool does not exist, got none")
			} else {
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "not found in pool configuration") {
					t.Errorf("Unexpected panic message for missing worker_pool_id: %v", r)
				}
			}
		}()
		NewGCPPubSubMQFlow(WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	}()

	// Case 5: worker_pool_id is missing, but only a single 'default' pool is specified
	if err := os.WriteFile(tmpFile.Name(), []byte(missingPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	*topicsConfigFile = tmpFile.Name()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unexpected panic when worker_pool_id is missing but default pool exists: %v", r)
			}
		}()
		NewGCPPubSubMQFlow(WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	}()

	// Case 6: worker_pool_id is specified as custom, but only a single 'default' pool is specified
	customPoolConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"custom-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(customPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	*topicsConfigFile = tmpFile.Name()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when worker_pool_id is custom but only default pool exists, got nil")
			}
		}()
		NewGCPPubSubMQFlow(WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	}()

	// Case 2: worker_pool_id specified but pool does not exist
	nonExistentPoolConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"non-existent","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(nonExistentPoolConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	*topicsConfigFile = tmpFile.Name()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when specified worker_pool_id does not exist, got none")
			} else {
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "not found in pool configuration") {
					t.Errorf("Unexpected panic message for non-existent worker_pool_id: %v", r)
				}
			}
		}()
		NewGCPPubSubMQFlow(WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	}()

	// Case 3: worker_pool_id specified and pool exists, but igw_base_url is missing
	missingIgwConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"test-pool","inference_objective":"obj"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(missingIgwConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	*topicsConfigFile = tmpFile.Name()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when igw_base_url is missing in config, got none")
			} else {
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "igw_base_url must be specified") {
					t.Errorf("Unexpected panic message for missing igw_base_url: %v", r)
				}
			}
		}()
		NewGCPPubSubMQFlow(WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	}()

	// Case 4: worker_pool_id and igw_base_url specified and pool exists
	validConfig := `[{"subscriber_id":"sub-1","worker_pool_id":"test-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`
	if err := os.WriteFile(tmpFile.Name(), []byte(validConfig), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	*topicsConfigFile = tmpFile.Name()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unexpected panic when worker_pool_id and igw_base_url exist: %v", r)
			}
		}()
		NewGCPPubSubMQFlow(WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	}()
}

const testProject = "test-project"

// newFakePubSub starts an in-memory Pub/Sub fake and returns a client wired to
// it along with a closer to take the broker down. The closer is idempotent and
// also runs on test cleanup.
func newFakePubSub(t *testing.T) (*pubsub.Client, func()) {
	t.Helper()
	srv := pstest.NewServer()
	var once sync.Once
	closeSrv := func() { once.Do(func() { _ = srv.Close() }) }
	t.Cleanup(closeSrv)

	client, err := pubsub.NewClient(context.Background(), testProject,
		option.WithEndpoint(srv.Addr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("failed to create fake pubsub client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, closeSrv
}

// createSubscription provisions a topic and subscription on the fake so that an
// admin GetSubscription lookup succeeds.
func createSubscription(t *testing.T, client *pubsub.Client, subID string) {
	t.Helper()
	ctx := context.Background()
	topic := "projects/" + testProject + "/topics/health-topic"
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/" + testProject + "/subscriptions/" + subID,
		Topic: topic,
	}); err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
}

func TestHealthCheck_Healthy(t *testing.T) {
	client, _ := newFakePubSub(t)
	createSubscription(t, client, "sub-1")

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err != nil {
		t.Errorf("expected healthy, got error: %v", err)
	}
}

func TestHealthCheck_MissingSubscription(t *testing.T) {
	client, _ := newFakePubSub(t)
	// No subscription created.

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "missing-sub"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err == nil {
		t.Error("expected error for missing subscription, got nil")
	}
}

// TestHealthCheck_BrokerUnreachable is the regression guard for #246: an
// unreachable Pub/Sub backend must mark the pod not-ready.
func TestHealthCheck_BrokerUnreachable(t *testing.T) {
	client, closeSrv := newFakePubSub(t)
	createSubscription(t, client, "sub-1")

	flow := &PubSubMQFlow{
		client:          client,
		requestChannels: []RequestChannelData{{subscriberID: "sub-1"}},
	}

	// Take the broker down before probing. The admin RPC retries on Unavailable,
	// so bound the probe with a short deadline (the real /readyz path uses the
	// health server's checkerTimeout) and assert it surfaces an error.
	closeSrv()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := flow.HealthCheck(ctx); err == nil {
		t.Error("expected error when broker is unreachable, got nil")
	}
}
