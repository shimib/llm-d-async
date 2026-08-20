//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	redisgate "github.com/llm-d/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyVersionGate_Integration drives the version gate through a real
// worker pool. It covers the two verdicts that only mean something in that
// context: a request ahead of the backend parks until the live version is
// published (the worker's ActionWait loop), and a request behind it is dropped
// without ever reaching the inference backend.
func TestPolicyVersionGate_Integration(t *testing.T) {
	var mu sync.Mutex
	dispatched := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		dispatched = append(dispatched, r.Header.Get("X-Request-Id"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer server.Close()

	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	gate := redisgate.NewPolicyVersionGate(rdb,
		redisgate.DefaultPolicyVersionAttribute, redisgate.DefaultPolicyVersionKey, "userid", 0)

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 5)
	retryChannel := make(chan pipeline.RetryMessage, 5)
	resultChannel := make(chan asyncapi.ResultMessage, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
			client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil, gate)
	}()

	submit := func(id, version string) {
		t.Helper()
		requestChannel <- pipeline.EmbelishedRequestMessage{
			InternalRequest: asyncapi.NewInternalRequest(
				asyncapi.InternalRouting{RequestQueueName: "test-queue"},
				&asyncapi.RequestMessage{
					ID:       id,
					Created:  time.Now().Unix(),
					Deadline: time.Now().Add(5 * time.Minute).Unix(),
					Payload:  map[string]any{"model": "test"},
					Metadata: map[string]string{redisgate.DefaultPolicyVersionAttribute: version},
				},
			),
			HttpHeaders: map[string]string{"X-Request-Id": id},
			RequestURL:  server.URL + "/v1/completions",
		}
	}

	t.Run("request ahead of the backend parks until its version goes live", func(t *testing.T) {
		require.NoError(t, mr.Set(redisgate.DefaultPolicyVersionKey, "1"))

		submit("req-ahead", "2")

		// The worker must hold the request rather than dispatch it.
		select {
		case res := <-resultChannel:
			t.Fatalf("request dispatched before its version went live: %+v", res)
		case <-time.After(500 * time.Millisecond):
		}
		mu.Lock()
		assert.Empty(t, dispatched, "backend was hit while the gate should have been parking")
		mu.Unlock()

		// Publishing the version releases the parked worker.
		require.NoError(t, mr.Set(redisgate.DefaultPolicyVersionKey, "2"))

		select {
		case res := <-resultChannel:
			assert.Equal(t, "req-ahead", res.ID)
			assert.Equal(t, http.StatusOK, res.StatusCode)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the parked request to be released")
		}

		mu.Lock()
		assert.Equal(t, []string{"req-ahead"}, dispatched)
		mu.Unlock()
	})

	t.Run("request behind the backend is dropped without dispatch", func(t *testing.T) {
		require.NoError(t, mr.Set(redisgate.DefaultPolicyVersionKey, "5"))

		submit("req-stale", "3")

		select {
		case res := <-resultChannel:
			assert.Equal(t, "req-stale", res.ID)
			assert.Equal(t, asyncapi.ErrCodeGateDropped, res.ErrorCode)
			assert.Contains(t, res.ErrorMessage, "stale")
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the stale request to be dropped")
		}

		mu.Lock()
		assert.NotContains(t, dispatched, "req-stale", "a stale rollout reached the backend")
		mu.Unlock()
	})

	t.Run("matching version dispatches immediately", func(t *testing.T) {
		require.NoError(t, mr.Set(redisgate.DefaultPolicyVersionKey, "5"))

		submit("req-match", "5")

		select {
		case res := <-resultChannel:
			assert.Equal(t, "req-match", res.ID)
			assert.Equal(t, http.StatusOK, res.StatusCode)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the matching request")
		}

		mu.Lock()
		assert.Contains(t, dispatched, "req-match")
		mu.Unlock()
	})

	cancel()
	wg.Wait()
}

// TestGateFactory_PolicyVersion validates that GateFactory parses
// "policy-version" params, applies the documented defaults, and rejects a
// config with no Redis address.
func TestGateFactory_PolicyVersion(t *testing.T) {
	mr := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	_, err := factory.CreateGate(pipeline.GateConfig{GateType: "policy-version", GateParams: map[string]any{}})
	assert.Error(t, err, "should fail when address is missing")

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "policy-version", GateParams: map[string]any{
		"address": mr.Addr(),
	}})
	require.NoError(t, err)
	require.NotNil(t, gate)
	assert.Equal(t, 1.0, gate.Budget(context.Background()))

	// Defaults: attribute "policy_version", key "policy:live_version".
	require.NoError(t, mr.Set(redisgate.DefaultPolicyVersionKey, "4"))

	ctx := context.Background()
	var releases []pipeline.GateReleaseFunc
	req := asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &asyncapi.RequestMessage{
		Metadata: map[string]string{redisgate.DefaultPolicyVersionAttribute: "4"},
	})
	verdict, err := gate.Apply(ctx, req, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	stale := asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &asyncapi.RequestMessage{
		Metadata: map[string]string{redisgate.DefaultPolicyVersionAttribute: "2"},
	})
	verdict, err = gate.Apply(ctx, stale, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionDrop, verdict.Action)

	// max_lag widens what counts as current, so the same request that was
	// stale above is admitted by a gate configured to the trainer's tolerance.
	tolerant, err := factory.CreateGate(pipeline.GateConfig{GateType: "policy-version", GateParams: map[string]any{
		"address": mr.Addr(),
		"max_lag": 2,
	}})
	require.NoError(t, err)
	verdict, err = tolerant.Apply(ctx, stale, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	_, err = factory.CreateGate(pipeline.GateConfig{GateType: "policy-version", GateParams: map[string]any{
		"address": mr.Addr(),
		"max_lag": -1,
	}})
	assert.Error(t, err, "should reject a negative max_lag")
}
