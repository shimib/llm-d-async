package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"google.golang.org/api/iterator"
)

// MockObjectIterator implements ObjectIterator interface for testing.
type MockObjectIterator struct {
	objects []*storage.ObjectAttrs
	index   int
}

func (m *MockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if m.index >= len(m.objects) {
		return nil, iterator.Done
	}
	obj := m.objects[m.index]
	m.index++
	return obj, nil
}

// MockClientWrapper implements ClientWrapper interface for testing.
type MockClientWrapper struct {
	mu            sync.Mutex
	objects       map[string][]byte // bucket/name -> data
	generations   map[string]int64  // bucket/name -> generation
	listResponses map[string][]*storage.ObjectAttrs
	
	AtomicMoveCalled int
	DownloadCalled   int
	UploadCalled     int
	DeleteCalled     int
	ListPrefixCalled int
}

func NewMockClientWrapper() *MockClientWrapper {
	return &MockClientWrapper{
		objects:       make(map[string][]byte),
		generations:   make(map[string]int64),
		listResponses: make(map[string][]*storage.ObjectAttrs),
	}
}

func (m *MockClientWrapper) AtomicMove(ctx context.Context, bucket, srcObject, dstObject string, srcGeneration int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AtomicMoveCalled++

	srcKey := fmt.Sprintf("%s/%s", bucket, srcObject)
	dstKey := fmt.Sprintf("%s/%s", bucket, dstObject)

	data, ok := m.objects[srcKey]
	if !ok {
		return errors.New("source object not found")
	}

	gen := m.generations[srcKey]
	if srcGeneration != 0 && gen != srcGeneration {
		return ErrObjectPreconditionFailed
	}

	m.objects[dstKey] = data
	m.generations[dstKey] = gen + 1
	delete(m.objects, srcKey)
	delete(m.generations, srcKey)

	return nil
}

func (m *MockClientWrapper) Download(ctx context.Context, bucket, object string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DownloadCalled++

	key := fmt.Sprintf("%s/%s", bucket, object)
	data, ok := m.objects[key]
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	return data, nil
}

func (m *MockClientWrapper) Upload(ctx context.Context, bucket, object string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UploadCalled++

	key := fmt.Sprintf("%s/%s", bucket, object)
	m.objects[key] = data
	m.generations[key]++
	return nil
}

func (m *MockClientWrapper) Delete(ctx context.Context, bucket, object string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteCalled++

	key := fmt.Sprintf("%s/%s", bucket, object)
	delete(m.objects, key)
	delete(m.generations, key)
	return nil
}

func (m *MockClientWrapper) ListPrefix(ctx context.Context, bucket, prefix string) ObjectIterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ListPrefixCalled++

	return &MockObjectIterator{objects: m.listResponses[bucket]}
}

func (m *MockClientWrapper) Close() error {
	return nil
}

func TestGCSMQFlow_BootstrapScanner(t *testing.T) {
	mockGCS := NewMockClientWrapper()
	bucket := "test-bucket"
	*gcsRequestBucket = bucket
	*gcsBootstrapPrefix = "new/"
	*gcsProcessingPrefix = "processing/"

	// Setup initial objects in GCS
	msg := api.RequestMessage{Id: "msg-1", Payload: map[string]any{"test": "data"}}
	msgBytes, _ := json.Marshal(msg)
	
	mockGCS.objects[bucket+"/new/msg-1.json"] = msgBytes
	mockGCS.generations[bucket+"/new/msg-1.json"] = 1
	mockGCS.listResponses[bucket] = []*storage.ObjectAttrs{
		{Bucket: bucket, Name: "new/msg-1.json", Generation: 1},
	}

	flow := NewGCSMQFlowWithClients(mockGCS, nil)
	ch := make(chan api.RequestMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go flow.bootstrapScanner(ctx, ch)

	select {
	case received := <-ch:
		if received.Id != "msg-1" {
			t.Errorf("Expected msg-1, got %s", received.Id)
		}
		// Confirm it was moved to processing and deleted from original
		mockGCS.mu.Lock()
		if _, ok := mockGCS.objects[bucket+"/new/msg-1.json"]; ok {
			t.Error("Original object still exists")
		}
		if _, ok := mockGCS.objects[bucket+"/processing/new/msg-1.json"]; !ok {
			t.Error("Object not found in processing folder")
		}
		mockGCS.mu.Unlock()
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for bootstrap scanner")
	}
}

func TestGCSMQFlow_ProcessGCSFile(t *testing.T) {
	mockGCS := NewMockClientWrapper()
	bucket := "test-bucket"
	*gcsRequestBucket = bucket
	*gcsProcessingPrefix = "processing/"

	msg := api.RequestMessage{Id: "msg-2", Payload: map[string]any{"test": "data2"}}
	msgBytes, _ := json.Marshal(msg)
	
	mockGCS.objects[bucket+"/new/msg-2.json"] = msgBytes
	mockGCS.generations[bucket+"/new/msg-2.json"] = 123

	flow := NewGCSMQFlowWithClients(mockGCS, nil)
	ch := make(chan api.RequestMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run resultWorker in background to handle the completion
	go flow.resultWorker(ctx)

	go flow.processGCSFile(ctx, bucket, "new/msg-2.json", 123, ch, "test")

	select {
	case received := <-ch:
		if received.Id != "msg-2" {
			t.Errorf("Expected msg-2, got %s", received.Id)
		}
		
		// Simulate successful processing
		flow.ResultChannel() <- api.ResultMessage{
			Id: "msg-2",
			Payload: "result-2",
			Metadata: received.Metadata,
		}
		
		// Wait a bit for resultWorker to delete the file
		time.Sleep(100 * time.Millisecond)
		
		mockGCS.mu.Lock()
		if _, ok := mockGCS.objects[bucket+"/processing/new/msg-2.json"]; ok {
			t.Error("Processing object was not deleted after success")
		}
		mockGCS.mu.Unlock()
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout")
	}
}

func TestGCSMQFlow_ResultWorker(t *testing.T) {
	mockGCS := NewMockClientWrapper()
	bucket := "result-bucket"
	*gcsResultBucket = bucket

	flow := NewGCSMQFlowWithClients(mockGCS, nil)
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	go flow.resultWorker(ctx)
	
	trackerID := "result-tracker"
	resChan := make(chan bool, 1)
	gcsResultChannels.Store(trackerID, resChan)
	
	msg := api.ResultMessage{
		Id: "msg-result",
		Payload: "{\"result\": \"ok\"}",
		Metadata: map[string]string{GCS_PUBSUB_ID: trackerID},
	}
	
	flow.ResultChannel() <- msg
	
	select {
	case res := <-resChan:
		if res != true {
			t.Error("Expected true for successful result upload")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout")
	}
	
	// Verify it was uploaded to GCS
	mockGCS.mu.Lock()
	defer mockGCS.mu.Unlock()
	expectedKey := bucket + "/results/msg-result.json"
	if _, ok := mockGCS.objects[expectedKey]; !ok {
		t.Errorf("Result object not found in GCS: %s", expectedKey)
	}
}

func TestGCSMQFlow_NoRaceCondition(t *testing.T) {
	mockGCS := NewMockClientWrapper()
	bucket := "race-bucket"
	*gcsRequestBucket = bucket
	*gcsBootstrapPrefix = "new/"
	*gcsProcessingPrefix = "processing/"

	numMessages := 20
	var listAttrs []*storage.ObjectAttrs
	for i := 0; i < numMessages; i++ {
		name := fmt.Sprintf("new/msg-%d.json", i)
		msg := api.RequestMessage{Id: fmt.Sprintf("msg-%d", i), Payload: map[string]any{"i": i}}
		msgBytes, _ := json.Marshal(msg)
		mockGCS.objects[bucket+"/"+name] = msgBytes
		mockGCS.generations[bucket+"/"+name] = 1
		listAttrs = append(listAttrs, &storage.ObjectAttrs{Bucket: bucket, Name: name, Generation: 1})
	}
	mockGCS.listResponses[bucket] = listAttrs

	flow := NewGCSMQFlowWithClients(mockGCS, nil)
	ch := make(chan api.RequestMessage, numMessages)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start 3 bootstrap scanners (simulating multiple instances or concurrent calls)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			flow.bootstrapScanner(ctx, ch)
		}()
	}

	// Also start result worker to handle results
	go flow.resultWorker(ctx)

	receivedIDs := make(map[string]int)
	var mu sync.Mutex
	
	done := make(chan bool)
	go func() {
		for i := 0; i < numMessages; i++ {
			msg := <-ch
			mu.Lock()
			receivedIDs[msg.Id]++
			mu.Unlock()
			
			// Finish processing
			flow.ResultChannel() <- api.ResultMessage{
				Id: msg.Id,
				Payload: "done",
				Metadata: msg.Metadata,
			}
		}
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for all messages")
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(receivedIDs) != numMessages {
		t.Errorf("Expected %d unique messages, got %d", numMessages, len(receivedIDs))
	}
	for id, count := range receivedIDs {
		if count > 1 {
			t.Errorf("Message %s received %d times", id, count)
		}
	}
}

func TestGCSMQFlow_ContextCancellation(t *testing.T) {
	mockGCS := NewMockClientWrapper()
	flow := NewGCSMQFlowWithClients(mockGCS, nil)

	ctx, cancel := context.WithCancel(context.Background())
	
	done := make(chan bool)
	go func() {
		flow.resultWorker(ctx)
		done <- true
	}()
	
	time.Sleep(100 * time.Millisecond)
	cancel()
	
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("resultWorker did not stop after context cancellation")
	}
}

func TestGCSMQFlow_RetryQueueWorker(t *testing.T) {
	mockGCS := NewMockClientWrapper()
	flow := NewGCSMQFlowWithClients(mockGCS, nil)
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	go flow.retryQueueWorker(ctx)
	
	trackerID := "test-tracker"
	resChan := make(chan bool, 1)
	gcsResultChannels.Store(trackerID, resChan)
	
	retryMsg := api.RetryMessage{
		EmbelishedRequestMessage: api.EmbelishedRequestMessage{
			RequestMessage: api.RequestMessage{
				Id: "msg-retry",
				Metadata: map[string]string{GCS_PUBSUB_ID: trackerID},
			},
		},
	}
	
	flow.RetryChannel() <- retryMsg
	
	select {
	case res := <-resChan:
		if res != false {
			t.Error("Expected false for retry")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout")
	}
}
