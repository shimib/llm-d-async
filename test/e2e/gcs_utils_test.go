package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/onsi/gomega"
	"google.golang.org/api/iterator"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
)

var gcsTestClient *storage.Client

func setupGCSClient() {
	os.Setenv("STORAGE_EMULATOR_HOST", gcsMockHost)
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gcsTestClient = client

	// Create buckets if they don't exist
	for _, bucket := range []string{"e2e-requests", "e2e-results"} {
		err := gcsTestClient.Bucket(bucket).Create(ctx, "e2e-test-project", nil)
		if err != nil && !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "Conflict") {
			gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		}
	}
}

func enqueueGCSMessage(ctx context.Context, msg api.RequestMessage) {
	data, err := json.Marshal(msg)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())

	obj := gcsTestClient.Bucket("e2e-requests").Object("new/" + msg.Id + ".json")
	w := obj.NewWriter(ctx)
	w.ChunkSize = 0
	_, err = w.Write(data)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	err = w.Close()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
}

func getGCSResultCount(ctx context.Context) int64 {
	it := gcsTestClient.Bucket("e2e-results").Objects(ctx, &storage.Query{Prefix: "results/"})
	var count int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		fmt.Println("Found result object:", attrs.Name)
		count++
	}
	return count
}

func popGCSResult(ctx context.Context, expectedId string) *api.ResultMessage {
	objName := "results/" + expectedId + ".json"
	
	// Instead of NewReader which has issues with fake-gcs-server, use direct HTTP GET
	url := fmt.Sprintf("http://%s/storage/v1/b/e2e-results/o/%s?alt=media", gcsMockHost, "results%2F" + expectedId + ".json")
	resp, err := http.Get(url)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	gomega.ExpectWithOffset(1, resp.StatusCode).To(gomega.Equal(http.StatusOK))

	val, err := io.ReadAll(resp.Body)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())

	var msg api.ResultMessage
	gomega.ExpectWithOffset(1, json.Unmarshal(val, &msg)).To(gomega.Succeed())
	
	// delete the result so we don't count it again
	gcsTestClient.Bucket("e2e-results").Object(objName).Delete(ctx) //nolint:errcheck

	return &msg
}

func cleanupGCSBuckets(ctx context.Context) {
	for _, bucket := range []string{"e2e-requests", "e2e-results"} {
		it := gcsTestClient.Bucket(bucket).Objects(ctx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
			gcsTestClient.Bucket(bucket).Object(attrs.Name).Delete(ctx) //nolint:errcheck
		}
	}
}
