package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("GCS Flow E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		cleanupGCSBuckets(ctx)
		resetMock(adminURL)
	})

	ginkgo.It("processes a message end-to-end via GCS", func() {
		msg := makeRequestMessage("e2e-gcs-1", 5*time.Minute)
		enqueueGCSMessage(ctx, msg)

		gomega.Eventually(func() int64 {
			return getGCSResultCount(ctx)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popGCSResult(ctx, "e2e-gcs-1")
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.Id).To(gomega.Equal("e2e-gcs-1"))
	})

	ginkgo.It("retries on 5xx from inference gateway via GCS", func() {
		// Configure mock to fail the first request with 500
		setMockFailures(adminURL, 500, 1)

		msg := makeRequestMessage("retry-gcs-msg", 5*time.Minute)
		enqueueGCSMessage(ctx, msg)

		// Eventually the result should appear after retry
		gomega.Eventually(func() int64 {
			return getGCSResultCount(ctx)
		}, 120*time.Second, 1*time.Second).Should(gomega.BeNumerically(">=", 1))

		result := popGCSResult(ctx, "retry-gcs-msg")
		gomega.Expect(result).NotTo(gomega.BeNil())
		gomega.Expect(result.Id).To(gomega.Equal("retry-gcs-msg"))
	})
})
