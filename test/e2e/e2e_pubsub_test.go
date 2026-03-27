package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
)

var _ = ginkgo.Describe("GCP Pub/Sub E2E", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		drainPubSub(ctx, pubsubClient, resultSubID)
		resetMock(adminURL)
	})

	ginkgo.It("processes a message end-to-end using Pub/Sub", func() {
		msg := makeRequestMessage("e2e-pubsub-1", 5*time.Minute)
		publishToPubSub(ctx, pubsubClient, requestTopicID, msg)

		var result *api.ResultMessage
		gomega.Eventually(func() bool {
			result = receiveFromPubSub(ctx, pubsubClient, resultSubID)
			return result != nil
		}, 60*time.Second, 1*time.Second).Should(gomega.BeTrue())

		gomega.Expect(result.Id).To(gomega.Equal("e2e-pubsub-1"))
	})
})
