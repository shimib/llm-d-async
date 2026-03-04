/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"

	"golang.org/x/oauth2/google"
)

// BinaryGMPMetricDispatchGate implements DispatchGate based on a GMP (Google Managed Prometheus) Collector.
// It returns 0.0 (no capacity) if the metric value is non-zero,
// and 1.0 (full capacity) if the metric value is zero.
type BinaryGMPMetricDispatchGate struct {
	v1api v1.API
	query string
}

// NewBinaryNumericMetricDispatchGate creates a new gate based on the provided Prometheus metric.
func NewBinaryGMPMetricDispatchGate(projectID string, query string) *BinaryGMPMetricDispatchGate {

	ctx := context.Background()
	logger := log.FromContext(ctx)
	// 1. Create the authenticated GCP client
	// This automatically picks up Application Default Credentials and refreshes the token.
	gcpClient, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/monitoring.read")
	if err != nil {
		logger.Error(err, "Failed to create authenticated GCP client")
		panic(err)
	}

	// 2. Configure the Prometheus API Client
	// Notice we do NOT include /api/v1/query here; the client library appends it automatically.
	promURL := fmt.Sprintf("https://monitoring.googleapis.com/v1/projects/%s/location/global/prometheus", projectID)

	clientConfig := api.Config{
		Address: promURL,
		// Inject the authenticated transport here so every request gets the Bearer token.
		RoundTripper: gcpClient.Transport,
	}
	client, err := api.NewClient(clientConfig)
	if err != nil {
		logger.Error(err, "Error creating Prometheus API client")
		panic(err)
	}
	v1api := v1.NewAPI(client)

	// 2. Define your PromQL query.
	// Replace "my_custom_metric" with the actual metric your PodMonitoring is scraping.

	return &BinaryGMPMetricDispatchGate{
		v1api: v1api,
		query: query,
	}
}

// Budget implements DispatchGate.
func (g *BinaryGMPMetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	// 3. Execute the query
	// We use Query() for instant queries. Use QueryRange() for over-time data.
	result, warnings, err := g.v1api.Query(ctx, g.query, time.Now())
	if err != nil {
		logger.Error(err, "Error querying Prometheus")
		return 1.0
	}
	if len(warnings) > 0 {
		logger.V(logutil.DEFAULT).Info("Warnings", "warnings", warnings)
		return 1.0
	}
	vec, ok := result.(model.Vector)
	if !ok {
		logger.V(logutil.DEFAULT).Info("Expected Vector result, got: ", result)
		return 1.0
	}

	if len(vec) == 0 {
		logger.V(logutil.DEFAULT).Info("No metrics found for the given query.")
		return 1.0
	}

	for _, sample := range vec {
		// sample.Metric contains the labels
		// sample.Value contains the actual float64 scraped value
		logger.V(logutil.DEBUG).Info("labels and values...", "labels", sample.Metric, "values", sample.Value)
		if sample.Value == 0.0 {
			return 1.0
		} else {
			return 0.0
		}
	}
	return 1.0
}

func AverageQueueSizeGMPGate(projectID string, modelName string) *BinaryGMPMetricDispatchGate {
	return NewBinaryGMPMetricDispatchGate(projectID, `inference_pool_average_queue_size{name="`+modelName+`"}`)
}
