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
	"flag"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var isGMP = flag.Bool("gate.pmetric.is-gmp", false, "Is this GMP (Google Managed Prometheus).")
var prometheusURL = flag.String("gate.prometheus.url", "", "Prometheus URL for non GMP metric")
var gmpProjectID = flag.String("gate.pmetric.gmp.project-id", "", "Project ID for Google Managed Prometheus")
var prometheusQueryModelName = flag.String("gate.prometheus.model-name", "", "metrics name to use for avg_queue_size")

//

// BinaryMetricDispatchGate implements DispatchGate supporting GMP (Google Managed Prometheus) Collector or general Prometheus collector.
// It returns 0.0 (no capacity) if the metric value is non-zero,
// and 1.0 (full capacity) if the metric value is zero.
type BinaryMetricDispatchGate struct {
	v1api v1.API
	query string
}

// NewBinaryMetricDispatchGate creates a new gate based on the provided Prometheus metric.
func NewBinaryMetricDispatchGate(clientConfig api.Config, query string) *BinaryMetricDispatchGate {

	ctx := context.Background()
	logger := log.FromContext(ctx)
	// 1. Create the authenticated GCP client
	// This automatically picks up Application Default Credentials and refreshes the token.

	client, err := api.NewClient(clientConfig)
	if err != nil {
		logger.Error(err, "Error creating Prometheus API client")
		panic(err)
	}
	v1api := v1.NewAPI(client)

	// 2. Define your PromQL query.
	// Replace "my_custom_metric" with the actual metric your PodMonitoring is scraping.

	return &BinaryMetricDispatchGate{
		v1api: v1api,
		query: query,
	}
}

// Budget implements DispatchGate.
func (g *BinaryMetricDispatchGate) Budget(ctx context.Context) float64 {
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

func AverageQueueSizeGate() *BinaryMetricDispatchGate {
	ctx := context.Background()
	logger := log.FromContext(ctx)
	if *isGMP {

		gcpClient, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/monitoring.read")
		if err != nil {
			logger.Error(err, "Failed to create authenticated GCP client")
			panic(err)
		}

		// 2. Configure the Prometheus API Client
		// Notice we do NOT include /api/v1/query here; the client library appends it automatically.
		promURL := fmt.Sprintf("https://monitoring.googleapis.com/v1/projects/%s/location/global/prometheus", *gmpProjectID)

		clientConfig := api.Config{
			Address: promURL,
			// Inject the authenticated transport here so every request gets the Bearer token.
			RoundTripper: gcpClient.Transport,
		}

		return NewBinaryMetricDispatchGate(clientConfig, `inference_pool_average_queue_size{name="`+*prometheusQueryModelName+`"}`)
	} else {
		clientConfig := api.Config{
			Address: *prometheusURL,
		}
		return NewBinaryMetricDispatchGate(clientConfig, `inference_pool_average_queue_size{name="`+*prometheusQueryModelName+`"}`)
	}
}
