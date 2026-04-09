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

	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/prometheus/client_golang/api"
)

var isGMP = flag.Bool("gate.pmetric.is-gmp", false, "Is this GMP (Google Managed Prometheus).")
var prometheusURL = flag.String("gate.prometheus.url", "", "Prometheus URL for non GMP metric")
var gmpProjectID = flag.String("gate.pmetric.gmp.project-id", "", "Project ID for Google Managed Prometheus")
var prometheusQueryModelName = flag.String("gate.prometheus.model-name", "", "metrics name to use for avg_queue_size")

// BinaryMetricDispatchGate implements DispatchGate using a MetricSource.
// It returns 0.0 (no capacity) if the metric value is non-zero,
// and 1.0 (full capacity) if the metric value is zero.
type BinaryMetricDispatchGate struct {
	source MetricSource
}

// NewBinaryMetricDispatchGateWithSource creates a new gate using the provided MetricSource.
func NewBinaryMetricDispatchGateWithSource(source MetricSource) *BinaryMetricDispatchGate {
	return &BinaryMetricDispatchGate{
		source: source,
	}
}

// Budget implements DispatchGate.
func (g *BinaryMetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := logging.Log

	samples, err := g.source.Query(ctx)
	if err != nil {
		logger.V(logging.DEFAULT).Info("MetricSource error, failing open", "error", err)
		return 1.0
	}

	if len(samples) == 0 {
		logger.V(logging.DEFAULT).Info("No metrics found, failing open")
		return 1.0
	}

	if samples[0].Value == 0.0 {
		return 1.0
	}
	return 0.0
}

// AverageQueueSizeGate creates a BinaryMetricDispatchGate from command-line flags.
func AverageQueueSizeGate() *BinaryMetricDispatchGate {
	expr := buildPromQL("inference_pool_average_queue_size",
		map[string]string{"name": *prometheusQueryModelName})

	var source MetricSource
	if *isGMP {
		var err error
		source, err = NewGMPPromQLMetricSource(*gmpProjectID, expr)
		if err != nil {
			panic(err)
		}
	} else {
		var err error
		source, err = NewPromQLMetricSource(api.Config{
			Address: *prometheusURL,
		}, expr)
		if err != nil {
			panic(err)
		}
	}

	return NewBinaryMetricDispatchGateWithSource(source)
}
