// Package metrics provides metrics registration for the async processor.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	controllerruntime "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// SchedulerSubsystem is the metric prefix of the package.
	SchedulerSubsystem = "llm_d_async"
)

var (
	Retries = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_request_retries_total",
		Help: "Total number of async request retries.",
	})
	AsyncReqs = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_request_total",
		Help: "Total number of async requests.",
	})
	ExceededDeadlineReqs = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_exceeded_deadline_requests_total",
		Help: "Total number of async requests that exceeded their deadline.",
	})
	FailedReqs = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_failed_requests_total",
		Help: "Total number of async requests that failed.",
	})
	SuccessfulReqs = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_successful_requests_total",
		Help: "Total number of async requests that succeeded.",
	})
	SheddedRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_shedded_requests_total",
		Help: "Total number of async requests that were shedded.",
	})
	MessageLatencyTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Subsystem: SchedulerSubsystem, Name: "async_message_latency_time_millis",
		Help:    "Time from message publish to message being succefully processed.",
		Buckets: []float64{100, 1000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000},
	})
)

// GetCollectors returns all custom collectors for the async processor.
func GetAsyncProcessorCollectors(supportsMessageLatency bool) []prometheus.Collector {
	collectors := []prometheus.Collector{
		Retries, AsyncReqs, ExceededDeadlineReqs, FailedReqs, SuccessfulReqs, SheddedRequests,
	}
	if supportsMessageLatency {
		collectors = append(collectors, MessageLatencyTime)
	}
	return collectors
}

var registerMetrics sync.Once

// Register all metrics.
func Register(customCollectors ...prometheus.Collector) {
	registerMetrics.Do(func() {
		for _, collector := range customCollectors {
			controllerruntime.Registry.MustRegister(collector)
		}
	})
}
