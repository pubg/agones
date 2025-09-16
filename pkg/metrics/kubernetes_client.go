// Copyright 2019 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"context"
	"net/url"
	"time"

	"agones.dev/agones/pkg/util/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/util/workqueue"
)

var (
	keyQueueName = "queue_name"

	// OTel instruments
	httpRequestTotalCounter metric.Int64Counter
	httpRequestLatencyHist  metric.Float64Histogram

	cacheListTotalCounter           metric.Int64Counter
	cacheListLatencyHist            metric.Float64Histogram
	cacheListItemCountHist          metric.Float64Histogram
	cacheWatchesTotalCounter        metric.Int64Counter
	cacheShortWatchesTotalCounter   metric.Int64Counter
	cacheWatchesLatencyHist         metric.Float64Histogram
	cacheItemsInWatchesCountCounter metric.Int64Counter
	cacheLastResourceVersionGauge   metric.Int64Gauge

	workQueueDepthGauge                   metric.Int64Gauge
	workQueueItemsTotalCounter            metric.Int64Counter
	workQueueLatencyHist                  metric.Float64Histogram
	workQueueWorkDurationHist             metric.Float64Histogram
	workQueueRetriesTotalCounter          metric.Int64Counter
	workQueueLongestRunningProcessorGauge metric.Int64Gauge
	workQueueUnfinishedWorkGauge          metric.Int64Gauge
)

func init() {
	// Initialize OTel instruments
	m := GetMeter("agones.dev/agones/pkg/metrics/kubernetes_client")
	var err error

	httpRequestTotalCounter, err = m.Int64Counter("k8s_client_http_request_total")
	runtime.Must(err)
	httpRequestLatencyHist, err = m.Float64Histogram("k8s_client_http_request_duration_seconds")
	runtime.Must(err)

	cacheListTotalCounter, err = m.Int64Counter("k8s_client_cache_list_total")
	runtime.Must(err)
	cacheListLatencyHist, err = m.Float64Histogram("k8s_client_cache_list_duration_seconds")
	runtime.Must(err)
	cacheListItemCountHist, err = m.Float64Histogram("k8s_client_cache_list_items")
	runtime.Must(err)
	cacheWatchesTotalCounter, err = m.Int64Counter("k8s_client_cache_watches_total")
	runtime.Must(err)
	cacheShortWatchesTotalCounter, err = m.Int64Counter("k8s_client_cache_short_watches_total")
	runtime.Must(err)
	cacheWatchesLatencyHist, err = m.Float64Histogram("k8s_client_cache_watch_duration_seconds")
	runtime.Must(err)
	cacheItemsInWatchesCountCounter, err = m.Int64Counter("k8s_client_cache_watch_events_total")
	runtime.Must(err)
	cacheLastResourceVersionGauge, err = m.Int64Gauge("k8s_client_cache_last_resource_version")
	runtime.Must(err)

	workQueueDepthGauge, err = m.Int64Gauge("k8s_client_workqueue_depth")
	runtime.Must(err)
	workQueueItemsTotalCounter, err = m.Int64Counter("k8s_client_workqueue_items_total")
	runtime.Must(err)
	workQueueLatencyHist, err = m.Float64Histogram("k8s_client_workqueue_latency_seconds")
	runtime.Must(err)
	workQueueWorkDurationHist, err = m.Float64Histogram("k8s_client_workqueue_work_duration_seconds")
	runtime.Must(err)
	workQueueRetriesTotalCounter, err = m.Int64Counter("k8s_client_workqueue_retries_total")
	runtime.Must(err)
	workQueueLongestRunningProcessorGauge, err = m.Int64Gauge("k8s_client_workqueue_longest_running_processor")
	runtime.Must(err)
	workQueueUnfinishedWorkGauge, err = m.Int64Gauge("k8s_client_workqueue_unfinished_work_seconds")
	runtime.Must(err)

	clientGoRequest := &clientGoMetricAdapter{}
	clientGoRequest.Register()
}

// Definition of client-go metrics adapter for HTTP requests, caches and workerqueues observations
type clientGoMetricAdapter struct{}

func (c *clientGoMetricAdapter) Register() {
	metrics.Register(metrics.RegisterOpts{
		RequestLatency: c,
		RequestResult:  c,
	})
	workqueue.SetProvider(c)
}

func (clientGoMetricAdapter) Increment(ctx context.Context, code string, method string, _ string) {
	attrs := []attribute.KeyValue{attribute.String(keyStatusCode, code), attribute.String(keyVerb, method)}
	httpRequestTotalCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (clientGoMetricAdapter) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	// url is without {namespace} and {name}, so cardinality of resulting metrics is low.
	attrs := []attribute.KeyValue{attribute.String(keyVerb, verb), attribute.String(keyEndpoint, u.Path)}
	httpRequestLatencyHist.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
}

// otelMetric adapters to provide client-go metrics using OTel instruments
type otelMetric struct {
	recordFunc func(ctx context.Context, v float64)
	ctx        context.Context
}

func newOtelMetric(recorder func(ctx context.Context, v float64)) *otelMetric {
	return &otelMetric{recordFunc: recorder, ctx: context.Background()}
}

// withAttr is no-op helper retained for compatibility; attributes are applied at record time
func (m *otelMetric) withAttr(_ string, _ string) *otelMetric { return m }

func (m *otelMetric) Inc() {
	m.recordFunc(m.ctx, 1)
}

func (m *otelMetric) Dec() {
	m.recordFunc(m.ctx, -1)
}

// observeFunc is an adapter that allows the use of functions as summary metric.
// useful for converting metrics unit before sending them to OC
type observeFunc func(float64)

func (o observeFunc) Observe(f float64) {
	o(f)
}

func (m *otelMetric) Observe(f float64) {
	m.recordFunc(m.ctx, f)
}

func (m *otelMetric) Set(f float64) {
	m.recordFunc(m.ctx, f)
}

func (clientGoMetricAdapter) NewListsMetric(string) cache.CounterMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheListTotalCounter.Add(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewListDurationMetric(string) cache.SummaryMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheListLatencyHist.Record(ctx, v)
	})
}

func (clientGoMetricAdapter) NewItemsInListMetric(string) cache.SummaryMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheListItemCountHist.Record(ctx, v)
	})
}

func (clientGoMetricAdapter) NewWatchesMetric(string) cache.CounterMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheWatchesTotalCounter.Add(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewShortWatchesMetric(string) cache.CounterMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheShortWatchesTotalCounter.Add(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewWatchDurationMetric(string) cache.SummaryMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheWatchesLatencyHist.Record(ctx, v)
	})
}

func (clientGoMetricAdapter) NewItemsInWatchMetric(string) cache.SummaryMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheItemsInWatchesCountCounter.Add(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewLastResourceVersionMetric(string) cache.GaugeMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		cacheLastResourceVersionGauge.Record(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewDepthMetric(name string) workqueue.GaugeMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		workQueueDepthGauge.Record(ctx, int64(v), metric.WithAttributes(attribute.String(keyQueueName, name)))
	})
}

func (clientGoMetricAdapter) NewAddsMetric(name string) workqueue.CounterMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		workQueueItemsTotalCounter.Add(ctx, int64(v), metric.WithAttributes(attribute.String(keyQueueName, name)))
	})
}

func (clientGoMetricAdapter) NewLatencyMetric(name string) workqueue.HistogramMetric {
	// Convert microseconds to seconds for consistency across metrics.
	return observeFunc(func(f float64) {
		workQueueLatencyHist.Record(context.Background(), f/1e6, metric.WithAttributes(attribute.String(keyQueueName, name)))
	})
}

func (clientGoMetricAdapter) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	// Convert microseconds to seconds for consistency across metrics.
	return observeFunc(func(f float64) {
		workQueueWorkDurationHist.Record(context.Background(), f/1e6, metric.WithAttributes(attribute.String(keyQueueName, name)))
	})
}

func (clientGoMetricAdapter) NewRetriesMetric(name string) workqueue.CounterMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		workQueueRetriesTotalCounter.Add(ctx, int64(v), metric.WithAttributes(attribute.String(keyQueueName, name)))
	})
}

func (clientGoMetricAdapter) NewLongestRunningProcessorSecondsMetric(string) workqueue.SettableGaugeMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		workQueueLongestRunningProcessorGauge.Record(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewUnfinishedWorkSecondsMetric(string) workqueue.SettableGaugeMetric {
	return newOtelMetric(func(ctx context.Context, v float64) {
		workQueueUnfinishedWorkGauge.Record(ctx, int64(v))
	})
}

func (clientGoMetricAdapter) NewDeprecatedDepthMetric(name string) workqueue.GaugeMetric {
	return clientGoMetricAdapter{}.NewDepthMetric(name)
}

func (clientGoMetricAdapter) NewDeprecatedAddsMetric(name string) workqueue.CounterMetric {
	return clientGoMetricAdapter{}.NewAddsMetric(name)
}

func (clientGoMetricAdapter) NewDeprecatedLatencyMetric(name string) workqueue.SummaryMetric {
	return clientGoMetricAdapter{}.NewLatencyMetric(name)
}

func (clientGoMetricAdapter) NewDeprecatedLongestRunningProcessorMicrosecondsMetric(string) workqueue.SettableGaugeMetric {
	return clientGoMetricAdapter{}.NewLongestRunningProcessorSecondsMetric("")
}

func (clientGoMetricAdapter) NewDeprecatedRetriesMetric(name string) workqueue.CounterMetric {
	return clientGoMetricAdapter{}.NewRetriesMetric(name)
}

func (clientGoMetricAdapter) NewDeprecatedUnfinishedWorkSecondsMetric(string) workqueue.SettableGaugeMetric {
	return clientGoMetricAdapter{}.NewUnfinishedWorkSecondsMetric("")
}

func (clientGoMetricAdapter) NewDeprecatedWorkDurationMetric(name string) workqueue.SummaryMetric {
	return clientGoMetricAdapter{}.NewWorkDurationMetric(name)
}
