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

package gameserverallocations

import (
	"context"
	"strconv"
	"sync"
	"time"

	agonesv1 "agones.dev/agones/pkg/apis/agones/v1"
	allocationv1 "agones.dev/agones/pkg/apis/allocation/v1"
	listerv1 "agones.dev/agones/pkg/client/listers/agones/v1"
	mt "agones.dev/agones/pkg/metrics"
	"agones.dev/agones/pkg/util/runtime"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

var (
	logger = runtime.NewLoggerWithSource("metrics")

	// OpenTelemetry metrics
	allocationMetricsOnce sync.Once
	allocationMeter       metric.Meter

	gameServerAllocationsLatencyHistogram     metric.Float64Histogram
	gameServerAllocationsRetryHistogram       metric.Int64Histogram
	gameServerAllocationsPendingRequestsGauge metric.Int64Gauge

	// Attribute keys
	keyFleetName          = "fleet_name"
	keyClusterName        = "cluster_name"
	keyMultiCluster       = "is_multicluster"
	keyStatus             = "status"
	keySchedulingStrategy = "scheduling_strategy"
)

// InitializeAllocationMetrics initializes OpenTelemetry metrics for allocations
func InitializeAllocationMetrics() {
	allocationMetricsOnce.Do(func() {
		allocationMeter = mt.GetMeter("agones.dev/agones/pkg/gameserverallocations")

		var err error

		gameServerAllocationsLatencyHistogram, err = allocationMeter.Float64Histogram(
			"gameserver_allocations_duration_seconds",
			metric.WithDescription("The distribution of gameserver allocation requests latencies"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2, 3),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver allocations latency histogram")
		}

		gameServerAllocationsRetryHistogram, err = allocationMeter.Int64Histogram(
			"gameserver_allocations_retry_total",
			metric.WithDescription("The count of gameserver allocation retry until it succeeds"),
			metric.WithUnit("1"),
			metric.WithExplicitBucketBoundaries(1, 2, 3, 4, 5),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver allocations retry histogram")
		}

		gameServerAllocationsPendingRequestsGauge, err = allocationMeter.Int64Gauge(
			"gameserver_allocations_pending_requests",
			metric.WithDescription("Current number of pending allocation requests awaiting batch processing"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver allocations pending requests gauge")
		}
	})
}

// register all our state views to OpenTelemetry
func registerViews() {
	InitializeAllocationMetrics()
}

// unregister views - no-op with OpenTelemetry
func unRegisterViews() {}

// default set of attributes for latency metric
var defaultAttributes = []attribute.KeyValue{
	attribute.String(keyMultiCluster, "none"),
	attribute.String(keyClusterName, "none"),
	attribute.String(keySchedulingStrategy, "none"),
	attribute.String(keyFleetName, "none"),
	attribute.String(keyStatus, "none"),
}

type metrics struct {
	ctx              context.Context
	gameServerLister listerv1.GameServerLister
	logger           *logrus.Entry
	start            time.Time
	attributes       []attribute.KeyValue
}

// mutate the current set of metric attributes
func (r *metrics) mutate(attrs ...attribute.KeyValue) {
	// Update or add attributes
	for _, newAttr := range attrs {
		found := false
		for i, existingAttr := range r.attributes {
			if existingAttr.Key == newAttr.Key {
				r.attributes[i] = newAttr
				found = true
				break
			}
		}
		if !found {
			r.attributes = append(r.attributes, newAttr)
		}
	}
}

// setStatus set the latency status attribute.
func (r *metrics) setStatus(status string) {
	r.mutate(attribute.String(keyStatus, status))
}

// setError set the latency status attribute as error.
func (r *metrics) setError() {
	r.mutate(attribute.String(keyStatus, "error"))
}

// setRequest set request metric attributes.
func (r *metrics) setRequest(in *allocationv1.GameServerAllocation) {
	attrs := []attribute.KeyValue{
		attribute.String(keySchedulingStrategy, string(in.Spec.Scheduling)),
		attribute.String(keyMultiCluster, strconv.FormatBool(in.Spec.MultiClusterSetting.Enabled)),
	}
	r.mutate(attrs...)
}

// setResponse set response metric attributes.
func (r *metrics) setResponse(o k8sruntime.Object) {
	out, ok := o.(*allocationv1.GameServerAllocation)
	if out == nil || !ok {
		return
	}
	r.setStatus(string(out.Status.State))
	var attrs []attribute.KeyValue
	// sets the fleet name attribute if possible
	if out.Status.State == allocationv1.GameServerAllocationAllocated && out.Status.Source == localAllocationSource {
		gs, err := r.gameServerLister.GameServers(out.Namespace).Get(out.Status.GameServerName)
		if err != nil {
			r.logger.WithError(err).Warnf("failed to get gameserver:%s namespace:%s", out.Status.GameServerName, out.Namespace)
			return
		}
		fleetName := gs.Labels[agonesv1.FleetNameLabel]
		if fleetName != "" {
			attrs = append(attrs, attribute.String(keyFleetName, fleetName))
		}
	}
	r.mutate(attrs...)
}

// record the current allocation latency.
func (r *metrics) record() {
	InitializeAllocationMetrics()
	duration := time.Since(r.start).Seconds()
	gameServerAllocationsLatencyHistogram.Record(r.ctx, duration, metric.WithAttributes(r.attributes...))
}

// record the current allocation retry rate.
func (r *metrics) recordAllocationRetrySuccess(ctx context.Context, retryCount int) {
	InitializeAllocationMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyStatus, "Success"),
	}
	gameServerAllocationsRetryHistogram.Record(ctx, int64(retryCount), metric.WithAttributes(attrs...))
}

func (r *metrics) recordPendingRequestsGauge(depth int) {
	if r == nil {
		return
	}
	InitializeAllocationMetrics()
	if gameServerAllocationsPendingRequestsGauge == nil {
		return
	}
	gameServerAllocationsPendingRequestsGauge.Record(r.ctx, int64(depth), metric.WithAttributes(r.attributes...))
}
