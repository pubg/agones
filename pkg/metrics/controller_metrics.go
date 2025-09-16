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
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	fleetRolloutPercent                     = "fleet_rollout_percent"
	fleetReplicaCountName                   = "fleets_replicas_count"
	fleetAutoscalerBufferLimitName          = "fleet_autoscalers_buffer_limits"
	fleetAutoscalterBufferSizeName          = "fleet_autoscalers_buffer_size"
	fleetAutoscalerCurrentReplicaCountName  = "fleet_autoscalers_current_replicas_count"
	fleetAutoscalersDesiredReplicaCountName = "fleet_autoscalers_desired_replicas_count"
	fleetAutoscalersAbleToScaleName         = "fleet_autoscalers_able_to_scale"
	fleetAutoscalersLimitedName             = "fleet_autoscalers_limited"
	fleetCountersName                       = "fleet_counters"
	fleetListsName                          = "fleet_lists"
	gameServersCountName                    = "gameservers_count"
	gameServersTotalName                    = "gameservers_total"
	gameServersPlayerConnectedTotalName     = "gameserver_player_connected_total"
	gameServersPlayerCapacityTotalName      = "gameserver_player_capacity_total"
	nodeCountName                           = "nodes_count"
	gameServersNodeCountName                = "gameservers_node_count"
	gameServerStateDurationName             = "gameserver_state_duration"
)

var (
	// fleetAutoscalerViews are metric views associated with FleetAutoscalers
	fleetAutoscalerViews = []string{fleetAutoscalerBufferLimitName, fleetAutoscalterBufferSizeName, fleetAutoscalerCurrentReplicaCountName,
		fleetAutoscalersDesiredReplicaCountName, fleetAutoscalersAbleToScaleName, fleetAutoscalersLimitedName}
	// fleetViews are metric views associated with Fleets
	fleetViews = append([]string{fleetRolloutPercent, fleetReplicaCountName, gameServersCountName, gameServersTotalName, gameServersPlayerConnectedTotalName, gameServersPlayerCapacityTotalName, gameServerStateDurationName, fleetCountersName, fleetListsName}, fleetAutoscalerViews...)

	// OpenTelemetry metrics
	metricsOnce sync.Once
	meter       metric.Meter

	// Gauges for current state values
	fleetRolloutPercentGauge            metric.Int64Gauge
	fleetsReplicasCountGauge            metric.Int64Gauge
	fasBufferLimitsCountGauge           metric.Int64Gauge
	fasBufferSizeGauge                  metric.Int64Gauge
	fasCurrentReplicasGauge             metric.Int64Gauge
	fasDesiredReplicasGauge             metric.Int64Gauge
	fasAbleToScaleGauge                 metric.Int64Gauge
	fasLimitedGauge                     metric.Int64Gauge
	fleetCountersGauge                  metric.Int64Gauge
	fleetListsGauge                     metric.Int64Gauge
	gameServerCountGauge                metric.Int64Gauge
	gameServerPlayerConnectedTotalGauge metric.Int64Gauge
	gameServerPlayerCapacityTotalGauge  metric.Int64Gauge
	nodesCountGauge                     metric.Int64Gauge
	gsPerNodesCountGauge                metric.Int64Gauge

	// Counters for totals
	gameServerTotalCounter metric.Int64Counter

	// Histograms for distributions
	gsPerNodesCountHistogram metric.Int64Histogram
	gsStateDurationHistogram metric.Float64Histogram
)

// InitializeMetrics initializes all OpenTelemetry metrics
func InitializeMetrics() {
	metricsOnce.Do(func() {
		meter = GetMeter("agones.dev/agones/pkg/metrics")

		var err error

		// Initialize gauges
		fleetRolloutPercentGauge, err = meter.Int64Gauge(
			fleetRolloutPercent,
			metric.WithDescription("The current fleet rollout percentage"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create fleet rollout percent gauge")
		}

		fleetsReplicasCountGauge, err = meter.Int64Gauge(
			fleetReplicaCountName,
			metric.WithDescription("The count of replicas per fleet"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create fleet replicas count gauge")
		}

		fasBufferLimitsCountGauge, err = meter.Int64Gauge(
			fleetAutoscalerBufferLimitName,
			metric.WithDescription("The buffer limits of autoscalers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create FAS buffer limits gauge")
		}

		fasBufferSizeGauge, err = meter.Int64Gauge(
			fleetAutoscalterBufferSizeName,
			metric.WithDescription("The buffer size value of autoscalers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create FAS buffer size gauge")
		}

		fasCurrentReplicasGauge, err = meter.Int64Gauge(
			fleetAutoscalerCurrentReplicaCountName,
			metric.WithDescription("The current replicas count as seen by autoscalers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create FAS current replicas gauge")
		}

		fasDesiredReplicasGauge, err = meter.Int64Gauge(
			fleetAutoscalersDesiredReplicaCountName,
			metric.WithDescription("The desired replicas count as seen by autoscalers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create FAS desired replicas gauge")
		}

		fasAbleToScaleGauge, err = meter.Int64Gauge(
			fleetAutoscalersAbleToScaleName,
			metric.WithDescription("The fleet autoscaler can access the fleet to scale (0 indicates false, 1 indicates true)"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create FAS able to scale gauge")
		}

		fasLimitedGauge, err = meter.Int64Gauge(
			fleetAutoscalersLimitedName,
			metric.WithDescription("The fleet autoscaler is capped (0 indicates false, 1 indicates true)"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create FAS limited gauge")
		}

		fleetCountersGauge, err = meter.Int64Gauge(
			fleetCountersName,
			metric.WithDescription("Aggregated Counters counts and capacity across GameServers in the Fleet"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create fleet counters gauge")
		}

		fleetListsGauge, err = meter.Int64Gauge(
			fleetListsName,
			metric.WithDescription("Aggregated Lists counts and capacity across GameServers in the Fleet"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create fleet lists gauge")
		}

		gameServerCountGauge, err = meter.Int64Gauge(
			gameServersCountName,
			metric.WithDescription("The count of gameservers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver count gauge")
		}

		gameServerPlayerConnectedTotalGauge, err = meter.Int64Gauge(
			gameServersPlayerConnectedTotalName,
			metric.WithDescription("The total number of players connected to gameservers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver player connected gauge")
		}

		gameServerPlayerCapacityTotalGauge, err = meter.Int64Gauge(
			gameServersPlayerCapacityTotalName,
			metric.WithDescription("The available player capacity for gameservers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver player capacity gauge")
		}

		nodesCountGauge, err = meter.Int64Gauge(
			nodeCountName,
			metric.WithDescription("The count of nodes in the cluster"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create nodes count gauge")
		}

		gsPerNodesCountGauge, err = meter.Int64Gauge(
			gameServersNodeCountName,
			metric.WithDescription("The count of gameservers per node in the cluster"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameservers per node gauge")
		}

		// Initialize counters
		gameServerTotalCounter, err = meter.Int64Counter(
			gameServersTotalName,
			metric.WithDescription("The total of gameservers"),
			metric.WithUnit("1"),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver total counter")
		}

		// Initialize histograms
		gsPerNodesCountHistogram, err = meter.Int64Histogram(
			gameServersNodeCountName+"_histogram",
			metric.WithDescription("The distribution of gameservers per node in the cluster"),
			metric.WithUnit("1"),
			metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 32, 40, 50, 60, 70, 80, 90, 100, 110, 120),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameservers per node histogram")
		}

		gsStateDurationHistogram, err = meter.Float64Histogram(
			gameServerStateDurationName,
			metric.WithDescription("The time gameserver exists in the current state in seconds"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384),
		)
		if err != nil {
			logger.WithError(err).Error("failed to create gameserver state duration histogram")
		}
	})
}

// RecordFleetRolloutPercent records the fleet rollout percentage
func RecordFleetRolloutPercent(ctx context.Context, value int64, name, fleetType, namespace string) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyName, name),
		attribute.String(keyType, fleetType),
		attribute.String(keyNamespace, namespace),
	}
	fleetRolloutPercentGauge.Record(ctx, value, metric.WithAttributes(attrs...))
}

// RecordFleetReplicasCount records the fleet replicas count
func RecordFleetReplicasCount(ctx context.Context, value int64, name, fleetType, namespace string) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyName, name),
		attribute.String(keyType, fleetType),
		attribute.String(keyNamespace, namespace),
	}
	fleetsReplicasCountGauge.Record(ctx, value, metric.WithAttributes(attrs...))
}

// RecordGameServerCount records the gameserver count
func RecordGameServerCount(ctx context.Context, value int64, fleetType, fleetName, namespace string) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyType, fleetType),
		attribute.String(keyFleetName, fleetName),
		attribute.String(keyNamespace, namespace),
	}
	gameServerCountGauge.Record(ctx, value, metric.WithAttributes(attrs...))
}

// RecordGameServerTotal records the gameserver total
func RecordGameServerTotal(ctx context.Context, fleetType, fleetName, namespace string) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyType, fleetType),
		attribute.String(keyFleetName, fleetName),
		attribute.String(keyNamespace, namespace),
	}
	gameServerTotalCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordGameServerStateDuration records the gameserver state duration
func RecordGameServerStateDuration(ctx context.Context, value float64, fleetType, fleetName, namespace string) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyType, fleetType),
		attribute.String(keyFleetName, fleetName),
		attribute.String(keyNamespace, namespace),
	}
	gsStateDurationHistogram.Record(ctx, value, metric.WithAttributes(attrs...))
}

// RecordNodesCount records the node count
func RecordNodesCount(ctx context.Context, value int64) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String(keyEmpty, ""),
	}
	nodesCountGauge.Record(ctx, value, metric.WithAttributes(attrs...))
}

// RecordGameServersPerNodeCount records gameservers per node count
func RecordGameServersPerNodeCount(ctx context.Context, value int64, nodeName string) {
	InitializeMetrics()
	attrs := []attribute.KeyValue{
		attribute.String("node_name", nodeName),
	}
	gsPerNodesCountGauge.Record(ctx, value, metric.WithAttributes(attrs...))
	gsPerNodesCountHistogram.Record(ctx, value, metric.WithAttributes(attrs...))
}

// register all our state views to OpenTelemetry - this is a no-op now since metrics are created on demand
func registerViews() {
	InitializeMetrics()
}

// unregister views, this is only useful for tests as it trigger reporting.
// In OpenTelemetry, this is a no-op since metrics are managed by the MeterProvider
func unRegisterViews() {
	// No-op in OpenTelemetry
}

// resetViews resets the values of metrics.
// In OpenTelemetry, this is a no-op since we don't have direct control over metric values
func resetViews(names []string) {
	// No-op in OpenTelemetry - metrics are managed by the MeterProvider
}
