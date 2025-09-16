// Copyright 2025 Google LLC All Rights Reserved.
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

package gameserversets

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	agonesv1 "agones.dev/agones/pkg/apis/agones/v1"
	mt "agones.dev/agones/pkg/metrics"
	agtesting "agones.dev/agones/pkg/testing"
	"agones.dev/agones/pkg/util/httpserver"
	utilruntime "agones.dev/agones/pkg/util/runtime"
	"agones.dev/agones/test/e2e/framework"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func resetMetrics() {
	unRegisterViews()
	registerViews()
}

func TestGSSMetrics(t *testing.T) {
	resetMetrics()

	utilruntime.FeatureTestMutex.Lock()
	defer utilruntime.FeatureTestMutex.Unlock()

	conf := mt.Config{
		PrometheusMetrics: true,
	}
	server := &httpserver.Server{
		Port:   "3001",
		Logger: framework.TestLogger(t),
	}

	health, closer := mt.SetupMetrics(conf, server)
	defer t.Cleanup(closer)

	assert.NotNil(t, health, "Health check handler should not be nil")
	server.Handle("/", health)

	gsSet := defaultFixture()
	c, m := newFakeController()
	expected := 10
	count := 0

	m.AgonesClient.AddReactor("create", "gameservers", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateAction)
		gs := ca.GetObject().(*agonesv1.GameServer)

		assert.True(t, metav1.IsControlledBy(gs, gsSet))
		count++

		return true, gs, nil
	})

	ctx, cancel := agtesting.StartInformers(m)
	defer cancel()

	err := c.addMoreGameServers(ctx, gsSet, expected)
	assert.Nil(t, err)
	assert.Equal(t, expected, count)
	agtesting.AssertEventContains(t, m.FakeRecorder.Events, "SuccessfulCreate")

	ctxHTTP, cancelHTTP := context.WithCancel(context.Background())
	defer cancelHTTP()

	// Start the HTTP server
	go func() {
		_ = server.Run(ctxHTTP, 0)
	}()
	time.Sleep(300 * time.Millisecond)

	resp, err := http.Get("http://localhost:3001/metrics")
	require.NoError(t, err, "Failed to GET metrics endpoint")
	defer func() {
		assert.NoError(t, resp.Body.Close())
	}()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected status code 200")

	metricsSet := collectMetricNames(resp)
	expectedMetrics := getMetricNames()

	for _, metric := range expectedMetrics {
		assert.Contains(t, metricsSet, metric, "Missing expected metric: %s", metric)
	}
}

// getMetricNames returns all metric view names.
func getMetricNames() []string {
	var metricNames []string
	// Expect the known set of OpenTelemetry metric names emitted by the controller
	metricNames = append(metricNames,
		"agones_fleet_rollout_percent",
		"agones_fleets_replicas_count",
		"agones_fleet_autoscalers_buffer_limits",
		"agones_fleet_autoscalers_buffer_size",
		"agones_fleet_autoscalers_current_replicas_count",
		"agones_fleet_autoscalers_desired_replicas_count",
		"agones_fleet_autoscalers_able_to_scale",
		"agones_fleet_autoscalers_limited",
		"agones_fleet_counters",
		"agones_fleet_lists",
		"agones_gameservers_count",
		"agones_gameservers_total",
		"agones_gameserver_player_connected_total",
		"agones_gameserver_player_capacity_total",
		"agones_nodes_count",
		"agones_gameservers_node_count",
		"agones_gameserver_state_duration",
		"agones_gameservers_node_count_histogram",
	)
	return metricNames
}

func collectMetricNames(resp *http.Response) map[string]bool {
	metrics := make(map[string]bool)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) > 0 {
			// Extract only the metric name, excluding labels
			metricName := fields[0]
			if idx := strings.Index(metricName, "{"); idx != -1 {
				metricName = metricName[:idx]
			}
			metrics[metricName] = true
		}
	}
	return metrics
}
