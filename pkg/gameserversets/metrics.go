// Copyright 2024 Google LLC All Rights Reserved.
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
	"context"
	"time"

	listerv1 "agones.dev/agones/pkg/client/listers/agones/v1"
	"agones.dev/agones/pkg/util/runtime"
	"github.com/sirupsen/logrus"
)

var (
	logger = runtime.NewLoggerWithSource("metrics")

	keyName      = "name"
	keyNamespace = "namespace"
	keyFleetName = "fleet_name"
	keyType      = "type"

	// deprecated in favor of OpenTelemetry metrics in pkg/gameserversets/controller_metrics.go
)

// register all our state views to OpenCensus
func registerViews() {}

// unregister views, this is only useful for tests as it trigger reporting.
func unRegisterViews() {}

// default set of tags for latency metric
var latencyTags = []interface{}{"deprecated"}

type metrics struct {
	ctx              context.Context
	gameServerLister listerv1.GameServerLister
	logger           *logrus.Entry
	start            time.Time
}

// record the current current gameserver creation latency
func (r *metrics) record() {}

// mutate the current set of metric tags
func (r *metrics) mutate(_ ...interface{}) {}

// setError set the latency status tag as error.
func (r *metrics) setError(errorType string) { _ = errorType }

// setRequest set request metric tags.
func (r *metrics) setRequest(count int) { _ = count }
