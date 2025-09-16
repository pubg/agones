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
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"agones.dev/agones/pkg/util/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	logger = runtime.NewLoggerWithSource("metrics")

	keyName       = "name"
	keyNamespace  = "namespace"
	keyFleetName  = "fleet_name"
	keyType       = "type"
	keyStatusCode = "status_code"
	keyVerb       = "verb"
	keyEndpoint   = "endpoint"
	keyEmpty      = "empty"
	keyCounter    = "counter"
	keyList       = "list"
)

// RecordCounter records a counter metric with attributes
func RecordCounter(ctx context.Context, counter metric.Int64Counter, value int64, attrs ...attribute.KeyValue) {
	counter.Add(ctx, value, metric.WithAttributes(attrs...))
}

// RecordUpDownCounter records an up/down counter metric with attributes
func RecordUpDownCounter(ctx context.Context, counter metric.Int64UpDownCounter, value int64, attrs ...attribute.KeyValue) {
	counter.Add(ctx, value, metric.WithAttributes(attrs...))
}

// RecordHistogram records a histogram metric with attributes
func RecordHistogram(ctx context.Context, histogram metric.Int64Histogram, value int64, attrs ...attribute.KeyValue) {
	histogram.Record(ctx, value, metric.WithAttributes(attrs...))
}

// RecordGauge records a gauge metric with attributes
func RecordGauge(ctx context.Context, gauge metric.Int64Gauge, value int64, attrs ...attribute.KeyValue) {
	gauge.Record(ctx, value, metric.WithAttributes(attrs...))
}

// GetMeter returns the global meter for the given name
func GetMeter(name string) metric.Meter {
	return otel.Meter(name)
}

// CreateAttributes creates OpenTelemetry attributes from key-value pairs
func CreateAttributes(tags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}

func parseOTLPLabels(s string) ([]attribute.KeyValue, error) {
	var attributes []attribute.KeyValue
	if s == "" {
		return attributes, nil
	}
	pairs := strings.Split(s, ",")
	for _, p := range pairs {
		keyValue := strings.Split(p, "=")
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid labels: %s, expect key=value,key2=value2", s)
		}
		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])

		if key == "" {
			return nil, errors.New("invalid key: can not be empty")
		}

		if value == "" {
			return nil, fmt.Errorf("invalid value for key %s: can not be empty", key)
		}

		if !utf8.ValidString(key) {
			return nil, fmt.Errorf("invalid key: %s, must be a valid utf-8 string", key)
		}

		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("invalid value: %s, must be a valid utf-8 string", value)
		}

		attributes = append(attributes, attribute.String(key, value))
	}
	return attributes, nil
}
