// Copyright 2018 Google LLC All Rights Reserved.
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
	"net/http"
	"os"
	"strings"
	"time"

	"agones.dev/agones/pkg/util/httpserver"
	"cloud.google.com/go/compute/metadata"
	"github.com/heptiolabs/healthcheck"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"

	// metric import not directly needed here
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

// Config holds configuration for metrics reporting
type Config struct {
	GCPProjectID      string
	OTLPLabels        string
	OTLP              bool
	PrometheusMetrics bool
}

// RegisterPrometheusExporter register a prometheus exporter to OpenTelemetry with a given prometheus metric registry.
// It will automatically add go runtime and process metrics using default prometheus collectors.
// The function return an http.handler that you can use to expose the prometheus endpoint.
func RegisterPrometheusExporter(registry *prom.Registry) (http.Handler, *sdkmetric.MeterProvider, error) {
	if err := registry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, nil, err
	}
	if err := registry.Register(collectors.NewGoCollector()); err != nil {
		return nil, nil, err
	}

	// Create a new Prometheus exporter
	exporter, err := prometheus.New(
		prometheus.WithRegisterer(registry),
		prometheus.WithNamespace("agones"),
	)
	if err != nil {
		return nil, nil, err
	}

	// Create a meter provider with the Prometheus exporter
	res, err := resource.New(context.Background(), resource.WithFromEnv())
	if err != nil {
		return nil, nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)

	// Expose the configured registry via a minimal handler without promhttp
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mfs, err := registry.Gather()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", string(expfmt.FmtText))
		enc := expfmt.NewEncoder(w, expfmt.FmtText)
		for _, mf := range mfs {
			_ = enc.Encode(mf)
		}
	})
	return handler, meterProvider, nil
}

// RegisterOTLPExporter register an OTLP exporter to OpenTelemetry.
// It will send Agones metrics via OTLP protocol.
func RegisterOTLPExporter(ctx context.Context, projectID string, defaultLabels string) (sdkmetric.Exporter, *sdkmetric.MeterProvider, error) {
	monitoredRes, err := getMonitoredResource(projectID)
	if err != nil {
		logger.WithError(err).Warn("error discovering monitored resource")
	}

	// Create OTLP metric exporter via gRPC or HTTP based on OTEL_* protocol env
	exporter, err := newOTLPMetricsExporter(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Create resource attributes; start with dynamic/env-detected values.
	attributes := []attribute.KeyValue{}

	// Add monitored resource attributes
	if monitoredRes != nil {
		for k, v := range monitoredRes.Labels {
			attributes = append(attributes, attribute.String(k, v))
		}
	}

	// Parse and add default labels (legacy Agones config)
	if defaultLabels != "" {
		labels, err := parseOTLPLabels(defaultLabels)
		if err != nil {
			return nil, nil, err
		}
		attributes = append(attributes, labels...)
	}

	// Parse and add OTEL_RESOURCE_ATTRIBUTES per spec (k=v,k2=v2)
	if ra := os.Getenv("OTEL_RESOURCE_ATTRIBUTES"); ra != "" {
		labels, err := parseOTLPLabels(ra)
		if err != nil {
			return nil, nil, err
		}
		attributes = append(attributes, labels...)
	}

	custom, err := resource.New(
		ctx,
		resource.WithContainer(),
		resource.WithProcess(),
		resource.WithAttributes(attributes...),
	)
	if err != nil {
		return nil, nil, err
	}
	res, err := resource.Merge(resource.Default(), custom)
	if err != nil {
		return nil, nil, err
	}

	// Determine export interval from OTEL_METRIC_EXPORT_INTERVAL (ms) if set
	interval := 60 * time.Second
	if v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); v != "" {
		// Spec defines milliseconds
		if ms, err := time.ParseDuration(v + "ms"); err == nil {
			if ms > 0 {
				interval = ms
			}
		}
	}

	// Create meter provider with OTLP exporter
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval))),
	)

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)

	return exporter, meterProvider, nil
}

// newOTLPMetricsExporter selects gRPC or HTTP metrics exporter based on env.
func newOTLPMetricsExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	// Use metrics-specific protocol first, then global protocol; default to grpc
	proto := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")))
	if proto == "" {
		proto = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	switch proto {
	case "http", "http/protobuf", "http_protobuf":
		return otlpmetrichttp.New(ctx)
	default: // grpc or empty
		return otlpmetricgrpc.New(ctx)
	}
}

// RegisterOTLPTraceExporter registers an OTLP trace exporter and installs a TracerProvider.
// Configuration (endpoint, TLS, headers, etc.) is taken from standard OTEL_* environment variables.
func RegisterOTLPTraceExporter(ctx context.Context, projectID string, defaultLabels string) (*otlptrace.Exporter, *trace.TracerProvider, error) {
	monitoredRes, err := getMonitoredResource(projectID)
	if err != nil {
		logger.WithError(err).Warn("error discovering monitored resource for traces")
	}

	exp, err := newOTLPTraceExporter(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Build resource attributes; start with dynamic/env-detected values
	attrs := []attribute.KeyValue{}
	if monitoredRes != nil {
		for k, v := range monitoredRes.Labels {
			attrs = append(attrs, attribute.String(k, v))
		}
	}
	if defaultLabels != "" {
		labels, err := parseOTLPLabels(defaultLabels)
		if err != nil {
			return nil, nil, err
		}
		attrs = append(attrs, labels...)
	}

	custom, err := resource.New(ctx,
		resource.WithContainer(),
		resource.WithProcess(),
		resource.WithAttributes(attrs...),
	)
	if err != nil {
		return nil, nil, err
	}
	res, err := resource.Merge(resource.Default(), custom)
	if err != nil {
		return nil, nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithBatcher(exp),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(newPropagator())

	return exp, tp, nil
}

// newOTLPTraceExporter selects gRPC or HTTP trace exporter based on env.
func newOTLPTraceExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	proto := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")))
	if proto == "" {
		proto = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	switch proto {
	case "http", "http/protobuf", "http_protobuf":
		return otlptracehttp.New(ctx)
	default: // grpc or empty
		return otlptracegrpc.New(ctx)
	}
}

// SetReportingPeriod is deprecated in OpenTelemetry as reporting periods are configured per reader.
// This function is kept for backward compatibility but does nothing.
func SetReportingPeriod(forPrometheus, forOTLP bool) {
	// In OpenTelemetry, reporting periods are configured when creating readers
	// Prometheus exporter reports on scrape, OTLP uses periodic reader with configurable interval
	logger.Info("SetReportingPeriod is deprecated with OpenTelemetry, configure readers directly")
}

// isSDKDisabled returns true if OTEL_SDK_DISABLED is truthy per spec
func isSDKDisabled() bool {
	v := os.Getenv("OTEL_SDK_DISABLED")
	switch v {
	case "1", "true", "TRUE", "True", "yes", "YES", "on", "ON":
		return true
	default:
		return false
	}
}

func getMonitoredResource(projectID string) (*monitoredres.MonitoredResource, error) {
	zone, err := metadata.ZoneWithContext(context.TODO())
	if err != nil {
		return nil, errors.Wrap(err, "error getting zone")
	}
	clusterName, err := metadata.InstanceAttributeValueWithContext(context.TODO(), "cluster-name")
	if err != nil {
		return nil, errors.Wrap(err, "error getting cluster-name")
	}

	return &monitoredres.MonitoredResource{
		Type: "k8s_container",
		Labels: map[string]string{
			"project_id":   projectID,
			"location":     zone,
			"cluster_name": clusterName,

			// See: https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
			"namespace_name": os.Getenv("POD_NAMESPACE"),
			"pod_name":       os.Getenv("POD_NAME"),
			"container_name": os.Getenv("CONTAINER_NAME"),
		},
	}, nil
}

// SetupMetrics initializes metrics reporting with the provided configuration
func SetupMetrics(conf Config, server *httpserver.Server) (healthcheck.Handler, func()) {
	var health healthcheck.Handler
	var closer = func() {}
	ctx := context.Background()

	// Respect OTEL_SDK_DISABLED to entirely disable the SDK
	if isSDKDisabled() {
		health = healthcheck.NewHandler()
		return health, closer
	}

	// OTLP Metrics
	if conf.OTLP {
		exporter, meterProvider, err := RegisterOTLPExporter(ctx, conf.GCPProjectID, conf.OTLPLabels)
		if err != nil {
			logger.WithError(err).Fatal("Could not register OTLP exporter")
		}
		closer = func() {
			if err := exporter.Shutdown(ctx); err != nil {
				logger.WithError(err).Error("Failed to shutdown OTLP exporter")
			}
			if err := meterProvider.Shutdown(ctx); err != nil {
				logger.WithError(err).Error("Failed to shutdown meter provider")
			}
		}
	}

	// Prometheus Metrics
	if conf.PrometheusMetrics {
		registry := prom.NewRegistry()
		metricHandler, meterProvider, err := RegisterPrometheusExporter(registry)
		if err != nil {
			logger.WithError(err).Fatal("Could not register Prometheus exporter")
		}
		server.Handle("/metrics", metricHandler)
		health = healthcheck.NewMetricsHandler(registry, "agones")
		if closer == nil {
			closer = func() {
				if err := meterProvider.Shutdown(ctx); err != nil {
					logger.WithError(err).Error("Failed to shutdown meter provider")
				}
			}
		}
	} else {
		health = healthcheck.NewHandler()
	}

	return health, closer
}

// SetupTracing initializes trace exporting via OTLP if enabled in the provided config (conf.OTLP).
// Returns a closer function to flush and shutdown the tracing provider on exit.
func SetupTracing(conf Config) func() {
	if !conf.OTLP {
		return func() {}
	}
	ctx := context.Background()
	exp, tp, err := RegisterOTLPTraceExporter(ctx, conf.GCPProjectID, conf.OTLPLabels)
	if err != nil {
		logger.WithError(err).Fatal("Could not register OTLP trace exporter")
	} else {
		logger.Info("OTLP trace exporter registered")
	}
	return func() {
		logger.Info("Shutting down trace provider")
		if err := tp.Shutdown(ctx); err != nil {
			logger.WithError(err).Error("Failed to shutdown trace provider")
		}
		if err := exp.Shutdown(ctx); err != nil {
			logger.WithError(err).Error("Failed to shutdown trace exporter")
		}
	}
}

// newPropagator builds a text map propagator based on OTEL_PROPAGATORS env var.
// Defaults to tracecontext + baggage + b3 (single+multi) if unset or invalid.
func newPropagator() propagation.TextMapPropagator {
	val := strings.TrimSpace(strings.ToLower(os.Getenv("OTEL_PROPAGATORS")))
	if val == "" {
		return propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
			b3.New(b3.WithInjectEncoding(b3.B3SingleHeader)),
			b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)),
		)
	}
	parts := strings.Split(val, ",")
	var list []propagation.TextMapPropagator
	for _, p := range parts {
		switch strings.TrimSpace(p) {
		case "tracecontext":
			list = append(list, propagation.TraceContext{})
		case "baggage":
			list = append(list, propagation.Baggage{})
		case "b3":
			list = append(list, b3.New())
		case "b3multi", "b3multiheader":
			list = append(list, b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)))
		}
	}
	if len(list) == 0 {
		return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}
	return propagation.NewCompositeTextMapPropagator(list...)
}
