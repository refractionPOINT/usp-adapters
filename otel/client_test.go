package usp_otel

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func testClientOptions(t *testing.T) uspclient.ClientOptions {
	return uspclient.ClientOptions{
		TestSinkMode: true,
		OnError: func(err error) {
			t.Logf("ERROR: %v", err)
		},
		OnWarning: func(msg string) {
			t.Logf("WARNING: %s", msg)
		},
		DebugLog: func(msg string) {
			t.Logf("DEBUG: %s", msg)
		},
	}
}

func makeTraceID() []byte {
	b, _ := hex.DecodeString("0102030405060708090a0b0c0d0e0f10")
	return b
}

func makeSpanID() []byte {
	b, _ := hex.DecodeString("0102030405060708")
	return b
}

func makeParentSpanID() []byte {
	b, _ := hex.DecodeString("0807060504030201")
	return b
}

// --- Conversion tests ---

func TestConvertAttributes(t *testing.T) {
	attrs := []*commonpb.KeyValue{
		{Key: "string_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}},
		{Key: "int_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}},
		{Key: "double_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}},
		{Key: "bool_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		{Key: "array_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: []*commonpb.AnyValue{
					{Value: &commonpb.AnyValue_StringValue{StringValue: "a"}},
					{Value: &commonpb.AnyValue_IntValue{IntValue: 1}},
				},
			},
		}}},
		{Key: "kvlist_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
			KvlistValue: &commonpb.KeyValueList{
				Values: []*commonpb.KeyValue{
					{Key: "nested", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value"}}},
				},
			},
		}}},
		{Key: "bytes_key", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xDE, 0xAD}}}},
	}

	result := convertAttributes(attrs)

	assert.Equal(t, "hello", result["string_key"])
	assert.Equal(t, int64(42), result["int_key"])
	assert.Equal(t, 3.14, result["double_key"])
	assert.Equal(t, true, result["bool_key"])
	assert.Equal(t, []interface{}{"a", int64(1)}, result["array_key"])
	assert.Equal(t, map[string]interface{}{"nested": "value"}, result["kvlist_key"])
	assert.Equal(t, "3q0=", result["bytes_key"]) // base64 of 0xDEAD
}

func TestConvertAttributesNil(t *testing.T) {
	assert.Nil(t, convertAttributes(nil))
	assert.Nil(t, convertAttributes([]*commonpb.KeyValue{}))
}

func TestConvertAnyValueNil(t *testing.T) {
	assert.Nil(t, convertAnyValue(nil))
	assert.Nil(t, convertAnyValue(&commonpb.AnyValue{}))
}

func TestConvertResource(t *testing.T) {
	r := &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my-service"}}},
			{Key: "service.version", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "1.0.0"}}},
		},
	}
	result := convertResource(r)
	assert.Equal(t, "my-service", result["service.name"])
	assert.Equal(t, "1.0.0", result["service.version"])
}

func TestConvertResourceNil(t *testing.T) {
	assert.Nil(t, convertResource(nil))
}

func TestConvertScope(t *testing.T) {
	scope := &commonpb.InstrumentationScope{
		Name:    "my-library",
		Version: "2.0",
		Attributes: []*commonpb.KeyValue{
			{Key: "scope_attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "val"}}},
		},
	}
	result := convertScope(scope)
	assert.Equal(t, "my-library", result["name"])
	assert.Equal(t, "2.0", result["version"])
	assert.Equal(t, map[string]interface{}{"scope_attr": "val"}, result["attributes"])
}

func TestConvertScopeNil(t *testing.T) {
	assert.Nil(t, convertScope(nil))
}

func TestConvertSpan(t *testing.T) {
	traceID := makeTraceID()
	spanID := makeSpanID()
	parentSpanID := makeParentSpanID()

	span := &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		TraceState:        "key=value",
		Name:              "GET /api/users",
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: 1000000000000, // 1s in nanoseconds
		EndTimeUnixNano:   2000000000000, // 2s in nanoseconds
		Attributes: []*commonpb.KeyValue{
			{Key: "http.method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
			{Key: "http.status_code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 200}}},
		},
		Status: &tracepb.Status{
			Code:    tracepb.Status_STATUS_CODE_OK,
			Message: "success",
		},
		Events: []*tracepb.Span_Event{
			{
				Name:         "exception",
				TimeUnixNano: 1500000000000,
				Attributes: []*commonpb.KeyValue{
					{Key: "exception.message", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "not found"}}},
				},
				DroppedAttributesCount: 1,
			},
		},
		Links: []*tracepb.Span_Link{
			{
				TraceId:    traceID,
				SpanId:     spanID,
				TraceState: "linked",
				Attributes: []*commonpb.KeyValue{
					{Key: "link_attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "v"}}},
				},
				DroppedAttributesCount: 2,
			},
		},
		DroppedAttributesCount: 3,
		DroppedEventsCount:     4,
		DroppedLinksCount:      5,
	}

	result := convertSpan(span)

	assert.Equal(t, hex.EncodeToString(traceID), result["trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), result["span_id"])
	assert.Equal(t, hex.EncodeToString(parentSpanID), result["parent_span_id"])
	assert.Equal(t, "key=value", result["trace_state"])
	assert.Equal(t, "GET /api/users", result["name"])
	assert.Equal(t, "SPAN_KIND_SERVER", result["kind"])
	assert.Equal(t, uint64(1000000000000), result["start_time_unix_nano"])
	assert.Equal(t, uint64(2000000000000), result["end_time_unix_nano"])

	attrs := result["attributes"].(map[string]interface{})
	assert.Equal(t, "GET", attrs["http.method"])
	assert.Equal(t, int64(200), attrs["http.status_code"])

	status := result["status"].(map[string]interface{})
	assert.Equal(t, "STATUS_CODE_OK", status["code"])
	assert.Equal(t, "success", status["message"])

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	event := events[0].(map[string]interface{})
	assert.Equal(t, "exception", event["name"])
	assert.Equal(t, uint64(1500000000000), event["time_unix_nano"])
	assert.Equal(t, uint32(1), event["dropped_attributes_count"])

	links := result["links"].([]interface{})
	require.Len(t, links, 1)
	link := links[0].(map[string]interface{})
	assert.Equal(t, "linked", link["trace_state"])
	assert.Equal(t, uint32(2), link["dropped_attributes_count"])

	assert.Equal(t, uint32(3), result["dropped_attributes_count"])
	assert.Equal(t, uint32(4), result["dropped_events_count"])
	assert.Equal(t, uint32(5), result["dropped_links_count"])
}

func TestConvertSpanMinimal(t *testing.T) {
	span := &tracepb.Span{
		TraceId:           makeTraceID(),
		SpanId:            makeSpanID(),
		Name:              "simple",
		Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
		StartTimeUnixNano: 1000000000000,
		EndTimeUnixNano:   2000000000000,
	}
	result := convertSpan(span)
	assert.Equal(t, "simple", result["name"])
	assert.Nil(t, result["parent_span_id"])
	assert.Nil(t, result["trace_state"])
	assert.Nil(t, result["attributes"])
	assert.Nil(t, result["status"])
	assert.Nil(t, result["events"])
	assert.Nil(t, result["links"])
}

func TestConvertLogRecord(t *testing.T) {
	traceID := makeTraceID()
	spanID := makeSpanID()

	lr := &logspb.LogRecord{
		TimeUnixNano:         1000000000000,
		ObservedTimeUnixNano: 1000000100000,
		SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		SeverityText:         "ERROR",
		Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "something went wrong"}},
		Attributes: []*commonpb.KeyValue{
			{Key: "log.source", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "app"}}},
		},
		TraceId:                traceID,
		SpanId:                 spanID,
		Flags:                  1,
		DroppedAttributesCount: 2,
	}

	result := convertLogRecord(lr)

	assert.Equal(t, uint64(1000000000000), result["time_unix_nano"])
	assert.Equal(t, uint64(1000000100000), result["observed_time_unix_nano"])
	assert.Equal(t, int32(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR), result["severity_number"])
	assert.Equal(t, "ERROR", result["severity_text"])
	assert.Equal(t, "something went wrong", result["body"])
	assert.Equal(t, hex.EncodeToString(traceID), result["trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), result["span_id"])
	assert.Equal(t, uint32(1), result["flags"])
	assert.Equal(t, uint32(2), result["dropped_attributes_count"])

	attrs := result["attributes"].(map[string]interface{})
	assert.Equal(t, "app", attrs["log.source"])
}

func TestConvertLogRecordMinimal(t *testing.T) {
	lr := &logspb.LogRecord{}
	result := convertLogRecord(lr)
	assert.Empty(t, result)
}

func TestConvertMetricGauge(t *testing.T) {
	metric := &metricspb.Metric{
		Name:        "cpu.usage",
		Description: "CPU usage percentage",
		Unit:        "%",
		Data: &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{
					{
						StartTimeUnixNano: 1000000000000,
						TimeUnixNano:      2000000000000,
						Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 75.5},
						Attributes: []*commonpb.KeyValue{
							{Key: "host", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "server1"}}},
						},
					},
					{
						StartTimeUnixNano: 1000000000000,
						TimeUnixNano:      2000000000000,
						Value:             &metricspb.NumberDataPoint_AsInt{AsInt: 80},
					},
				},
			},
		},
	}

	result := convertMetric(metric)

	assert.Equal(t, "cpu.usage", result["name"])
	assert.Equal(t, "CPU usage percentage", result["description"])
	assert.Equal(t, "%", result["unit"])
	assert.Equal(t, "gauge", result["type"])

	dps := result["data_points"].([]interface{})
	require.Len(t, dps, 2)

	dp0 := dps[0].(map[string]interface{})
	assert.Equal(t, 75.5, dp0["value"])
	assert.Equal(t, "server1", dp0["attributes"].(map[string]interface{})["host"])

	dp1 := dps[1].(map[string]interface{})
	assert.Equal(t, int64(80), dp1["value"])
}

func TestConvertMetricSum(t *testing.T) {
	metric := &metricspb.Metric{
		Name: "http.requests",
		Data: &metricspb.Metric_Sum{
			Sum: &metricspb.Sum{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				IsMonotonic:           true,
				DataPoints: []*metricspb.NumberDataPoint{
					{
						TimeUnixNano: 2000000000000,
						Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 100},
					},
				},
			},
		},
	}

	result := convertMetric(metric)
	assert.Equal(t, "sum", result["type"])
	assert.Equal(t, "AGGREGATION_TEMPORALITY_CUMULATIVE", result["aggregation_temporality"])
	assert.Equal(t, true, result["is_monotonic"])
}

func TestConvertMetricHistogram(t *testing.T) {
	sum := 123.0
	min := 1.0
	max := 100.0
	metric := &metricspb.Metric{
		Name: "http.request.duration",
		Data: &metricspb.Metric_Histogram{
			Histogram: &metricspb.Histogram{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints: []*metricspb.HistogramDataPoint{
					{
						StartTimeUnixNano: 1000000000000,
						TimeUnixNano:      2000000000000,
						Count:             10,
						Sum:               &sum,
						Min:               &min,
						Max:               &max,
						BucketCounts:      []uint64{2, 3, 5},
						ExplicitBounds:    []float64{10.0, 50.0},
						Exemplars: []*metricspb.Exemplar{
							{
								TimeUnixNano: 1500000000000,
								Value:        &metricspb.Exemplar_AsDouble{AsDouble: 42.0},
								TraceId:      makeTraceID(),
								SpanId:       makeSpanID(),
								FilteredAttributes: []*commonpb.KeyValue{
									{Key: "filtered", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "val"}}},
								},
							},
						},
					},
				},
			},
		},
	}

	result := convertMetric(metric)
	assert.Equal(t, "histogram", result["type"])
	assert.Equal(t, "AGGREGATION_TEMPORALITY_DELTA", result["aggregation_temporality"])

	dps := result["data_points"].([]interface{})
	require.Len(t, dps, 1)
	dp := dps[0].(map[string]interface{})
	assert.Equal(t, uint64(10), dp["count"])
	assert.Equal(t, 123.0, dp["sum"])
	assert.Equal(t, 1.0, dp["min"])
	assert.Equal(t, 100.0, dp["max"])
	assert.Equal(t, []uint64{2, 3, 5}, dp["bucket_counts"])
	assert.Equal(t, []float64{10.0, 50.0}, dp["explicit_bounds"])

	exemplars := dp["exemplars"].([]interface{})
	require.Len(t, exemplars, 1)
	ex := exemplars[0].(map[string]interface{})
	assert.Equal(t, 42.0, ex["value"])
	assert.Equal(t, hex.EncodeToString(makeTraceID()), ex["trace_id"])
	assert.Equal(t, hex.EncodeToString(makeSpanID()), ex["span_id"])
	assert.Equal(t, "val", ex["filtered_attributes"].(map[string]interface{})["filtered"])
}

func TestConvertMetricExponentialHistogram(t *testing.T) {
	sum := 500.0
	metric := &metricspb.Metric{
		Name: "exp_hist",
		Data: &metricspb.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricspb.ExponentialHistogram{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				DataPoints: []*metricspb.ExponentialHistogramDataPoint{
					{
						TimeUnixNano: 2000000000000,
						Count:        100,
						Sum:          &sum,
						Scale:        5,
						ZeroCount:    3,
						Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
							Offset:       1,
							BucketCounts: []uint64{10, 20, 30},
						},
						Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{
							Offset:       -2,
							BucketCounts: []uint64{5, 15},
						},
					},
				},
			},
		},
	}

	result := convertMetric(metric)
	assert.Equal(t, "exponential_histogram", result["type"])

	dps := result["data_points"].([]interface{})
	require.Len(t, dps, 1)
	dp := dps[0].(map[string]interface{})
	assert.Equal(t, uint64(100), dp["count"])
	assert.Equal(t, 500.0, dp["sum"])
	assert.Equal(t, int32(5), dp["scale"])
	assert.Equal(t, uint64(3), dp["zero_count"])

	pos := dp["positive"].(map[string]interface{})
	assert.Equal(t, int32(1), pos["offset"])
	assert.Equal(t, []uint64{10, 20, 30}, pos["bucket_counts"])

	neg := dp["negative"].(map[string]interface{})
	assert.Equal(t, int32(-2), neg["offset"])
	assert.Equal(t, []uint64{5, 15}, neg["bucket_counts"])
}

func TestConvertMetricSummary(t *testing.T) {
	metric := &metricspb.Metric{
		Name: "request.duration.summary",
		Data: &metricspb.Metric_Summary{
			Summary: &metricspb.Summary{
				DataPoints: []*metricspb.SummaryDataPoint{
					{
						TimeUnixNano: 2000000000000,
						Count:        50,
						Sum:          1234.5,
						QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{
							{Quantile: 0.5, Value: 20.0},
							{Quantile: 0.99, Value: 99.0},
						},
					},
				},
			},
		},
	}

	result := convertMetric(metric)
	assert.Equal(t, "summary", result["type"])

	dps := result["data_points"].([]interface{})
	require.Len(t, dps, 1)
	dp := dps[0].(map[string]interface{})
	assert.Equal(t, uint64(50), dp["count"])
	assert.Equal(t, 1234.5, dp["sum"])

	qvs := dp["quantile_values"].([]interface{})
	require.Len(t, qvs, 2)
	assert.Equal(t, 0.5, qvs[0].(map[string]interface{})["quantile"])
	assert.Equal(t, 20.0, qvs[0].(map[string]interface{})["value"])
	assert.Equal(t, 0.99, qvs[1].(map[string]interface{})["quantile"])
	assert.Equal(t, 99.0, qvs[1].(map[string]interface{})["value"])
}

func TestNanoToMs(t *testing.T) {
	assert.Equal(t, uint64(0), nanoToMs(0))
	assert.Equal(t, uint64(1000), nanoToMs(1000000000))
	assert.Equal(t, uint64(1), nanoToMs(1000000))
	assert.Equal(t, uint64(1500), nanoToMs(1500000000))
}

// --- Validation tests ---

func validClientOptions() uspclient.ClientOptions {
	return uspclient.ClientOptions{
		TestSinkMode: true,
		Identity: uspclient.Identity{
			Oid:             "test-oid",
			InstallationKey: "test-key",
		},
		Platform: "json",
	}
}

func TestOTelConfigValidate(t *testing.T) {
	t.Run("valid with grpc", func(t *testing.T) {
		conf := OTelConfig{
			ClientOptions: validClientOptions(),
			GRPCPort:      4317,
		}
		assert.NoError(t, conf.Validate())
	})

	t.Run("valid with http", func(t *testing.T) {
		conf := OTelConfig{
			ClientOptions: validClientOptions(),
			HTTPPort:      4318,
		}
		assert.NoError(t, conf.Validate())
	})

	t.Run("valid with both", func(t *testing.T) {
		conf := OTelConfig{
			ClientOptions: validClientOptions(),
			GRPCPort:      4317,
			HTTPPort:      4318,
		}
		assert.NoError(t, conf.Validate())
	})

	t.Run("invalid no ports", func(t *testing.T) {
		conf := OTelConfig{
			ClientOptions: validClientOptions(),
		}
		err := conf.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "grpc_port or http_port")
	})

	t.Run("invalid client options", func(t *testing.T) {
		conf := OTelConfig{
			ClientOptions: uspclient.ClientOptions{},
			GRPCPort:      4317,
		}
		err := conf.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client_options")
	})
}

// --- Integration tests: gRPC ---

func TestGRPCTraceExport(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      0, // Will use a random port
	}

	// Use a random available port
	conf.GRPCPort = getFreePort(t)

	ctx := context.Background()
	adapter, chStopped, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Connect gRPC client
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", conf.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := coltracepb.NewTraceServiceClient(conn)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-svc"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: &commonpb.InstrumentationScope{Name: "test-lib", Version: "1.0"},
						Spans: []*tracepb.Span{
							{
								TraceId:           makeTraceID(),
								SpanId:            makeSpanID(),
								Name:              "test-span",
								Kind:              tracepb.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: 1000000000000,
								EndTimeUnixNano:   2000000000000,
							},
						},
					},
				},
			},
		},
	}

	resp, err := client.Export(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Verify adapter is still running
	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}
}

func TestGRPCMetricsExport(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", conf.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-svc"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "test-lib"},
						Metrics: []*metricspb.Metric{
							{
								Name:        "test.counter",
								Description: "A test counter",
								Unit:        "1",
								Data: &metricspb.Metric_Sum{
									Sum: &metricspb.Sum{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										IsMonotonic:           true,
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: 2000000000000,
												Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 42},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp, err := client.Export(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestGRPCLogsExport(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", conf.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-svc"}}},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{Name: "test-lib"},
						LogRecords: []*logspb.LogRecord{
							{
								TimeUnixNano:   1000000000000,
								SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
								SeverityText:   "INFO",
								Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log message"}},
							},
						},
					},
				},
			},
		},
	}

	resp, err := client.Export(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// --- Integration tests: HTTP ---

func TestHTTPTraceExportProtobuf(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "http-test"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId:           makeTraceID(),
								SpanId:            makeSpanID(),
								Name:              "http-span",
								Kind:              tracepb.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: 1000000000000,
								EndTimeUnixNano:   2000000000000,
							},
						},
					},
				},
			},
		},
	}

	body, err := proto.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/traces", conf.HTTPPort),
		"application/x-protobuf",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	respMsg := &coltracepb.ExportTraceServiceResponse{}
	require.NoError(t, proto.Unmarshal(respBody, respMsg))
}

func TestHTTPTraceExportJSON(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId:           makeTraceID(),
								SpanId:            makeSpanID(),
								Name:              "json-span",
								StartTimeUnixNano: 1000000000000,
								EndTimeUnixNano:   2000000000000,
							},
						},
					},
				},
			},
		},
	}

	body, err := protojson.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/traces", conf.HTTPPort),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

func TestHTTPMetricsExportProtobuf(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: "test.gauge",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: 2000000000000, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	body, err := proto.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/metrics", conf.HTTPPort),
		"application/x-protobuf",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHTTPLogsExportProtobuf(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								TimeUnixNano:   1000000000000,
								SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
								SeverityText:   "WARN",
								Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "disk space low"}},
							},
						},
					},
				},
			},
		},
	}

	body, err := proto.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/logs", conf.HTTPPort),
		"application/x-protobuf",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHTTPMethodNotAllowed(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	endpoints := []string{"/v1/traces", "/v1/metrics", "/v1/logs"}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%d%s", conf.HTTPPort, ep))
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
		})
	}
}

func TestHTTPBadBody(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/traces", conf.HTTPPort),
		"application/x-protobuf",
		bytes.NewReader([]byte("not valid protobuf")),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHTTPBadJSON(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/traces", conf.HTTPPort),
		"application/json",
		bytes.NewReader([]byte("{invalid json")),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- Adapter lifecycle tests ---

func TestAdapterGRPCOnly(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, chStopped, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)

	// Verify it's running
	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	require.NoError(t, adapter.Close())

	// Verify it stopped
	select {
	case <-chStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("adapter didn't stop after Close()")
	}
}

func TestAdapterHTTPOnly(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, chStopped, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	require.NoError(t, adapter.Close())

	select {
	case <-chStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("adapter didn't stop after Close()")
	}
}

func TestAdapterBothPorts(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      getFreePort(t),
		HTTPPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, chStopped, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Send via gRPC
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", conf.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	traceClient := coltracepb.NewTraceServiceClient(conn)
	_, err = traceClient.Export(ctx, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{ScopeSpans: []*tracepb.ScopeSpans{
				{Spans: []*tracepb.Span{{TraceId: makeTraceID(), SpanId: makeSpanID(), Name: "grpc-span", StartTimeUnixNano: 1000000000000, EndTimeUnixNano: 2000000000000}}},
			}},
		},
	})
	require.NoError(t, err)

	// Send via HTTP
	httpReq := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{ScopeSpans: []*tracepb.ScopeSpans{
				{Spans: []*tracepb.Span{{TraceId: makeTraceID(), SpanId: makeSpanID(), Name: "http-span", StartTimeUnixNano: 1000000000000, EndTimeUnixNano: 2000000000000}}},
			}},
		},
	}
	body, err := proto.Marshal(httpReq)
	require.NoError(t, err)
	resp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/v1/traces", conf.HTTPPort),
		"application/x-protobuf",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Graceful shutdown
	require.NoError(t, adapter.Close())
	select {
	case <-chStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("adapter didn't stop after Close()")
	}
}

func TestGRPCMultipleResourceSpans(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", conf.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := coltracepb.NewTraceServiceClient(conn)

	// Send request with multiple resources, scopes, and spans
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc-a"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: &commonpb.InstrumentationScope{Name: "lib-1"},
						Spans: []*tracepb.Span{
							{TraceId: makeTraceID(), SpanId: makeSpanID(), Name: "span-1", StartTimeUnixNano: 1000000000000, EndTimeUnixNano: 2000000000000},
							{TraceId: makeTraceID(), SpanId: makeSpanID(), Name: "span-2", StartTimeUnixNano: 1000000000000, EndTimeUnixNano: 2000000000000},
						},
					},
					{
						Scope: &commonpb.InstrumentationScope{Name: "lib-2"},
						Spans: []*tracepb.Span{
							{TraceId: makeTraceID(), SpanId: makeSpanID(), Name: "span-3", StartTimeUnixNano: 1000000000000, EndTimeUnixNano: 2000000000000},
						},
					},
				},
			},
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc-b"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{TraceId: makeTraceID(), SpanId: makeSpanID(), Name: "span-4", StartTimeUnixNano: 1000000000000, EndTimeUnixNano: 2000000000000},
						},
					},
				},
			},
		},
	}

	resp, err := client.Export(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestGRPCEmptyRequest(t *testing.T) {
	conf := OTelConfig{
		ClientOptions: testClientOptions(t),
		GRPCPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, _, err := NewOTelAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", conf.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	// Empty requests should succeed without errors
	traceClient := coltracepb.NewTraceServiceClient(conn)
	_, err = traceClient.Export(ctx, &coltracepb.ExportTraceServiceRequest{})
	assert.NoError(t, err)

	metricsClient := colmetricspb.NewMetricsServiceClient(conn)
	_, err = metricsClient.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{})
	assert.NoError(t, err)

	logsClient := collogspb.NewLogsServiceClient(conn)
	_, err = logsClient.Export(ctx, &collogspb.ExportLogsServiceRequest{})
	assert.NoError(t, err)
}

// --- Helpers ---

func getFreePort(t *testing.T) uint16 {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return uint16(port)
}
