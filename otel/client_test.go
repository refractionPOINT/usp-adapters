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
		GRPCPort:      getFreePort(t),
	}

	ctx := context.Background()
	adapter, chStopped, err := NewOTelAdapter(ctx, conf)
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
										IsMonotonic:            true,
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
