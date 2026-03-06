package usp_otel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	defaultWriteTimeout  = 60 * 10
	maxGRPCRecvMsgSize   = 64 * 1024 * 1024 // 64MB, matching OTel Collector default
	maxHTTPRequestBody   = 64 * 1024 * 1024
	eventTypeOTelTrace   = "otel_trace"
	eventTypeOTelMetric  = "otel_metric"
	eventTypeOTelLog     = "otel_log"
)

type OTelConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	GRPCPort      uint16                  `json:"grpc_port,omitempty" yaml:"grpc_port,omitempty"`
	HTTPPort      uint16                  `json:"http_port,omitempty" yaml:"http_port,omitempty"`
	Interface     string                  `json:"iface,omitempty" yaml:"iface,omitempty"`
}

func (c *OTelConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.GRPCPort == 0 && c.HTTPPort == 0 {
		return errors.New("at least one of grpc_port or http_port must be specified")
	}
	return nil
}

type OTelAdapter struct {
	conf         OTelConfig
	uspClient    *uspclient.Client
	grpcServer   *grpc.Server
	httpServer   *http.Server
	grpcListener net.Listener
	httpListener net.Listener
	wg           sync.WaitGroup
	isRunning    uint32
	writeTimeout time.Duration
}

func NewOTelAdapter(ctx context.Context, conf OTelConfig) (*OTelAdapter, chan struct{}, error) {
	a := &OTelAdapter{
		conf:         conf,
		isRunning:    1,
		writeTimeout: time.Duration(defaultWriteTimeout) * time.Second,
	}

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	if conf.GRPCPort != 0 {
		addr := fmt.Sprintf("%s:%d", conf.Interface, conf.GRPCPort)
		a.grpcListener, err = net.Listen("tcp", addr)
		if err != nil {
			a.uspClient.Close()
			return nil, nil, fmt.Errorf("grpc listen on %s: %v", addr, err)
		}
		a.grpcServer = grpc.NewServer(
			grpc.MaxRecvMsgSize(maxGRPCRecvMsgSize),
		)
		coltracepb.RegisterTraceServiceServer(a.grpcServer, &traceServiceServer{adapter: a})
		colmetricspb.RegisterMetricsServiceServer(a.grpcServer, &metricsServiceServer{adapter: a})
		collogspb.RegisterLogsServiceServer(a.grpcServer, &logsServiceServer{adapter: a})
	}

	if conf.HTTPPort != 0 {
		addr := fmt.Sprintf("%s:%d", conf.Interface, conf.HTTPPort)
		a.httpListener, err = net.Listen("tcp", addr)
		if err != nil {
			if a.grpcListener != nil {
				a.grpcListener.Close()
			}
			a.uspClient.Close()
			return nil, nil, fmt.Errorf("http listen on %s: %v", addr, err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/traces", a.handleHTTPTraces)
		mux.HandleFunc("/v1/metrics", a.handleHTTPMetrics)
		mux.HandleFunc("/v1/logs", a.handleHTTPLogs)
		a.httpServer = &http.Server{Handler: mux}
	}

	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		var serverWg sync.WaitGroup

		if a.grpcServer != nil {
			serverWg.Add(1)
			go func() {
				defer serverWg.Done()
				conf.ClientOptions.DebugLog(fmt.Sprintf("gRPC server listening on %s:%d", conf.Interface, conf.GRPCPort))
				if err := a.grpcServer.Serve(a.grpcListener); err != nil {
					if atomic.LoadUint32(&a.isRunning) == 1 {
						conf.ClientOptions.OnError(fmt.Errorf("grpc serve: %v", err))
					}
				}
			}()
		}

		if a.httpServer != nil {
			serverWg.Add(1)
			go func() {
				defer serverWg.Done()
				conf.ClientOptions.DebugLog(fmt.Sprintf("HTTP server listening on %s:%d", conf.Interface, conf.HTTPPort))
				if err := a.httpServer.Serve(a.httpListener); err != nil && err != http.ErrServerClosed {
					if atomic.LoadUint32(&a.isRunning) == 1 {
						conf.ClientOptions.OnError(fmt.Errorf("http serve: %v", err))
					}
				}
			}()
		}

		serverWg.Wait()
		close(chStopped)
	}()

	return a, chStopped, nil
}

func (a *OTelAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isRunning, 0)

	if a.grpcServer != nil {
		a.grpcServer.GracefulStop()
	}
	if a.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a.httpServer.Shutdown(ctx)
	}

	a.wg.Wait()

	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (a *OTelAdapter) shipJSON(eventType string, payload map[string]interface{}, timestampMs uint64) {
	if timestampMs == 0 {
		timestampMs = uint64(time.Now().UnixMilli())
	}
	msg := &protocol.DataMessage{
		JsonPayload: payload,
		EventType:   eventType,
		TimestampMs: timestampMs,
	}
	err := a.uspClient.Ship(msg, a.writeTimeout)
	if err == uspclient.ErrorBufferFull {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		err = a.uspClient.Ship(msg, 1*time.Hour)
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
	}
}

// gRPC service implementations

type traceServiceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	adapter *OTelAdapter
}

func (s *traceServiceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.adapter.processTraces(req)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type metricsServiceServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	adapter *OTelAdapter
}

func (s *metricsServiceServer) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	s.adapter.processMetrics(req)
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type logsServiceServer struct {
	collogspb.UnimplementedLogsServiceServer
	adapter *OTelAdapter
}

func (s *logsServiceServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.adapter.processLogs(req)
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// HTTP handlers

func (a *OTelAdapter) handleHTTPTraces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPRequestBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := &coltracepb.ExportTraceServiceRequest{}
	if err := unmarshalOTLPRequest(r, body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.processTraces(req)
	marshalOTLPResponse(w, r, &coltracepb.ExportTraceServiceResponse{})
}

func (a *OTelAdapter) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPRequestBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := &colmetricspb.ExportMetricsServiceRequest{}
	if err := unmarshalOTLPRequest(r, body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.processMetrics(req)
	marshalOTLPResponse(w, r, &colmetricspb.ExportMetricsServiceResponse{})
}

func (a *OTelAdapter) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPRequestBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := &collogspb.ExportLogsServiceRequest{}
	if err := unmarshalOTLPRequest(r, body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.processLogs(req)
	marshalOTLPResponse(w, r, &collogspb.ExportLogsServiceResponse{})
}

func unmarshalOTLPRequest(r *http.Request, body []byte, msg proto.Message) error {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		return protojson.Unmarshal(body, msg)
	}
	return proto.Unmarshal(body, msg)
}

func marshalOTLPResponse(w http.ResponseWriter, r *http.Request, msg proto.Message) {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		respBytes, err := protojson.Marshal(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(respBytes)
		return
	}
	respBytes, err := proto.Marshal(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(respBytes)
}

// Processing functions

func (a *OTelAdapter) processTraces(req *coltracepb.ExportTraceServiceRequest) {
	for _, rs := range req.ResourceSpans {
		resource := convertResource(rs.Resource)
		for _, ss := range rs.ScopeSpans {
			scope := convertScope(ss.Scope)
			for _, span := range ss.Spans {
				record := convertSpan(span)
				record["resource"] = resource
				record["scope"] = scope
				ts := nanoToMs(span.StartTimeUnixNano)
				a.shipJSON(eventTypeOTelTrace, record, ts)
			}
		}
	}
}

func (a *OTelAdapter) processMetrics(req *colmetricspb.ExportMetricsServiceRequest) {
	for _, rm := range req.ResourceMetrics {
		resource := convertResource(rm.Resource)
		for _, sm := range rm.ScopeMetrics {
			scope := convertScope(sm.Scope)
			for _, metric := range sm.Metrics {
				record := convertMetric(metric)
				record["resource"] = resource
				record["scope"] = scope
				a.shipJSON(eventTypeOTelMetric, record, 0)
			}
		}
	}
}

func (a *OTelAdapter) processLogs(req *collogspb.ExportLogsServiceRequest) {
	for _, rl := range req.ResourceLogs {
		resource := convertResource(rl.Resource)
		for _, sl := range rl.ScopeLogs {
			scope := convertScope(sl.Scope)
			for _, logRecord := range sl.LogRecords {
				record := convertLogRecord(logRecord)
				record["resource"] = resource
				record["scope"] = scope
				ts := nanoToMs(logRecord.TimeUnixNano)
				a.shipJSON(eventTypeOTelLog, record, ts)
			}
		}
	}
}
