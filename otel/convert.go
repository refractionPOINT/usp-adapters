package usp_otel

import (
	"encoding/base64"
	"encoding/hex"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func nanoToMs(nanos uint64) uint64 {
	if nanos == 0 {
		return 0
	}
	return nanos / 1_000_000
}

func convertAttributes(attrs []*commonpb.KeyValue) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	result := make(map[string]interface{}, len(attrs))
	for _, kv := range attrs {
		result[kv.Key] = convertAnyValue(kv.Value)
	}
	return result
}

func convertAnyValue(v *commonpb.AnyValue) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_ArrayValue:
		if val.ArrayValue == nil {
			return nil
		}
		arr := make([]interface{}, len(val.ArrayValue.Values))
		for i, v := range val.ArrayValue.Values {
			arr[i] = convertAnyValue(v)
		}
		return arr
	case *commonpb.AnyValue_KvlistValue:
		if val.KvlistValue == nil {
			return nil
		}
		return convertAttributes(val.KvlistValue.Values)
	case *commonpb.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(val.BytesValue)
	}
	return nil
}

func convertResource(r *resourcepb.Resource) map[string]interface{} {
	if r == nil {
		return nil
	}
	return convertAttributes(r.Attributes)
}

func convertScope(scope *commonpb.InstrumentationScope) map[string]interface{} {
	if scope == nil {
		return nil
	}
	result := map[string]interface{}{}
	if scope.Name != "" {
		result["name"] = scope.Name
	}
	if scope.Version != "" {
		result["version"] = scope.Version
	}
	if attrs := convertAttributes(scope.Attributes); attrs != nil {
		result["attributes"] = attrs
	}
	return result
}

// Traces

func convertSpan(span *tracepb.Span) map[string]interface{} {
	record := map[string]interface{}{
		"trace_id":             hex.EncodeToString(span.TraceId),
		"span_id":              hex.EncodeToString(span.SpanId),
		"name":                 span.Name,
		"kind":                 span.Kind.String(),
		"start_time_unix_nano": span.StartTimeUnixNano,
		"end_time_unix_nano":   span.EndTimeUnixNano,
	}
	if len(span.ParentSpanId) > 0 {
		record["parent_span_id"] = hex.EncodeToString(span.ParentSpanId)
	}
	if span.TraceState != "" {
		record["trace_state"] = span.TraceState
	}
	if attrs := convertAttributes(span.Attributes); attrs != nil {
		record["attributes"] = attrs
	}
	if span.Status != nil {
		status := map[string]interface{}{
			"code": span.Status.Code.String(),
		}
		if span.Status.Message != "" {
			status["message"] = span.Status.Message
		}
		record["status"] = status
	}
	if len(span.Events) > 0 {
		events := make([]interface{}, len(span.Events))
		for i, e := range span.Events {
			event := map[string]interface{}{
				"name":           e.Name,
				"time_unix_nano": e.TimeUnixNano,
			}
			if attrs := convertAttributes(e.Attributes); attrs != nil {
				event["attributes"] = attrs
			}
			if e.DroppedAttributesCount > 0 {
				event["dropped_attributes_count"] = e.DroppedAttributesCount
			}
			events[i] = event
		}
		record["events"] = events
	}
	if len(span.Links) > 0 {
		links := make([]interface{}, len(span.Links))
		for i, l := range span.Links {
			link := map[string]interface{}{
				"trace_id": hex.EncodeToString(l.TraceId),
				"span_id":  hex.EncodeToString(l.SpanId),
			}
			if l.TraceState != "" {
				link["trace_state"] = l.TraceState
			}
			if attrs := convertAttributes(l.Attributes); attrs != nil {
				link["attributes"] = attrs
			}
			if l.DroppedAttributesCount > 0 {
				link["dropped_attributes_count"] = l.DroppedAttributesCount
			}
			links[i] = link
		}
		record["links"] = links
	}
	if span.DroppedAttributesCount > 0 {
		record["dropped_attributes_count"] = span.DroppedAttributesCount
	}
	if span.DroppedEventsCount > 0 {
		record["dropped_events_count"] = span.DroppedEventsCount
	}
	if span.DroppedLinksCount > 0 {
		record["dropped_links_count"] = span.DroppedLinksCount
	}
	return record
}

// Metrics

func convertMetric(metric *metricspb.Metric) map[string]interface{} {
	record := map[string]interface{}{
		"name": metric.Name,
	}
	if metric.Description != "" {
		record["description"] = metric.Description
	}
	if metric.Unit != "" {
		record["unit"] = metric.Unit
	}

	switch data := metric.Data.(type) {
	case *metricspb.Metric_Gauge:
		record["type"] = "gauge"
		record["data_points"] = convertNumberDataPoints(data.Gauge.DataPoints)
	case *metricspb.Metric_Sum:
		record["type"] = "sum"
		record["data_points"] = convertNumberDataPoints(data.Sum.DataPoints)
		record["aggregation_temporality"] = data.Sum.AggregationTemporality.String()
		record["is_monotonic"] = data.Sum.IsMonotonic
	case *metricspb.Metric_Histogram:
		record["type"] = "histogram"
		record["data_points"] = convertHistogramDataPoints(data.Histogram.DataPoints)
		record["aggregation_temporality"] = data.Histogram.AggregationTemporality.String()
	case *metricspb.Metric_ExponentialHistogram:
		record["type"] = "exponential_histogram"
		record["data_points"] = convertExponentialHistogramDataPoints(data.ExponentialHistogram.DataPoints)
		record["aggregation_temporality"] = data.ExponentialHistogram.AggregationTemporality.String()
	case *metricspb.Metric_Summary:
		record["type"] = "summary"
		record["data_points"] = convertSummaryDataPoints(data.Summary.DataPoints)
	}

	return record
}

func convertNumberDataPoints(dps []*metricspb.NumberDataPoint) []interface{} {
	result := make([]interface{}, len(dps))
	for i, dp := range dps {
		point := map[string]interface{}{
			"start_time_unix_nano": dp.StartTimeUnixNano,
			"time_unix_nano":       dp.TimeUnixNano,
		}
		switch v := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsDouble:
			point["value"] = v.AsDouble
		case *metricspb.NumberDataPoint_AsInt:
			point["value"] = v.AsInt
		}
		if attrs := convertAttributes(dp.Attributes); attrs != nil {
			point["attributes"] = attrs
		}
		if len(dp.Exemplars) > 0 {
			point["exemplars"] = convertExemplars(dp.Exemplars)
		}
		result[i] = point
	}
	return result
}

func convertHistogramDataPoints(dps []*metricspb.HistogramDataPoint) []interface{} {
	result := make([]interface{}, len(dps))
	for i, dp := range dps {
		point := map[string]interface{}{
			"start_time_unix_nano": dp.StartTimeUnixNano,
			"time_unix_nano":       dp.TimeUnixNano,
			"count":                dp.Count,
		}
		if len(dp.BucketCounts) > 0 {
			point["bucket_counts"] = dp.BucketCounts
		}
		if len(dp.ExplicitBounds) > 0 {
			point["explicit_bounds"] = dp.ExplicitBounds
		}
		if dp.Sum != nil {
			point["sum"] = *dp.Sum
		}
		if dp.Min != nil {
			point["min"] = *dp.Min
		}
		if dp.Max != nil {
			point["max"] = *dp.Max
		}
		if attrs := convertAttributes(dp.Attributes); attrs != nil {
			point["attributes"] = attrs
		}
		if len(dp.Exemplars) > 0 {
			point["exemplars"] = convertExemplars(dp.Exemplars)
		}
		result[i] = point
	}
	return result
}

func convertExponentialHistogramDataPoints(dps []*metricspb.ExponentialHistogramDataPoint) []interface{} {
	result := make([]interface{}, len(dps))
	for i, dp := range dps {
		point := map[string]interface{}{
			"start_time_unix_nano": dp.StartTimeUnixNano,
			"time_unix_nano":       dp.TimeUnixNano,
			"count":                dp.Count,
			"scale":                dp.Scale,
			"zero_count":           dp.ZeroCount,
		}
		if dp.Sum != nil {
			point["sum"] = *dp.Sum
		}
		if dp.Min != nil {
			point["min"] = *dp.Min
		}
		if dp.Max != nil {
			point["max"] = *dp.Max
		}
		if dp.Positive != nil {
			point["positive"] = map[string]interface{}{
				"offset":        dp.Positive.Offset,
				"bucket_counts": dp.Positive.BucketCounts,
			}
		}
		if dp.Negative != nil {
			point["negative"] = map[string]interface{}{
				"offset":        dp.Negative.Offset,
				"bucket_counts": dp.Negative.BucketCounts,
			}
		}
		if attrs := convertAttributes(dp.Attributes); attrs != nil {
			point["attributes"] = attrs
		}
		if len(dp.Exemplars) > 0 {
			point["exemplars"] = convertExemplars(dp.Exemplars)
		}
		result[i] = point
	}
	return result
}

func convertSummaryDataPoints(dps []*metricspb.SummaryDataPoint) []interface{} {
	result := make([]interface{}, len(dps))
	for i, dp := range dps {
		point := map[string]interface{}{
			"start_time_unix_nano": dp.StartTimeUnixNano,
			"time_unix_nano":       dp.TimeUnixNano,
			"count":                dp.Count,
			"sum":                  dp.Sum,
		}
		if len(dp.QuantileValues) > 0 {
			qvs := make([]interface{}, len(dp.QuantileValues))
			for j, qv := range dp.QuantileValues {
				qvs[j] = map[string]interface{}{
					"quantile": qv.Quantile,
					"value":    qv.Value,
				}
			}
			point["quantile_values"] = qvs
		}
		if attrs := convertAttributes(dp.Attributes); attrs != nil {
			point["attributes"] = attrs
		}
		result[i] = point
	}
	return result
}

func convertExemplars(exemplars []*metricspb.Exemplar) []interface{} {
	result := make([]interface{}, len(exemplars))
	for i, e := range exemplars {
		ex := map[string]interface{}{
			"time_unix_nano": e.TimeUnixNano,
		}
		switch v := e.Value.(type) {
		case *metricspb.Exemplar_AsDouble:
			ex["value"] = v.AsDouble
		case *metricspb.Exemplar_AsInt:
			ex["value"] = v.AsInt
		}
		if len(e.TraceId) > 0 {
			ex["trace_id"] = hex.EncodeToString(e.TraceId)
		}
		if len(e.SpanId) > 0 {
			ex["span_id"] = hex.EncodeToString(e.SpanId)
		}
		if attrs := convertAttributes(e.FilteredAttributes); attrs != nil {
			ex["filtered_attributes"] = attrs
		}
		result[i] = ex
	}
	return result
}

// Logs

func convertLogRecord(lr *logspb.LogRecord) map[string]interface{} {
	record := map[string]interface{}{}
	if lr.TimeUnixNano != 0 {
		record["time_unix_nano"] = lr.TimeUnixNano
	}
	if lr.ObservedTimeUnixNano != 0 {
		record["observed_time_unix_nano"] = lr.ObservedTimeUnixNano
	}
	if lr.SeverityNumber != 0 {
		record["severity_number"] = int32(lr.SeverityNumber)
	}
	if lr.SeverityText != "" {
		record["severity_text"] = lr.SeverityText
	}
	if lr.Body != nil {
		record["body"] = convertAnyValue(lr.Body)
	}
	if attrs := convertAttributes(lr.Attributes); attrs != nil {
		record["attributes"] = attrs
	}
	if len(lr.TraceId) > 0 {
		record["trace_id"] = hex.EncodeToString(lr.TraceId)
	}
	if len(lr.SpanId) > 0 {
		record["span_id"] = hex.EncodeToString(lr.SpanId)
	}
	if lr.Flags > 0 {
		record["flags"] = lr.Flags
	}
	if lr.DroppedAttributesCount > 0 {
		record["dropped_attributes_count"] = lr.DroppedAttributesCount
	}
	return record
}
