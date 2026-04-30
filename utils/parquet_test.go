//go:build !aix

package utils

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
)

func TestIsParquetFile(t *testing.T) {
	// A two-row parquet file is enough to exercise the magic-bytes
	// detection path; the header and trailer both need to be "PAR1".
	data := buildTestParquet(t, 2)

	cases := []struct {
		name string
		key  string
		data []byte
		want bool
	}{
		{"extension-parquet", "logs/events.parquet", nil, true},
		{"extension-mixed-case", "Logs/EVENTS.Parquet", nil, true},
		{"magic-bytes-only", "logs/events", data, true},
		{"not-parquet-text", "logs/events.json", []byte("{\"a\":1}"), false},
		{"not-parquet-short", "logs/tiny", []byte("PAR1"), false},
		{"empty", "logs/empty", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsParquetFile(c.key, c.data); got != c.want {
				t.Fatalf("IsParquetFile(%q)=%v, want %v", c.key, got, c.want)
			}
		})
	}
}

func TestParquetToJSONLines_MultiRow(t *testing.T) {
	const rows = 20
	out, err := ParquetToJSONLines(buildTestParquet(t, rows))
	if err != nil {
		t.Fatalf("ParquetToJSONLines: %v", err)
	}

	decoded := decodeJSONLines(t, out)
	if len(decoded) != rows {
		t.Fatalf("expected %d rows, got %d", rows, len(decoded))
	}

	// Arrow lowercases Parquet field names when it builds its schema.
	expected := []string{"event_type", "event_time", "uid", "session_id", "user", "event_data"}
	for i, row := range decoded {
		for _, key := range expected {
			if _, ok := row[key]; !ok {
				t.Fatalf("row %d missing key %q: %+v", i, key, row)
			}
		}
	}

	first := decoded[0]
	if first["event_type"] != "login.success" {
		t.Fatalf("row 0 event_type=%v, want login.success", first["event_type"])
	}
	if _, ok := first["event_time"].(string); !ok {
		t.Fatalf("row 0 event_time not a string: %T", first["event_time"])
	}

	// The event_data column holds a JSON string; confirm it parses and
	// its inner event matches the top-level event_type column.
	rawEventData, ok := first["event_data"].(string)
	if !ok {
		t.Fatalf("row 0 event_data not string: %T", first["event_data"])
	}
	inner := map[string]any{}
	if err := json.Unmarshal([]byte(rawEventData), &inner); err != nil {
		t.Fatalf("row 0 event_data not valid JSON: %v", err)
	}
	if inner["event"] != "login.success" {
		t.Fatalf("row 0 inner event=%v, want login.success", inner["event"])
	}
}

func TestParquetToJSONLines_SingleRow(t *testing.T) {
	out, err := ParquetToJSONLines(buildTestParquet(t, 1))
	if err != nil {
		t.Fatalf("ParquetToJSONLines: %v", err)
	}

	decoded := decodeJSONLines(t, out)
	if len(decoded) != 1 {
		t.Fatalf("expected 1 row, got %d", len(decoded))
	}
	if decoded[0]["event_type"] != "login.success" {
		t.Fatalf("event_type=%v, want login.success", decoded[0]["event_type"])
	}
}

func TestParquetToJSONLines_NotParquet(t *testing.T) {
	if _, err := ParquetToJSONLines([]byte("this is not parquet at all")); err == nil {
		t.Fatal("expected error for non-parquet input, got nil")
	}
}

func TestParquetToJSONLines_Empty(t *testing.T) {
	if _, err := ParquetToJSONLines(nil); err == nil {
		t.Fatal("expected error for nil input, got nil")
	}
	if _, err := ParquetToJSONLines([]byte{}); err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

func TestPrepareBundleData_PlainParquet(t *testing.T) {
	data, isCompressed, err := PrepareBundleData("events.parquet", buildTestParquet(t, 2))
	if err != nil {
		t.Fatalf("PrepareBundleData: %v", err)
	}
	if isCompressed {
		t.Fatal("plain parquet should not flag isCompressed")
	}
	rows := decodeJSONLines(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestPrepareBundleData_GzippedParquet(t *testing.T) {
	pq := buildTestParquet(t, 3)
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(pq); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	data, isCompressed, err := PrepareBundleData("events.parquet.gz", gzBuf.Bytes())
	if err != nil {
		t.Fatalf("PrepareBundleData: %v", err)
	}
	if isCompressed {
		t.Fatal("gzipped parquet must be returned uncompressed: adapter peels both layers")
	}
	rows := decodeJSONLines(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestPrepareBundleData_PlainGzipPassthrough(t *testing.T) {
	// Non-parquet *.gz must keep the existing behaviour: pass through
	// untouched with isCompressed=true so the proxy gunzips.
	payload := []byte("just a gzipped log line\n")
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write(payload)
	gw.Close()
	gzBytes := gzBuf.Bytes()

	data, isCompressed, err := PrepareBundleData("events.log.gz", gzBytes)
	if err != nil {
		t.Fatalf("PrepareBundleData: %v", err)
	}
	if !isCompressed {
		t.Fatal("plain *.gz should flag isCompressed=true for proxy-side decompression")
	}
	if !bytes.Equal(data, gzBytes) {
		t.Fatal("plain *.gz must be returned untouched")
	}
}

func TestPrepareBundleData_PlainText(t *testing.T) {
	in := []byte("plain log content\n")
	data, isCompressed, err := PrepareBundleData("events.log", in)
	if err != nil {
		t.Fatalf("PrepareBundleData: %v", err)
	}
	if isCompressed {
		t.Fatal("plain text should not flag isCompressed")
	}
	if !bytes.Equal(data, in) {
		t.Fatal("plain text must be returned untouched")
	}
}

func TestPrepareBundleData_GzippedParquet_BadGzip(t *testing.T) {
	// Name says .parquet.gz but the bytes are not valid gzip — must
	// surface as an error so the adapter skips the object instead of
	// shipping garbage.
	_, _, err := PrepareBundleData("events.parquet.gz", []byte("not gzip"))
	if err == nil {
		t.Fatal("expected gunzip error, got nil")
	}
}

// buildTestParquet synthesises a Parquet file that matches the schema
// shape we want to exercise: six REQUIRED columns including a
// timestamp-millis and a JSON-string payload column. Fully synthetic
// data — no customer content.
func buildTestParquet(t *testing.T, n int) []byte {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "event_type", Type: arrow.BinaryTypes.String},
		{Name: "event_time", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}},
		{Name: "uid", Type: arrow.BinaryTypes.String},
		{Name: "session_id", Type: arrow.BinaryTypes.String},
		{Name: "user", Type: arrow.BinaryTypes.String},
		{Name: "event_data", Type: arrow.BinaryTypes.String},
	}, nil)

	mem := memory.DefaultAllocator
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	for i := 0; i < n; i++ {
		rb.Field(0).(*array.StringBuilder).Append("login.success")
		rb.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000000 + int64(i)*1000))
		rb.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("uid-%04d", i))
		rb.Field(3).(*array.StringBuilder).Append("")
		rb.Field(4).(*array.StringBuilder).Append("alice")
		payload, _ := json.Marshal(map[string]any{
			"event": "login.success",
			"user":  "alice",
			"seq":   i,
		})
		rb.Field(5).(*array.StringBuilder).Append(string(payload))
	}

	rec := rb.NewRecord()
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(tbl, &buf, int64(n), parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("pqarrow.WriteTable: %v", err)
	}
	return buf.Bytes()
}

func decodeJSONLines(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("invalid JSON line %q: %v", string(line), err)
		}
		rows = append(rows, row)
	}
	return rows
}
