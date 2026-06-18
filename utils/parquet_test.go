//go:build !aix

package utils

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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

// TestGunzipBomb verifies that gunzip refuses to decompress past the
// caller-provided limit. Without this cap, a small malicious .gz of
// repeated zeros (every adapter consuming user-controlled gz files is
// exposed) would OOM the host before parquet decoding even started.
func TestGunzipBomb(t *testing.T) {
	// 10 MB of zeros gzips down to ~10 KB but expands back to 10 MB.
	plain := make([]byte, 10*1024*1024)
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(plain); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	// Limit is way below the decompressed size — must fail.
	if _, err := gunzip(gzBuf.Bytes(), 1024); err == nil {
		t.Fatal("expected limit-exceeded error from gunzip, got nil")
	}

	// Above the decompressed size — must succeed.
	out, err := gunzip(gzBuf.Bytes(), int64(len(plain)+1))
	if err != nil {
		t.Fatalf("gunzip within limit: %v", err)
	}
	if len(out) != len(plain) {
		t.Fatalf("gunzip output size = %d, want %d", len(out), len(plain))
	}
}

// TestParquetToJSONLines_NullValues verifies that null cells survive
// the parquet→JSON round-trip and surface as JSON null. Without explicit
// handling, Apache Arrow's GetOneForMarshal can return Go nil which
// json.Marshal renders correctly — this test pins that contract.
func TestParquetToJSONLines_NullValues(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	// Two rows: one fully populated, one fully null.
	rb.Field(0).(*array.StringBuilder).Append("hello")
	rb.Field(1).(*array.Int64Builder).Append(42)
	rb.Field(0).(*array.StringBuilder).AppendNull()
	rb.Field(1).(*array.Int64Builder).AppendNull()

	rec := rb.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(tbl, &buf, 2, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("pqarrow.WriteTable: %v", err)
	}

	out, err := ParquetToJSONLines(buf.Bytes())
	if err != nil {
		t.Fatalf("ParquetToJSONLines: %v", err)
	}
	rows := decodeJSONLines(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["a"] != "hello" {
		t.Fatalf("row 0 a=%v, want hello", rows[0]["a"])
	}
	if got := rows[0]["b"]; got != float64(42) {
		t.Fatalf("row 0 b=%v (%T), want 42", got, got)
	}
	if rows[1]["a"] != nil {
		t.Fatalf("row 1 a=%v, want JSON null", rows[1]["a"])
	}
	if rows[1]["b"] != nil {
		t.Fatalf("row 1 b=%v, want JSON null", rows[1]["b"])
	}
}

// TestParquetToJSONLines_ZeroRows verifies that a valid parquet file
// with a schema but no row groups produces no output without erroring.
// Arrow's RecordReader returns false from Next() immediately; we want
// this surfaced as an empty (rather than nil) byte slice.
func TestParquetToJSONLines_ZeroRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.BinaryTypes.String},
	}, nil)
	mem := memory.DefaultAllocator
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	rec := rb.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(tbl, &buf, 1, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("pqarrow.WriteTable: %v", err)
	}

	out, err := ParquetToJSONLines(buf.Bytes())
	if err != nil {
		t.Fatalf("ParquetToJSONLines: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("zero-row parquet should produce empty output, got %d bytes: %q", len(out), out)
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
