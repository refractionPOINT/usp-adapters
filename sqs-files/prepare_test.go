//go:build !aix

package usp_sqs_files

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/refractionPOINT/usp-adapters/utils"
)

func testParquet(t *testing.T) []byte {
	schema := arrow.NewSchema([]arrow.Field{{Name: "event", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for range 100 {
		b.Field(0).(*array.StringBuilder).Append("test")
	}
	r := b.NewRecord()
	defer r.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{r})
	defer table.Release()
	var out bytes.Buffer
	if err := pqarrow.WriteTable(table, &out, 100, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func gzipTest(t *testing.T, data []byte) []byte {
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func TestLeaseAwareBundleFormatMatchesSharedDecoder(t *testing.T) {
	raw := testParquet(t)
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"test.txt", []byte("test")}, {"test.txt.gz", gzipTest(t, []byte("test"))},
		{"test.parquet", raw}, {"magic", raw}, {"test.parquet.gz", gzipTest(t, raw)},
		{"bad.parquet", []byte("bad")}, {"bad.parquet.gz", []byte("bad")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected, expectedCompressed, expectedErr := utils.PrepareBundleData(tc.name, tc.data)
			got, compressed, err := prepareBundleData(context.Background(), tc.name, tc.data)
			if (expectedErr == nil) != (err == nil) || compressed != expectedCompressed || !bytes.Equal(got, expected) {
				t.Fatalf("bundle format changed: compressed %v/%v errors %v/%v", compressed, expectedCompressed, err, expectedErr)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, _, err := prepareBundleData(ctx, tc.name, tc.data); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled codec: %v", err)
			}
		})
	}
}

// Cancel during cooperative reads/rows, without relying on timing or large data.
type cancelDuringDecode struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int32
}

func (c *cancelDuringDecode) Err() error {
	if c.checks.Add(1) >= 25 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestLeaseCancellationInterruptsParquetConversion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &cancelDuringDecode{Context: ctx, cancel: cancel}
	_, _, err := prepareBundleData(c, "test.parquet", testParquet(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("conversion ignored lost lease: %v", err)
	}
}
