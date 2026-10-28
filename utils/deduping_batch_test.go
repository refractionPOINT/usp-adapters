package utils

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBatchDeduper(t *testing.T, window, ttl time.Duration) BatchDeduper {
	t.Helper()
	d, err := NewLocalDeduper(window, ttl)
	require.NoError(t, err)
	bd, ok := d.(BatchDeduper)
	require.True(t, ok, "the local deduper must implement BatchDeduper")
	t.Cleanup(d.Close)
	return bd
}

func keys(names ...string) []DedupeKey {
	out := make([]DedupeKey, len(names))
	for i, n := range names {
		out[i] = DedupeKey{Key: n}
	}
	return out
}

func TestLocalDeduperCheckManyAddMany(t *testing.T) {
	d := newTestBatchDeduper(t, time.Hour, 24*time.Hour)

	assert.Equal(t, []bool{false, false, false}, d.CheckMany(keys("a", "b", "c")))

	d.AddMany(keys("a", "c"))
	assert.Equal(t, []bool{true, false, true, false}, d.CheckMany(keys("a", "b", "c", "d")),
		"results follow the order of the keys")

	// CheckMany never records.
	assert.False(t, d.Check("b"))
	assert.False(t, d.Check("d"))

	// Adding again is harmless.
	d.AddMany(keys("a", "b"))
	assert.Equal(t, []bool{true, true}, d.CheckMany(keys("a", "b")))
}

func TestLocalDeduperBatchAgreesWithSingleKeyCalls(t *testing.T) {
	d := newTestBatchDeduper(t, time.Hour, 24*time.Hour)

	assert.False(t, d.CheckAndAdd("single"), "recorded by the single-key call")
	d.AddMany(keys("batched"))

	assert.Equal(t, []bool{true, true, false}, d.CheckMany(keys("single", "batched", "other")))
	assert.True(t, d.Check("batched"))
	assert.True(t, d.CheckAndAdd("batched"))
}

func TestLocalDeduperBatchEmpty(t *testing.T) {
	d := newTestBatchDeduper(t, time.Hour, 24*time.Hour)

	assert.Empty(t, d.CheckMany(nil))
	assert.Empty(t, d.CheckMany([]DedupeKey{}))
	d.AddMany(nil)
	d.AddMany([]DedupeKey{})
}

func TestLocalDeduperBatchIgnoresTime(t *testing.T) {
	d := newTestBatchDeduper(t, time.Hour, 24*time.Hour)

	d.AddMany([]DedupeKey{{Key: "a", Time: time.Now()}, {Key: "b"}})
	got := d.CheckMany([]DedupeKey{
		{Key: "a"}, // another time than it was recorded with
		{Key: "b", Time: time.Now().Add(-48 * time.Hour)},
		{Key: "c", Time: time.Now()},
	})
	assert.Equal(t, []bool{true, true, false}, got)
}

func TestLocalDeduperBatchExpires(t *testing.T) {
	window := 50 * time.Millisecond
	ttl := 150 * time.Millisecond
	d := newTestBatchDeduper(t, window, ttl)

	d.AddMany(keys("a", "b"))
	require.Equal(t, []bool{true, true}, d.CheckMany(keys("a", "b")))

	require.Eventually(t, func() bool {
		got := d.CheckMany(keys("a", "b"))
		return !got[0] && !got[1]
	}, 5*time.Second, 20*time.Millisecond, "keys recorded with AddMany must expire like any other")
}

func TestLocalDeduperBatchConcurrent(t *testing.T) {
	d := newTestBatchDeduper(t, time.Hour, 24*time.Hour)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := keys(fmt.Sprintf("g%d-%d", g, i), fmt.Sprintf("shared-%d", i))
				d.CheckMany(k)
				d.AddMany(k)
				d.CheckAndAdd(fmt.Sprintf("single-%d", i))
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < 8; g++ {
		assert.Equal(t, []bool{true}, d.CheckMany(keys(fmt.Sprintf("g%d-199", g))))
	}
	assert.Equal(t, []bool{true}, d.CheckMany(keys("shared-0")))
}
