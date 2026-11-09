package utils_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/lookbacktest"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

type markers struct {
	values         map[string]bool
	outage         bool
	checks, writes int
}

func (d *markers) Check(k string) bool       { d.checks++; return !d.outage && d.values[k] }
func (d *markers) CheckAndAdd(k string) bool { panic("must use batched post-receipt marking") }
func (d *markers) Close()                    {}
func (d *markers) CheckMany(keys []utils.DedupeKey) []bool {
	result := make([]bool, len(keys))
	for i, k := range keys {
		result[i] = d.Check(k.Key)
	}
	return result
}
func (d *markers) AddMany(keys []utils.DedupeKey) {
	for _, k := range keys {
		d.writes++
		if !d.outage {
			d.values[k.Key] = true
		}
	}
}
func config() (*utils.LookbackConfig, *markers) {
	d := &markers{values: map[string]bool{}}
	return &utils.LookbackConfig{Window: time.Hour, Namespace: "record", Deduper: d}, d
}
func events(c *utils.LookbackConfig, n int) []utils.ReplayEvent {
	out := make([]utils.ReplayEvent, n)
	for i := range out {
		out[i] = utils.ReplayEvent{Key: c.Key(time.Unix(100, 0), "source", fmt.Sprint(i)), Message: &protocol.DataMessage{TextPayload: fmt.Sprint(i)}}
	}
	return out
}
func TestLookbackReceiptChunksAndReplay(t *testing.T) {
	client, received := lookbacktest.Client(t, nil)
	c, d := config()
	input := events(c, 257)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, utils.DeliverLookback(ctx, c, client, input, client.ShipWithReceipt))
	require.Len(t, d.values, 257)
	for i := 0; i < 257; i++ {
		m := lookbacktest.Receive(t, received)
		if i == 255 || i == 256 {
			require.True(t, m.AckRequested)
		}
	}
	calls := 0
	admit := func(m *protocol.DataMessage, d time.Duration) (uspclient.AdmissionReceipt, error) {
		calls++
		return client.ShipWithReceipt(m, d)
	}
	require.NoError(t, utils.DeliverLookback(ctx, c, client, input, admit))
	require.Zero(t, calls)
	d.values = map[string]bool{}
	d.outage = true
	require.NoError(t, utils.DeliverLookback(ctx, c, client, input[:2], admit))
	require.Equal(t, 2, calls)
	// A marker write outage loses no delivery; a fresh process must replay.
	require.Empty(t, d.values)
	d.outage = false
	require.NoError(t, utils.DeliverLookback(ctx, c, client, input[:2], admit))
	require.Equal(t, 4, calls)
}
func TestLookbackUnconfirmedCrashDoesNotMark(t *testing.T) {
	gate := make(chan struct{})
	client, received := lookbacktest.Client(t, gate)
	c, d := config()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- utils.DeliverLookback(ctx, c, client, events(c, 257), client.ShipWithReceipt) }()
	for i := 0; i < 256; i++ {
		lookbacktest.Receive(t, received)
	}
	select {
	case <-received:
		t.Fatal("advanced beyond unconfirmed chunk")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, d.writes)
	close(gate)
}

type observation struct{ value uspclient.ReceiptObservation }

func (o observation) ObserveReceipt(uspclient.AdmissionReceipt) uspclient.ReceiptObservation {
	return o.value
}
func TestLookbackRejectsUnusableReceiptsAndFailedAdmission(t *testing.T) {
	for _, o := range []uspclient.ReceiptObservation{{}, {Lifetime: uspclient.ReceiptClosing, Frame: uspclient.ReceiptAcknowledged}, {Lifetime: uspclient.ReceiptClosed, Frame: uspclient.ReceiptClosedUnconfirmed}} {
		c, d := config()
		require.Error(t, utils.DeliverLookback(context.Background(), c, observation{o}, events(c, 1), func(*protocol.DataMessage, time.Duration) (uspclient.AdmissionReceipt, error) {
			return uspclient.AdmissionReceipt{}, nil
		}))
		require.Zero(t, d.writes)
	}
	c, d := config()
	require.Error(t, utils.DeliverLookback(context.Background(), c, observation{}, events(c, 1), func(*protocol.DataMessage, time.Duration) (uspclient.AdmissionReceipt, error) {
		return uspclient.AdmissionReceipt{}, errors.New("admission failed")
	}))
	require.Zero(t, d.writes)
}
func TestLookbackTailDuplicatesRequestACKAndStableKeys(t *testing.T) {
	client, received := lookbacktest.Client(t, nil)
	c, d := config()
	input := events(c, 2)
	input = append(input, input[0], input[1])
	d.values[input[1].Key.Key] = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, utils.DeliverLookback(ctx, c, client, input, client.ShipWithReceipt))
	require.True(t, lookbacktest.Receive(t, received).AckRequested)
	require.Equal(t, 1, d.writes)
	key := c.Key(time.Unix(100, 0), "a", "bc")
	require.NotEqual(t, key.Key, c.Key(key.Time, "ab", "c").Key)
	c.Namespace = "other"
	require.NotEqual(t, key.Key, c.Key(key.Time, "a", "bc").Key)
	require.Equal(t, time.Unix(100, 0), key.Time)
}

func TestLookbackPartialAdmissionMarksOnlyConfirmedPrefix(t *testing.T) {
	client, _ := lookbacktest.Client(t, nil)
	c, d := config()
	input := events(c, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	err := utils.DeliverLookback(ctx, c, client, input, func(m *protocol.DataMessage, timeout time.Duration) (uspclient.AdmissionReceipt, error) {
		calls++
		if calls == 2 {
			return uspclient.AdmissionReceipt{}, errors.New("failed tail")
		}
		m.AckRequested = true
		receipt, err := client.ShipWithReceipt(m, timeout)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return client.ObserveReceipt(receipt).Frame == uspclient.ReceiptAcknowledged }, 3*time.Second, time.Millisecond)
		return receipt, nil
	})
	require.Error(t, err)
	require.True(t, d.values[input[0].Key.Key])
	require.False(t, d.values[input[1].Key.Key])
}
func TestLookbackSourceAliasesAndConfiguration(t *testing.T) {
	require.Equal(t, utils.LookbackSource("https://EXAMPLE.invalid:443/"), utils.LookbackSource("https://example.invalid"))
	c, _ := config()
	require.NoError(t, c.Validate())
	c.Window = 0
	require.Error(t, c.Validate())
	var disabled *utils.LookbackConfig
	require.NoError(t, disabled.Validate())
	now := time.Now()
	require.True(t, disabled.Start(now).Equal(now))
}
