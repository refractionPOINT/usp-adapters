package acktrack

import (
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type receiptSequenceClient struct{ observerTestClient }

func (c *receiptSequenceClient) ShipWithReceipt(m *protocol.DataMessage, d time.Duration) (uspclient.AdmissionReceipt, error) {
	return uspclient.AdmissionReceipt{}, c.Ship(m, d)
}

// This fake proves only the tracker sequence invariant; delivery tests use SDK receipts.
func TestLookbackReceiptBridgePreservesSequenceInvariant(t *testing.T) {
	c := &receiptSequenceClient{}
	tracker := observerTracker(t, c)
	first, _, err := tracker.ShipWithReceipt(&protocol.DataMessage{}, time.Second)
	require.NoError(t, err)
	second := observerShip(t, tracker)
	_, _, err = tracker.ShipWithReceipt(&protocol.DataMessage{}, time.Second)
	require.NoError(t, err)
	require.NoError(t, tracker.Candidate("stream", []byte("page"), first, second))
	c.ack(2)
	observerConfirm(t, tracker, map[string]string{"stream": "page"})
	c.next++ // Skipping even one sequence must fail closed across BOTH entrypoints.
	_, _, err = tracker.ShipWithReceipt(&protocol.DataMessage{}, time.Second)
	require.ErrorIs(t, err, ErrSequence)
	_, err = tracker.Ship(&protocol.DataMessage{}, time.Second)
	require.ErrorIs(t, err, ErrSequence)
}
