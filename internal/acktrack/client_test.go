package acktrack

import (
	"context"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/stretchr/testify/require"
)

// This mode deliberately discards data; its empty pending set means immediate
// local sink acceptance, never production proxy or downstream acceptance.
func TestRealSDKTestSinkLocalAdmissions(t *testing.T) {
	client, err := NewClient(context.Background(), uspclient.ClientOptions{TestSinkMode: true})
	require.NoError(t, err)
	defer client.Close()
	tracker := observerTracker(t, client)
	for i := uint64(1); i <= 3; i++ {
		message := &protocol.DataMessage{SeqNum: 99, TextPayload: "fixture"}
		dep, err := tracker.Ship(message, time.Millisecond)
		require.NoError(t, err, "real testing sink must admit consecutive records")
		require.Equal(t, i, message.SeqNum, "only explicit testing sink gets local sequences")
		require.NoError(t, tracker.Candidate("stream", []byte{byte(i)}, dep))
	}
	pending, err := client.GetUnsent()
	require.NoError(t, err)
	require.Empty(t, pending)
	positions, err := tracker.Confirm()
	require.NoError(t, err)
	require.Equal(t, []byte{3}, positions["stream"], "the whole testing sink prefix is immediately accepted")
	positions, err = tracker.Confirm()
	require.NoError(t, err)
	require.Empty(t, positions)
	client.BeginDrain()
	require.NoError(t, client.Drain(time.Millisecond))
	pending, err = client.Close()
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestStrictTrackerRejectsUnadaptedRealSDKTestSink(t *testing.T) {
	// Constructors must select the adapter explicitly. Never relax sequence
	// validation to infer testing mode from repeated zero or an empty pending set.
	client, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{TestSinkMode: true})
	require.NoError(t, err)
	defer client.Close()
	tracker := observerTracker(t, client)
	_, err = tracker.Ship(&protocol.DataMessage{}, time.Millisecond)
	require.NoError(t, err)
	dep, err := tracker.Ship(&protocol.DataMessage{}, time.Millisecond)
	require.ErrorIs(t, err, ErrSequence, "raw nonconsecutive SDK sequences must still fail closed")
	require.ErrorIs(t, tracker.Candidate("stream", []byte("invalid"), dep), ErrSequence)
}
