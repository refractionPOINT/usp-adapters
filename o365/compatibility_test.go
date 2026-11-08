package usp_o365

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

func TestConstructorRejectsIncompatibleDeduper(t *testing.T) {
	conf := validConfig(t)
	conf.Deduper = newLegacyDeduper()
	// Compatibility must be checked before endpoint/client initialization.
	conf.Endpoint = "invalid"
	conf.ClientOptions.TestSinkMode = false
	a, stopped, err := NewOffice365Adapter(context.Background(), conf)
	require.ErrorContains(t, err, "Deduper must implement utils.CheckingDeduper")
	require.Nil(t, a)
	require.Nil(t, stopped)
}

func TestConstructorRejectsReceiptlessSinkMode(t *testing.T) {
	conf := validConfig(t)
	conf.Endpoint = "invalid"
	conf.ClientOptions.TestSinkMode = true
	a, stopped, err := NewOffice365Adapter(context.Background(), conf)
	require.ErrorContains(t, err, "TestSinkMode is unsupported")
	require.ErrorContains(t, err, "requires USP receipts")
	require.Nil(t, a)
	require.Nil(t, stopped)
}

// This sink controls admission errors only. Delivery confirmation is tested
// separately with real SDK websocket peers in ack_test.go.
type admissionErrorSink struct {
	captureSink
	admit func(*protocol.DataMessage, time.Duration) (uspclient.AdmissionReceipt, error)
}

func (s *admissionErrorSink) ShipWithReceipt(m *protocol.DataMessage, d time.Duration) (uspclient.AdmissionReceipt, error) {
	return s.admit(m, d)
}

func TestAdmissionBackpressureWarnsOncePerAttempt(t *testing.T) {
	calls, warnings := 0, 0
	sink := &admissionErrorSink{}
	sink.admit = func(m *protocol.DataMessage, d time.Duration) (uspclient.AdmissionReceipt, error) {
		calls++
		require.Equal(t, "record", m.TextPayload)
		// The warning must arrive at the first full buffer, before retrying.
		require.Equal(t, (calls+1)/3, warnings)
		if calls%3 != 0 {
			return uspclient.AdmissionReceipt{}, uspclient.ErrorBufferFull
		}
		return uspclient.AdmissionReceipt{}, nil
	}
	conf := validConfig(t)
	conf.ClientOptions.OnWarning = func(message string) { require.Equal(t, "stream falling behind", message); warnings++ }
	a := &Office365Adapter{conf: conf, uspClient: sink, doStop: utils.NewEvent()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for attempt := 1; attempt <= 2; attempt++ {
		_, ok := a.admitContext(ctx, "record")
		require.True(t, ok)
		require.Equal(t, attempt, warnings)
	}
	require.Equal(t, 6, calls)
}

func TestAdmissionBackpressureWarningOnFailureOrCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		name := "failure"
		if cancelled {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			calls, warnings := 0, 0
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sink := &admissionErrorSink{}
			sink.admit = func(*protocol.DataMessage, time.Duration) (uspclient.AdmissionReceipt, error) {
				calls++
				if calls == 1 {
					if cancelled {
						cancel()
					}
					return uspclient.AdmissionReceipt{}, uspclient.ErrorBufferFull
				}
				return uspclient.AdmissionReceipt{}, errors.New("synthetic admission failure")
			}
			conf := validConfig(t)
			conf.ClientOptions.OnWarning = func(msg string) { require.Equal(t, "stream falling behind", msg); warnings++ }
			a := &Office365Adapter{conf: conf, uspClient: sink, doStop: utils.NewEvent()}
			_, ok := a.admitContext(ctx, "record")
			require.False(t, ok)
			require.Equal(t, 1, warnings)
			if cancelled {
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, 2, calls)
			}
		})
	}
}
