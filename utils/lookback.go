package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

// LookbackConfig is host-only: nil preserves ordinary behavior; Namespace is stable.
type LookbackConfig struct {
	Window    time.Duration
	Deduper   CheckingDeduper
	Namespace string
	Observer  func(LookbackObservation)
}

// LookbackFailure is a bounded failure category, never vendor error text.
type LookbackFailure uint8

const (
	LookbackFailureNone LookbackFailure = iota
	LookbackFailureRequest
	LookbackFailureInvalidPage
	LookbackFailureContinuation
	LookbackFailureDelivery
	LookbackFailureHistoryPressure
)

// LookbackObservation contains deltas, including ongoing collection after startup.
// Starts and Completed count initial collection streams, not adapter instances.
// InvalidIDs counts skipped IDs (Okta) or rejected IDs (Entra, which retries the
// whole page). Fetched counts decoded page items, including repeats and rejects.
// Confirmed counts receipt ACKs, not durable dedup marker writes. Failures counts
// failed attempts by Failure; retries may count the same failure again.
type LookbackObservation struct {
	Starts, Completed, Fetched, Confirmed, InvalidIDs, Duplicates, Failures uint64
	Failure                                                                 LookbackFailure
}

// Observe emits a count delta if enabled. The callback must return promptly and
// be safe for concurrent calls when shared between adapter instances. Configure
// it before starting collection; it must not change collection state.
func (c *LookbackConfig) Observe(delta LookbackObservation) {
	if c != nil && c.Observer != nil {
		c.Observer(delta)
	}
}

func (c *LookbackConfig) Validate() error {
	if c != nil && (c.Window <= 0 || c.Deduper == nil || c.Namespace == "") {
		return errors.New("invalid lookback configuration")
	}
	return nil
}
func (c *LookbackConfig) Start(now time.Time) time.Time {
	if c == nil {
		return now.UTC()
	}
	return now.UTC().Add(-c.Window)
}

// Key namespaces adapter/source/collection/ID, with stable source time.
func (c *LookbackConfig) Key(sourceTime time.Time, fields ...string) DedupeKey {
	raw, _ := json.Marshal(append([]string{c.Namespace}, fields...))
	sum := sha256.Sum256(raw)
	return DedupeKey{Key: "lookback/v1/" + hex.EncodeToString(sum[:]), Time: sourceTime}
}

type ReceiptObserver interface {
	ObserveReceipt(uspclient.AdmissionReceipt) uspclient.ReceiptObservation
}
type ReplayEvent struct {
	Key     DedupeKey
	Message *protocol.DataMessage
}
type ReceiptAdmit func(*protocol.DataMessage, time.Duration) (uspclient.AdmissionReceipt, error)

const LookbackChunkSize = 256

// DeliverLookback confirms chunks (256 events / 1 MiB; one larger event alone).
// Callers bound pages. Errors never mark unconfirmed data or advance progress.
func DeliverLookback(ctx context.Context, c *LookbackConfig, observer ReceiptObserver, events []ReplayEvent, admit ReceiptAdmit, warning ...func(string)) (err error) {
	defer func() {
		if err != nil && ctx.Err() == nil && !errors.Is(err, uspclient.ErrorClosed) {
			c.Observe(LookbackObservation{Failures: 1, Failure: LookbackFailureDelivery})
		}
	}()
	nextWarning := time.Now().Add(10 * time.Second)
	warn := func() {
		if len(warning) > 0 && warning[0] != nil && !time.Now().Before(nextWarning) {
			warning[0]("stream falling behind")
			nextWarning = time.Now().Add(time.Minute)
		}
	}
	for len(events) > 0 {
		n, size := 0, 0
		for n < len(events) && n < LookbackChunkSize {
			raw, err := json.Marshal(events[n].Message)
			if err != nil {
				return err
			}
			if n > 0 && size+len(raw) > 1<<20 {
				break
			}
			size += len(raw)
			n++
		}
		if err := deliverChunk(ctx, c, observer, events[:n], admit, warn); err != nil {
			return err
		}
		events = events[n:]
	}
	return nil
}
func deliverChunk(ctx context.Context, c *LookbackConfig, observer ReceiptObserver, events []ReplayEvent, admit ReceiptAdmit, warn func()) error {
	keys := make([]DedupeKey, len(events))
	for i, event := range events {
		keys[i] = event.Key
	}
	hits := make([]bool, len(keys))
	batch, batched := c.Deduper.(BatchDeduper)
	if batched {
		copy(hits, batch.CheckMany(keys))
	} else {
		for i, key := range keys {
			if key.Key != "" {
				hits[i] = c.Deduper.Check(key.Key)
			}
		}
	}
	selected := make([]int, 0, len(events))
	seen := make(map[string]bool)
	for i, key := range keys {
		if key.Key != "" && (hits[i] || seen[key.Key]) {
			c.Observe(LookbackObservation{Duplicates: 1})
			continue
		}
		selected = append(selected, i)
		seen[key.Key] = true
	}
	receipts := make([]uspclient.AdmissionReceipt, 0, len(selected))
	markConfirmed := func() {
		confirmed := make([]DedupeKey, 0, len(receipts))
		for j, receipt := range receipts {
			o := observer.ObserveReceipt(receipt)
			key := keys[selected[j]]
			if key.Key != "" && o.Lifetime == uspclient.ReceiptLive && o.Frame == uspclient.ReceiptAcknowledged {
				confirmed = append(confirmed, key)
			}
		}
		if len(confirmed) > 0 {
			c.Observe(LookbackObservation{Confirmed: uint64(len(confirmed))})
		}
		if batched && len(confirmed) > 0 {
			batch.AddMany(confirmed)
		} else {
			for _, key := range confirmed {
				c.Deduper.CheckAndAdd(key.Key)
			}
		}
	}
	defer markConfirmed()
	for j, i := range selected {
		// Copy before requesting an ACK; the SDK owns the message after admission.
		msg := *events[i].Message
		msg.AckRequested = j == len(selected)-1
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			receipt, err := admit(&msg, 250*time.Millisecond)
			if errors.Is(err, uspclient.ErrorBufferFull) {
				warn()
				acktrack.Wait(ctx, 10*time.Millisecond)
				continue
			}
			if err != nil {
				return err
			}
			receipts = append(receipts, receipt)
			break
		}
	}
	for len(receipts) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		all := true
		for _, receipt := range receipts {
			o := observer.ObserveReceipt(receipt)
			if o.Lifetime != uspclient.ReceiptLive || (o.Frame != uspclient.ReceiptRetained && o.Frame != uspclient.ReceiptAcknowledged) {
				return errors.New("lookback receipt no longer confirmable")
			}
			all = all && o.Frame == uspclient.ReceiptAcknowledged
		}
		if all {
			return nil
		}
		warn()
		acktrack.Wait(ctx, 10*time.Millisecond)
	}
	return nil
}

// LookbackSource canonicalizes harmless origin aliases without changing path case.
func LookbackSource(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	if (u.Scheme == "https" && u.Port() == "443") || (u.Scheme == "http" && u.Port() == "80") {
		u.Host = strings.TrimSuffix(u.Host, ":"+u.Port())
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String()
}
