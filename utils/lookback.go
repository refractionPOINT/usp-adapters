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
)

// LookbackConfig is host-only. Nil keeps ordinary startup and collection behavior.
// Namespace identifies the configured destination across process restarts.
// Deduper must fail reads open and make writes best effort.
type LookbackConfig struct {
	Window    time.Duration
	Deduper   CheckingDeduper
	Namespace string
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

// Key includes the complete namespace even for legacy single-key dedup backends.
// Fields identify adapter, source, collection, vendor ID and (if mutable) revision.
// Source time must be stable for the same revision; unknown time is zero.
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

// DeliverLookback holds at most one synchronous chunk of receipts, never an
// entire scan. Pages are already bounded by the caller. The encoded payload
// budget is 1 MiB per chunk, except that one larger event may travel alone.
// A failure leaves source progress unchanged. Cancellation/closing never marks
// unconfirmed data; marker outages cause safe replay on the next traversal.
func DeliverLookback(ctx context.Context, c *LookbackConfig, observer ReceiptObserver, events []ReplayEvent, admit ReceiptAdmit) error {
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
		if err := deliverChunk(ctx, c, observer, events[:n], admit); err != nil {
			return err
		}
		events = events[n:]
	}
	return nil
}
func deliverChunk(ctx context.Context, c *LookbackConfig, observer ReceiptObserver, events []ReplayEvent, admit ReceiptAdmit) error {
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
			continue
		}
		selected = append(selected, i)
		if key.Key != "" {
			seen[key.Key] = true
		}
	}
	receipts := make([]uspclient.AdmissionReceipt, 0, len(selected))
	markConfirmed := func() int {
		confirmed := make([]DedupeKey, 0, len(receipts))
		count := 0
		for j, receipt := range receipts {
			o := observer.ObserveReceipt(receipt)
			if o.Lifetime == uspclient.ReceiptLive && o.Frame == uspclient.ReceiptAcknowledged {
				count++
				if key := keys[selected[j]]; key.Key != "" {
					confirmed = append(confirmed, key)
				}
			}
		}
		if len(confirmed) > 0 {
			if batched {
				batch.AddMany(confirmed)
			} else {
				for _, key := range confirmed {
					c.Deduper.CheckAndAdd(key.Key)
				}
			}
		}
		return count
	}
	for j, i := range selected {
		// Copy before requesting an ACK; the SDK owns the message after admission.
		msg := *events[i].Message
		msg.AckRequested = j == len(selected)-1
		for {
			if err := ctx.Err(); err != nil {
				markConfirmed()
				return err
			}
			receipt, err := admit(&msg, 250*time.Millisecond)
			if errors.Is(err, uspclient.ErrorBufferFull) {
				if !lookbackWait(ctx) {
					markConfirmed()
					return ctx.Err()
				}
				continue
			}
			if err != nil {
				markConfirmed()
				return err
			}
			receipts = append(receipts, receipt)
			break
		}
	}
	for len(receipts) > 0 {
		if err := ctx.Err(); err != nil {
			markConfirmed()
			return err
		}
		all := true
		for _, receipt := range receipts {
			o := observer.ObserveReceipt(receipt)
			if o.Lifetime != uspclient.ReceiptLive || (o.Frame != uspclient.ReceiptRetained && o.Frame != uspclient.ReceiptAcknowledged) {
				markConfirmed()
				return errors.New("lookback receipt no longer confirmable")
			}
			if o.Frame != uspclient.ReceiptAcknowledged {
				all = false
			}
		}
		if all {
			markConfirmed()
			return nil
		}
		if !lookbackWait(ctx) {
			markConfirmed()
			return ctx.Err()
		}
	}
	return nil
}
func lookbackWait(ctx context.Context) bool {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
