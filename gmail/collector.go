package usp_gmail

// This file implements the per-mailbox collector. The adapter (client.go) runs
// one mailboxCollector per mailbox it is configured to watch, and each collector
// ships to that mailbox's own LimaCharlie sensor. A collector owns everything
// that is mailbox-specific: its Gmail client (bound to one impersonated subject),
// its token source, its sensor sink, its collection state (the message
// high-water mark, the settings clock, the history cursor) and its dedupe keys.
//
// Collectors share nothing mutable with each other except, optionally, a single
// embedder-provided deduper -- and even then every dedupe key is namespaced with
// the mailbox address so two mailboxes that hold an identical item (the same
// filter, say) never suppress each other.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// Dedupe-key namespaces. Every key is additionally prefixed with the mailbox
// address (see dkey) so keys never collide across mailboxes.
const (
	dedupeKindMessage = "msg"
	dedupeKindHistory = "history"
)

// mailboxCollector collects from exactly one mailbox.
type mailboxCollector struct {
	a       *GmailAdapter
	mailbox string // mailbox address; identifies the sensor and namespaces dedupe keys
	tag     string // log prefix, e.g. `gmail[user@x]`

	client    *GmailClient
	ts        *tokenSource
	uspClient uspSink

	deduper     utils.Deduper
	ownsDeduper bool

	// since is the high-water mark of the message collection window. It advances
	// to the poll's end time after every fully-successful message poll.
	since time.Time
	// lastSettings is when the configuration-state capabilities last ran.
	lastSettings time.Time
	// lastHistoryID is the users.history.list cursor.
	lastHistoryID string

	// startDelay staggers the collector's first poll so that, when many mailboxes
	// start together, they do not all hit the Gmail API in the same instant.
	startDelay time.Duration

	// stop carries the stop signal two ways: the Event drives WaitFor sleeps and
	// IsSet checks, while stopCh (closed once) is selectable alongside the
	// adapter's poll-slot channel. requestStop trips both.
	stop     *utils.Event
	stopCh   chan struct{}
	stopOnce sync.Once

	stopped   chan struct{} // closed when run() returns
	cleanOnce sync.Once
}

// requestStop signals this collector to wind down. It is idempotent.
func (c *mailboxCollector) requestStop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.stop.Set()
	})
}

// run polls this mailbox until the collector is stopped (by the adapter closing,
// or by this mailbox being deprovisioned, or by a fatal credential error).
func (c *mailboxCollector) run() {
	defer c.a.wgSenders.Done()
	defer close(c.stopped)
	defer c.a.conf.ClientOptions.DebugLog(c.tag + ": collection stopped")

	if c.startDelay > 0 {
		if c.stop.WaitFor(c.startDelay) {
			return
		}
	}

	isFirstRun := true
	for isFirstRun || !c.stop.WaitFor(c.a.conf.PollInterval) {
		isFirstRun = false
		c.runCycle()
	}
}

// runCycle runs one tick of every enabled capability for this mailbox, while
// holding one of the adapter's bounded poll slots so concurrent API usage across
// all mailboxes stays capped. A capability failing does not abort the others.
func (c *mailboxCollector) runCycle() {
	if !c.acquire() {
		return
	}
	defer c.release()

	if c.a.conf.CollectMessages {
		c.pollMessages()
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.CollectHistory {
		c.pollHistory()
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.anySettingsCapabilityEnabled() && c.settingsDue(time.Now()) {
		c.pollSettings()
		c.lastSettings = time.Now()
	}
}

// acquire takes a poll slot from the adapter's concurrency limiter, returning
// false if the collector is asked to stop while waiting.
func (c *mailboxCollector) acquire() bool {
	select {
	case c.a.pollSlots <- struct{}{}:
		return true
	case <-c.stopCh:
		return false
	}
}

func (c *mailboxCollector) release() { <-c.a.pollSlots }

// settingsDue reports whether the configuration-state capabilities should run
// this cycle: always on the first pass, then once per SettingsPollInterval.
func (c *mailboxCollector) settingsDue(now time.Time) bool {
	return c.lastSettings.IsZero() || now.Sub(c.lastSettings) >= c.a.conf.SettingsPollInterval
}

// dkey builds a dedupe key namespaced to this mailbox so identical items in
// different mailboxes never collide (and so a shared embedder deduper stays
// correct across mailboxes).
func (c *mailboxCollector) dkey(kind, id string) string {
	return c.mailbox + "\x00" + kind + "\x00" + id
}

// pollMessages performs one message collection cycle over the rolling window:
// list message ids, fetch each not-yet-seen message, and ship it. On success the
// window's high-water mark advances; on any error it does not, so the next poll
// re-covers the same range (the deduper prevents re-shipping).
func (c *mailboxCollector) pollMessages() {
	end := time.Now()
	start := c.since.Add(-c.a.conf.Overlap)
	query := buildQuery(c.a.conf.Query, start)

	pageToken := ""
	nFetched, nShipped := 0, 0
	for page := 0; page < maxPagesPerPoll; page++ {
		if c.stop.IsSet() {
			return
		}

		raw, ok := c.list(query, pageToken)
		if !ok {
			return
		}
		list, err := parseListMessages(raw)
		if err != nil {
			c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: list parse error: %v", c.tag, err))
			return
		}

		for _, ref := range list.Messages {
			if c.stop.IsSet() {
				return
			}
			if ref.ID == "" {
				continue
			}

			// Fetch before consulting the deduper: a fetch can fail and we must
			// not mark a message seen unless it actually shipped.
			msg, status := c.fetch(ref.ID)
			switch status {
			case fetchSkip:
				continue
			case fetchAbort:
				return
			}
			nFetched++

			if c.deduper.CheckAndAdd(c.dkey(dedupeKindMessage, ref.ID)) {
				continue
			}
			if !c.ship(msg) {
				return
			}
			nShipped++
		}

		if list.NextPageToken == "" {
			break
		}
		pageToken = list.NextPageToken
	}

	// Only advance the high-water mark once the poll fully succeeded.
	c.since = end
	c.a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"%s: poll complete (fetched=%d shipped=%d) up to %s",
		c.tag, nFetched, nShipped, end.UTC().Format(time.RFC3339)))
}

// list issues one messages.list request with transient-retry backoff. The bool
// is false when the poll should be abandoned (a permanent error, or the
// collector is stopping).
func (c *mailboxCollector) list(query, pageToken string) ([]byte, bool) {
	raw, err := c.withRetry("messages.list", func() ([]byte, error) {
		return c.client.ListMessages(c.a.ctx, listMessagesParams{
			query:            query,
			pageToken:        pageToken,
			maxResults:       c.a.conf.MaxResults,
			includeSpamTrash: c.a.conf.IncludeSpamTrash,
			labelIDs:         c.a.conf.LabelIDs,
		})
	})
	if err != nil {
		c.handlePermanent("messages.list", err)
		return nil, false
	}
	return raw, true
}

// fetchResult tells the poll what to do after a messages.get attempt.
type fetchResult int

const (
	fetchOK    fetchResult = iota // message fetched, proceed to dedupe/ship
	fetchSkip                     // message gone (404); skip it, keep polling
	fetchAbort                    // unrecoverable; abandon this poll
)

// fetch retrieves one message with transient-retry backoff and classifies the
// outcome. A 404 (the message was deleted between the list and the get) is
// benign and skipped; auth/permission failures abort and stop this collector.
func (c *mailboxCollector) fetch(id string) (utils.Dict, fetchResult) {
	raw, err := c.withRetry("messages.get", func() ([]byte, error) {
		return c.client.GetMessage(c.a.ctx, id, c.a.conf.Format, c.a.conf.MetadataHeaders)
	})
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			c.a.conf.ClientOptions.DebugLog(fmt.Sprintf("%s: message %s vanished before fetch; skipping", c.tag, id))
			return nil, fetchSkip
		}
		c.handlePermanent("messages.get", err)
		return nil, fetchAbort
	}

	msg, err := utils.UnmarshalCleanJSON(string(raw))
	if err != nil {
		c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: failed to parse message %s: %v", c.tag, id, err))
		return nil, fetchAbort
	}
	return utils.Dict(msg), fetchOK
}

// withRetry runs fn, retrying transient errors with exponential backoff. It
// returns the final result, or the final (non-transient or attempts-exhausted)
// error for the caller to classify. It does not set the stop event.
func (c *mailboxCollector) withRetry(label string, fn func() ([]byte, error)) ([]byte, error) {
	var raw []byte
	var err error
	for attempt := 0; attempt < c.a.conf.MaxRetryAttempts; attempt++ {
		if c.stop.IsSet() {
			return nil, context.Canceled
		}

		raw, err = fn()
		if err == nil {
			return raw, nil
		}
		if !isTransientError(err) {
			return nil, err
		}
		if attempt+1 >= c.a.conf.MaxRetryAttempts {
			break
		}
		delay := c.a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > c.a.conf.MaxRetryDelay {
			delay = c.a.conf.MaxRetryDelay
		}
		c.a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"%s: %s transient error (attempt %d/%d), retrying in %v: %v",
			c.tag, label, attempt+1, c.a.conf.MaxRetryAttempts, delay, err))
		if c.stop.WaitFor(delay) {
			return nil, context.Canceled
		}
	}
	return nil, fmt.Errorf("%s failed after %d attempts: %w", label, c.a.conf.MaxRetryAttempts, err)
}

// handlePermanent logs a non-recoverable error and, when it is a credential
// failure, stops this collector so it does not spin uselessly every interval. A
// credential problem needs operator attention; other permanent errors (e.g. a
// bad query) abandon this poll but keep the collector alive to retry next
// interval. Only this mailbox's collector is affected -- its siblings keep
// running, since a rejected impersonation for one subject does not imply the
// others are dead.
func (c *mailboxCollector) handlePermanent(label string, err error) {
	if errors.Is(err, context.Canceled) {
		// Collector is stopping; not an error to surface.
		return
	}

	var tokenErr *TokenError
	var httpErr *HTTPError
	if errors.As(err, &tokenErr) ||
		(errors.As(err, &httpErr) &&
			(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden)) {
		c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: %s failed: %v", c.tag, label, err))
		c.a.conf.ClientOptions.OnError(fmt.Errorf(
			"%s: credentials rejected for this mailbox. Verify the OAuth client / refresh token "+
				"(or that domain-wide delegation grants the %q scope for subject %q), and that the "+
				"Gmail API is enabled for the project", c.tag, strings.Join(c.a.conf.Scopes, ","), c.mailbox))
		c.requestStop()
		return
	}
	c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: %s failed: %v", c.tag, label, err))
}

// ship forwards a single incoming message to this mailbox's sensor.
func (c *mailboxCollector) ship(msg utils.Dict) bool {
	return c.shipEvent(eventTypeMessage, msg, eventTime(msg))
}

// shipEvent forwards one event of the given type to this mailbox's sensor. It
// returns false if the collector should stop (an unrecoverable shipping error).
func (c *mailboxCollector) shipEvent(evtType string, payload utils.Dict, tsMs uint64) bool {
	dataMsg := &protocol.DataMessage{
		JsonPayload: payload,
		EventType:   evtType,
		TimestampMs: tsMs,
	}
	if err := c.uspClient.Ship(dataMsg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			c.a.conf.ClientOptions.OnWarning(c.tag + ": stream falling behind")
			err = c.uspClient.Ship(dataMsg, 1*time.Hour)
		}
		if err != nil {
			c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: Ship(): %v", c.tag, err))
			c.requestStop()
			return false
		}
	}
	return true
}

// cleanup drains and closes this collector's sensor and releases its owned
// deduper. It is idempotent and safe to call once the run loop has returned. The
// token source and Gmail client hold shared HTTP transports owned by the adapter,
// so they are not closed here.
func (c *mailboxCollector) cleanup() error {
	var err error
	c.cleanOnce.Do(func() {
		if c.uspClient != nil {
			err = c.uspClient.Drain(1 * time.Minute)
			if _, e := c.uspClient.Close(); err == nil {
				err = e
			}
		}
		if c.ownsDeduper && c.deduper != nil {
			c.deduper.Close()
		}
	})
	return err
}

// eventTime extracts the message's event time from its internalDate (epoch
// milliseconds, as a string), falling back to now when it is absent or
// unparseable.
func eventTime(msg utils.Dict) uint64 {
	if raw := msg.FindOneString("internalDate"); raw != "" {
		if ms, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && ms > 0 {
			return uint64(ms)
		}
	}
	return uint64(time.Now().UnixMilli())
}

// buildQuery appends a time bound to the configured search query so each poll
// only lists messages received at or after `start`. Gmail's `after:` operator
// accepts an epoch-seconds value for second-level precision.
func buildQuery(base string, start time.Time) string {
	bound := fmt.Sprintf("after:%d", start.Unix())
	base = strings.TrimSpace(base)
	if base == "" {
		return bound
	}
	return base + " " + bound
}
