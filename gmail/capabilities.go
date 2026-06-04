package usp_gmail

// This file implements the BEC (Business Email Compromise) capabilities: the
// configuration-state collectors (mail filters, forwarding addresses, send-as
// aliases, delegates, IMAP/POP, vacation responder) and the mailbox change
// history collector. They run per mailbox, as methods on mailboxCollector, and
// complement the message-telemetry collector in collector.go.
//
// The configuration-state collectors are change-only: an item is shipped when it
// first appears or its content changes, and suppressed otherwise. This is what
// surfaces the persistence and exfiltration changes typical of BEC -- an attacker
// adding an auto-forward rule, a forwarding address, a send-as identity, or a
// delegate -- without flooding the stream with the steady state on every poll.
// Change detection reuses the collector's deduper, keyed on a hash of each item's
// content (namespaced by mailbox), so an unchanged item maps to a key already
// seen.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	// historyMaxResults is the page size for users.history.list.
	historyMaxResults = 500
)

// historyChangeTypes restricts users.history.list to the change kinds that
// matter for BEC investigation/response: messages permanently deleted, and
// labels added/removed (e.g. marking a security alert read, or trashing the
// fraud thread). New-message arrivals are intentionally excluded -- the message
// telemetry capability already covers those.
var historyChangeTypes = []string{"messageDeleted", "labelAdded", "labelRemoved"}

// pollSettings runs each enabled configuration-state capability once. A single
// capability failing (e.g. delegates on a consumer account) does not abort the
// others.
func (c *mailboxCollector) pollSettings() {
	if c.a.conf.CollectFilters {
		c.pollListCapability("filters", eventTypeFilter, "filter", "filter", c.client.ListFilters)
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.CollectForwarding {
		c.pollListCapability("forwardingAddresses", eventTypeForwardingAddress,
			"forwardingAddresses", "forwarding", c.client.ListForwardingAddresses)
		if c.stop.IsSet() {
			return
		}
		c.pollSingletonCapability("autoForwarding", eventTypeAutoForwarding,
			"autoforwarding", c.client.GetAutoForwarding)
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.CollectSendAs {
		c.pollListCapability("sendAs", eventTypeSendAs, "sendAs", "sendas", c.client.ListSendAs)
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.CollectDelegates {
		c.pollListCapability("delegates", eventTypeDelegate, "delegates", "delegate", c.client.ListDelegates)
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.CollectImapPop {
		c.pollSingletonCapability("imap", eventTypeImap, "imap", c.client.GetImap)
		if c.stop.IsSet() {
			return
		}
		c.pollSingletonCapability("pop", eventTypePop, "pop", c.client.GetPop)
	}
	if c.stop.IsSet() {
		return
	}
	if c.a.conf.CollectVacation {
		c.pollSingletonCapability("vacation", eventTypeVacation, "vacation", c.client.GetVacation)
	}
}

// pollListCapability runs one list-shaped configuration-state capability: fetch
// the list, then change-ship each item. listField is the JSON array field in the
// response (e.g. "filter", "sendAs"); keyPrefix namespaces the dedupe keys.
func (c *mailboxCollector) pollListCapability(label, evtType, listField, keyPrefix string, fetch func(context.Context) ([]byte, error)) {
	d, ok := c.fetchCapability(label, fetch)
	if !ok {
		return
	}
	items, _ := d.GetListOfDict(listField)
	for _, item := range items {
		if c.stop.IsSet() {
			return
		}
		if !c.shipState(evtType, keyPrefix, item) {
			return
		}
	}
}

// pollSingletonCapability runs one object-shaped configuration-state capability:
// fetch the single settings object and change-ship it as a whole.
func (c *mailboxCollector) pollSingletonCapability(label, evtType, keyPrefix string, fetch func(context.Context) ([]byte, error)) {
	d, ok := c.fetchCapability(label, fetch)
	if !ok {
		return
	}
	c.shipState(evtType, keyPrefix, d)
}

// fetchCapability fetches and parses one capability's response, applying the
// transient-retry backoff and the non-fatal capability error handling. ok is
// false when the capability should be skipped this cycle.
func (c *mailboxCollector) fetchCapability(label string, fetch func(context.Context) ([]byte, error)) (utils.Dict, bool) {
	raw, err := c.withRetry(label, func() ([]byte, error) { return fetch(c.a.ctx) })
	if err != nil {
		c.handleCapabilityError(label, err)
		return nil, false
	}
	parsed, err := utils.UnmarshalCleanJSON(string(raw))
	if err != nil {
		c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: %s parse error: %v", c.tag, label, err))
		return nil, false
	}
	return utils.Dict(parsed), true
}

// shipState ships a configuration-state item under evtType, but only when it is
// new or its content changed since it was last seen. The dedupe key is the
// mailbox plus keyPrefix plus a hash of the item's content, so an unchanged item
// on a later poll maps to a key already in the deduper and is suppressed, while
// any change yields a new key and re-ships. Returns false if the collector
// should stop.
func (c *mailboxCollector) shipState(evtType, keyPrefix string, payload utils.Dict) bool {
	key := c.dkey(keyPrefix, contentHash(payload))
	if c.deduper.CheckAndAdd(key) {
		return true // unchanged since a recent poll; already shipped
	}
	return c.shipEvent(evtType, payload, uint64(time.Now().UnixMilli()))
}

// contentHash is a stable hash of a decoded JSON object. encoding/json sorts map
// keys, so two equal objects hash identically regardless of field order.
func contentHash(d utils.Dict) string {
	b, err := json.Marshal(d)
	if err != nil {
		b = []byte(fmt.Sprintf("%v", d))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// handleCapabilityError classifies an error from a BEC capability. A genuine
// authentication failure (a token error, or a 401) stops this collector, exactly
// as it does for the message poller -- the credentials for this mailbox need
// operator attention. Everything else is logged and the capability is skipped
// for this cycle, leaving the other capabilities running: a feature unavailable
// for this account type (delegates on a consumer account surfaces as 400/403), a
// settings scope the token lacks, or a transient blip that outlived its retries.
func (c *mailboxCollector) handleCapabilityError(label string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	var tokenErr *TokenError
	var httpErr *HTTPError
	if errors.As(err, &tokenErr) ||
		(errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized) {
		c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: %s failed (credentials rejected): %v", c.tag, label, err))
		c.requestStop()
		return
	}
	c.a.conf.ClientOptions.OnWarning(fmt.Sprintf("%s: %s unavailable, skipping this cycle: %v", c.tag, label, err))
}

// pollHistory collects mailbox change records (deletions and label changes) via
// users.history.list. On the first run it establishes a baseline historyId from
// the profile and ships nothing -- there is no prior state to diff against. Each
// later run lists history forward from the cursor, shipping each record, and
// advances the cursor only after a fully-successful pass (the deduper prevents
// re-shipping if a pass is re-covered). A 404 means the cursor aged out (Gmail
// keeps history for roughly a week); the collector re-baselines and resumes.
func (c *mailboxCollector) pollHistory() {
	if c.lastHistoryID == "" {
		if id, ok := c.fetchHistoryBaseline(); ok {
			c.lastHistoryID = id
			c.a.conf.ClientOptions.DebugLog(c.tag + ": history baseline established at " + id)
		}
		return
	}

	pageToken := ""
	newCursor := c.lastHistoryID
	nShipped := 0
	for page := 0; page < maxPagesPerPoll; page++ {
		if c.stop.IsSet() {
			return
		}

		raw, err := c.withRetry("history.list", func() ([]byte, error) {
			return c.client.ListHistory(c.a.ctx, listHistoryParams{
				startHistoryID: c.lastHistoryID,
				pageToken:      pageToken,
				maxResults:     historyMaxResults,
				historyTypes:   historyChangeTypes,
			})
		})
		if err != nil {
			var httpErr *HTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
				// The cursor aged out of Gmail's retained history. Forget it so
				// the next cycle re-baselines from the current profile.
				c.a.conf.ClientOptions.OnWarning(c.tag + ": history cursor expired; re-baselining next cycle")
				c.lastHistoryID = ""
				return
			}
			c.handleCapabilityError("history.list", err)
			return
		}

		parsed, err := utils.UnmarshalCleanJSON(string(raw))
		if err != nil {
			c.a.conf.ClientOptions.OnError(fmt.Errorf("%s: history parse error: %v", c.tag, err))
			return
		}
		d := utils.Dict(parsed)

		// The top-level historyId is the mailbox's current id; advance the cursor
		// to it so the next poll starts where this one ended.
		if hid := d.FindOneString("historyId"); hid != "" {
			newCursor = hid
		}

		records, _ := d.GetListOfDict("history")
		for _, rec := range records {
			if c.stop.IsSet() {
				return
			}
			id := rec.FindOneString("id")
			if id == "" {
				continue
			}
			if c.deduper.CheckAndAdd(c.dkey(dedupeKindHistory, id)) {
				continue
			}
			if !c.shipEvent(eventTypeHistory, rec, uint64(time.Now().UnixMilli())) {
				return
			}
			nShipped++
		}

		pageToken = d.FindOneString("nextPageToken")
		if pageToken == "" {
			break
		}
	}

	c.lastHistoryID = newCursor
	c.a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"%s: history poll complete (shipped=%d) cursor=%s", c.tag, nShipped, c.lastHistoryID))
}

// fetchHistoryBaseline reads the mailbox profile for its current historyId, used
// as the starting cursor for incremental history collection.
func (c *mailboxCollector) fetchHistoryBaseline() (string, bool) {
	d, ok := c.fetchCapability("getProfile", c.client.GetProfile)
	if !ok {
		return "", false
	}
	id := d.FindOneString("historyId")
	if id == "" {
		c.a.conf.ClientOptions.OnWarning(c.tag + ": profile returned no historyId; will retry next cycle")
		return "", false
	}
	return id, true
}
