package usp_gmail

// This file implements the BEC (Business Email Compromise) capabilities: the
// configuration-state collectors (mail filters, forwarding addresses, send-as
// aliases, delegates, IMAP/POP, vacation responder) and the mailbox change
// history collector. These complement the message-telemetry collector in
// client.go.
//
// The configuration-state collectors are change-only: an item is shipped when it
// first appears or its content changes, and suppressed otherwise. This is what
// surfaces the persistence and exfiltration changes typical of BEC -- an attacker
// adding an auto-forward rule, a forwarding address, a send-as identity, or a
// delegate -- without flooding the stream with the steady state on every poll.
// Change detection reuses the adapter's deduper, keyed on a hash of each item's
// content, so an unchanged item maps to a key already seen.

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
func (a *GmailAdapter) pollSettings() {
	if a.conf.CollectFilters {
		a.pollListCapability("filters", eventTypeFilter, "filter", "filter", a.client.ListFilters)
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.CollectForwarding {
		a.pollListCapability("forwardingAddresses", eventTypeForwardingAddress,
			"forwardingAddresses", "forwarding", a.client.ListForwardingAddresses)
		if a.doStop.IsSet() {
			return
		}
		a.pollSingletonCapability("autoForwarding", eventTypeAutoForwarding,
			"autoforwarding", a.client.GetAutoForwarding)
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.CollectSendAs {
		a.pollListCapability("sendAs", eventTypeSendAs, "sendAs", "sendas", a.client.ListSendAs)
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.CollectDelegates {
		a.pollListCapability("delegates", eventTypeDelegate, "delegates", "delegate", a.client.ListDelegates)
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.CollectImapPop {
		a.pollSingletonCapability("imap", eventTypeImap, "imap", a.client.GetImap)
		if a.doStop.IsSet() {
			return
		}
		a.pollSingletonCapability("pop", eventTypePop, "pop", a.client.GetPop)
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.CollectVacation {
		a.pollSingletonCapability("vacation", eventTypeVacation, "vacation", a.client.GetVacation)
	}
}

// pollListCapability runs one list-shaped configuration-state capability: fetch
// the list, then change-ship each item. listField is the JSON array field in the
// response (e.g. "filter", "sendAs"); keyPrefix namespaces the dedupe keys.
func (a *GmailAdapter) pollListCapability(label, evtType, listField, keyPrefix string, fetch func(context.Context) ([]byte, error)) {
	d, ok := a.fetchCapability(label, fetch)
	if !ok {
		return
	}
	items, _ := d.GetListOfDict(listField)
	for _, item := range items {
		if a.doStop.IsSet() {
			return
		}
		if !a.shipState(evtType, keyPrefix, item) {
			return
		}
	}
}

// pollSingletonCapability runs one object-shaped configuration-state capability:
// fetch the single settings object and change-ship it as a whole.
func (a *GmailAdapter) pollSingletonCapability(label, evtType, keyPrefix string, fetch func(context.Context) ([]byte, error)) {
	d, ok := a.fetchCapability(label, fetch)
	if !ok {
		return
	}
	a.shipState(evtType, keyPrefix, d)
}

// fetchCapability fetches and parses one capability's response, applying the
// transient-retry backoff and the non-fatal capability error handling. ok is
// false when the capability should be skipped this cycle.
func (a *GmailAdapter) fetchCapability(label string, fetch func(context.Context) ([]byte, error)) (utils.Dict, bool) {
	raw, err := a.withRetry(label, func() ([]byte, error) { return fetch(a.ctx) })
	if err != nil {
		a.handleCapabilityError(label, err)
		return nil, false
	}
	parsed, err := utils.UnmarshalCleanJSON(string(raw))
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("gmail: %s parse error: %v", label, err))
		return nil, false
	}
	return utils.Dict(parsed), true
}

// shipState ships a configuration-state item under evtType, but only when it is
// new or its content changed since it was last seen. The dedupe key is keyPrefix
// plus a hash of the item's content, so an unchanged item on a later poll maps to
// a key already in the deduper and is suppressed, while any change yields a new
// key and re-ships. Returns false if the adapter should stop.
func (a *GmailAdapter) shipState(evtType, keyPrefix string, payload utils.Dict) bool {
	key := keyPrefix + ":" + contentHash(payload)
	if a.deduper.CheckAndAdd(key) {
		return true // unchanged since a recent poll; already shipped
	}
	return a.shipEvent(evtType, payload, uint64(time.Now().UnixMilli()))
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
// authentication failure (a token error, or a 401) stops the adapter, exactly as
// it does for the message poller -- the credentials need operator attention.
// Everything else is logged and the capability is skipped for this cycle, leaving
// the other capabilities running: a feature unavailable for this account type
// (delegates on a consumer account surfaces as 400/403), a settings scope the
// token lacks, or a transient blip that outlived its retries.
func (a *GmailAdapter) handleCapabilityError(label string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	var tokenErr *TokenError
	var httpErr *HTTPError
	if errors.As(err, &tokenErr) ||
		(errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized) {
		a.conf.ClientOptions.OnError(fmt.Errorf("gmail: %s failed (credentials rejected): %v", label, err))
		a.doStop.Set()
		return
	}
	a.conf.ClientOptions.OnWarning(fmt.Sprintf("gmail: %s unavailable, skipping this cycle: %v", label, err))
}

// pollHistory collects mailbox change records (deletions and label changes) via
// users.history.list. On the first run it establishes a baseline historyId from
// the profile and ships nothing -- there is no prior state to diff against. Each
// later run lists history forward from the cursor, shipping each record, and
// advances the cursor only after a fully-successful pass (the deduper prevents
// re-shipping if a pass is re-covered). A 404 means the cursor aged out (Gmail
// keeps history for roughly a week); the adapter re-baselines and resumes.
func (a *GmailAdapter) pollHistory() {
	if a.lastHistoryID == "" {
		if id, ok := a.fetchHistoryBaseline(); ok {
			a.lastHistoryID = id
			a.conf.ClientOptions.DebugLog("gmail: history baseline established at " + id)
		}
		return
	}

	pageToken := ""
	newCursor := a.lastHistoryID
	nShipped := 0
	for page := 0; page < maxPagesPerPoll; page++ {
		if a.doStop.IsSet() {
			return
		}

		raw, err := a.withRetry("history.list", func() ([]byte, error) {
			return a.client.ListHistory(a.ctx, listHistoryParams{
				startHistoryID: a.lastHistoryID,
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
				a.conf.ClientOptions.OnWarning("gmail: history cursor expired; re-baselining next cycle")
				a.lastHistoryID = ""
				return
			}
			a.handleCapabilityError("history.list", err)
			return
		}

		parsed, err := utils.UnmarshalCleanJSON(string(raw))
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("gmail: history parse error: %v", err))
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
			if a.doStop.IsSet() {
				return
			}
			id := rec.FindOneString("id")
			if id == "" {
				continue
			}
			if a.deduper.CheckAndAdd("history:" + id) {
				continue
			}
			if !a.shipEvent(eventTypeHistory, rec, uint64(time.Now().UnixMilli())) {
				return
			}
			nShipped++
		}

		pageToken = d.FindOneString("nextPageToken")
		if pageToken == "" {
			break
		}
	}

	a.lastHistoryID = newCursor
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"gmail: history poll complete (shipped=%d) cursor=%s", nShipped, a.lastHistoryID))
}

// fetchHistoryBaseline reads the mailbox profile for its current historyId, used
// as the starting cursor for incremental history collection.
func (a *GmailAdapter) fetchHistoryBaseline() (string, bool) {
	d, ok := a.fetchCapability("getProfile", a.client.GetProfile)
	if !ok {
		return "", false
	}
	id := d.FindOneString("historyId")
	if id == "" {
		a.conf.ClientOptions.OnWarning("gmail: profile returned no historyId; will retry next cycle")
		return "", false
	}
	return id, true
}
