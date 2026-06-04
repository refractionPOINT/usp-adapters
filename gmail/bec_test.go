package usp_gmail

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the BEC capabilities: the configuration-state collectors
// (filters, forwarding, send-as, delegates, imap/pop, vacation) and the mailbox
// history collector. It extends the mock Gmail API in mock_test.go with the
// settings, profile and history endpoints.

// --- mock handlers for the BEC endpoints ------------------------------------

// capListHandler serves a settings list endpoint of the shape {"<field>": [...]},
// honoring any configured status override.
func (m *mockGmail) capListHandler(pathKey, field string, get func() []utils.Dict) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.authorize(w, r) {
			return
		}
		m.mu.Lock()
		m.capabilityRequests[pathKey]++
		status := m.statusOverride[pathKey]
		items := append([]utils.Dict(nil), get()...)
		m.mu.Unlock()

		if status != 0 {
			writeJSON(w, status, capErrorBody(status))
			return
		}
		resp := map[string]any{}
		if len(items) > 0 {
			resp[field] = items
		}
		writeJSON(w, http.StatusOK, mustJSONString(resp))
	}
}

// capObjectHandler serves a singleton settings endpoint (the whole body is the
// resource), honoring any configured status override.
func (m *mockGmail) capObjectHandler(pathKey string, get func() utils.Dict) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.authorize(w, r) {
			return
		}
		m.mu.Lock()
		m.capabilityRequests[pathKey]++
		status := m.statusOverride[pathKey]
		obj := get()
		m.mu.Unlock()

		if status != 0 {
			writeJSON(w, status, capErrorBody(status))
			return
		}
		if obj == nil {
			obj = utils.Dict{}
		}
		writeJSON(w, http.StatusOK, mustJSONString(obj))
	}
}

// profileHandler serves users.getProfile, reporting the current historyId.
func (m *mockGmail) profileHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.authorize(w, r) {
			return
		}
		m.mu.Lock()
		m.capabilityRequests["profile"]++
		hid := m.profileHistoryID
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, fmt.Sprintf(
			`{"emailAddress":%q,"messagesTotal":10,"threadsTotal":8,"historyId":%q}`, mockSubject, hid))
	}
}

// historyHandler serves users.history.list, returning the records whose id is
// strictly greater than startHistoryId, with a top-level historyId that advances
// the cursor monotonically.
func (m *mockGmail) historyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.authorize(w, r) {
			return
		}
		startN, _ := strconv.ParseUint(r.URL.Query().Get("startHistoryId"), 10, 64)

		m.mu.Lock()
		m.capabilityRequests["history"]++
		status := m.statusOverride["history"]
		var recs []utils.Dict
		maxID := startN
		for _, rec := range m.history {
			idN, _ := strconv.ParseUint(rec.FindOneString("id"), 10, 64)
			if idN > startN {
				recs = append(recs, rec)
			}
			if idN > maxID {
				maxID = idN
			}
		}
		m.mu.Unlock()

		if status != 0 {
			writeJSON(w, status, capErrorBody(status))
			return
		}
		resp := map[string]any{"historyId": strconv.FormatUint(maxID, 10)}
		if len(recs) > 0 {
			resp["history"] = recs
		}
		writeJSON(w, http.StatusOK, mustJSONString(resp))
	}
}

func capErrorBody(status int) string {
	return fmt.Sprintf(
		`{"error":{"code":%d,"message":"simulated","errors":[{"reason":"simulated"}],"status":"ERROR"}}`, status)
}

func mustJSONString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// --- mock mutators (used by tests; all lock m.mu) ---------------------------

func (m *mockGmail) setFilters(f ...utils.Dict) { m.mu.Lock(); m.filters = f; m.mu.Unlock() }
func (m *mockGmail) setForwarding(f ...utils.Dict) {
	m.mu.Lock()
	m.forwardingAddresses = f
	m.mu.Unlock()
}
func (m *mockGmail) setSendAs(s ...utils.Dict)      { m.mu.Lock(); m.sendAs = s; m.mu.Unlock() }
func (m *mockGmail) setDelegates(d ...utils.Dict)   { m.mu.Lock(); m.delegates = d; m.mu.Unlock() }
func (m *mockGmail) setAutoForwarding(d utils.Dict) { m.mu.Lock(); m.autoForwarding = d; m.mu.Unlock() }
func (m *mockGmail) setImap(d utils.Dict)           { m.mu.Lock(); m.imap = d; m.mu.Unlock() }
func (m *mockGmail) setPop(d utils.Dict)            { m.mu.Lock(); m.pop = d; m.mu.Unlock() }
func (m *mockGmail) setVacation(d utils.Dict)       { m.mu.Lock(); m.vacation = d; m.mu.Unlock() }
func (m *mockGmail) setProfileHistoryID(id string) {
	m.mu.Lock()
	m.profileHistoryID = id
	m.mu.Unlock()
}
func (m *mockGmail) addHistory(recs ...utils.Dict) {
	m.mu.Lock()
	m.history = append(m.history, recs...)
	m.mu.Unlock()
}
func (m *mockGmail) overrideStatus(key string, s int) {
	m.mu.Lock()
	m.statusOverride[key] = s
	m.mu.Unlock()
}

// --- test helpers -----------------------------------------------------------

// becConfig is a refresh-token config pointed at the mock with message
// collection OFF and fast settings/poll cadences, so a test can enable just the
// capabilities it exercises. The caller flips the Collect* flags it wants.
func becConfig(t *testing.T, baseURL string) GmailConfig {
	t.Helper()
	c := refreshConfig(t, baseURL)
	c.CollectMessages = false
	c.SettingsPollInterval = 25 * time.Millisecond
	return c
}

// eventsByType returns the JSON payloads of all shipped events of a given type.
func eventsByType(msgs []*protocol.DataMessage, evtType string) []utils.Dict {
	var out []utils.Dict
	for _, m := range msgs {
		if m.EventType == evtType {
			out = append(out, utils.Dict(m.JsonPayload))
		}
	}
	return out
}

func countByType(sink *captureSink, evtType string) int {
	return len(eventsByType(sink.snapshot(), evtType))
}

// --- tests ------------------------------------------------------------------

// TestFiltersChangeOnly verifies a mail filter ships once when first seen, is not
// re-shipped while unchanged, a newly-added filter ships, and an edited filter
// re-ships (change detection).
func TestFiltersChangeOnly(t *testing.T) {
	mock := newMockGmail()
	f1 := utils.Dict{
		"id":       "f1",
		"criteria": utils.Dict{"from": "ceo@company.test"},
		"action":   utils.Dict{"forward": "attacker@evil.test", "removeLabelIds": []any{"INBOX"}},
	}
	mock.setFilters(f1)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectFilters = true
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return countByType(sink, eventTypeFilter) == 1 },
		5*time.Second, 20*time.Millisecond, "the filter should ship once")
	require.Never(t, func() bool { return countByType(sink, eventTypeFilter) != 1 },
		300*time.Millisecond, 30*time.Millisecond, "an unchanged filter must not re-ship")

	got := eventsByType(sink.snapshot(), eventTypeFilter)[0]
	assert.Equal(t, "attacker@evil.test", got.FindOneString("action/forward"),
		"the filter payload must be preserved verbatim")

	// A second filter appears -> ships (total 2), the first still not re-shipped.
	f2 := utils.Dict{"id": "f2", "criteria": utils.Dict{"subject": "wire"}, "action": utils.Dict{"addLabelIds": []any{"TRASH"}}}
	mock.setFilters(f1, f2)
	require.Eventually(t, func() bool { return countByType(sink, eventTypeFilter) == 2 },
		5*time.Second, 20*time.Millisecond, "the new filter should ship")

	// The first filter is edited -> its content changed, so it re-ships (total 3).
	f1edited := utils.Dict{
		"id":       "f1",
		"criteria": utils.Dict{"from": "ceo@company.test"},
		"action":   utils.Dict{"forward": "attacker2@evil.test", "removeLabelIds": []any{"INBOX"}},
	}
	mock.setFilters(f1edited, f2)
	require.Eventually(t, func() bool { return countByType(sink, eventTypeFilter) == 3 },
		5*time.Second, 20*time.Millisecond, "the edited filter should re-ship")
	require.Never(t, func() bool { return countByType(sink, eventTypeFilter) > 3 },
		300*time.Millisecond, 30*time.Millisecond)
}

// TestForwardingCapability verifies forwarding addresses and the account-wide
// auto-forwarding setting are both collected under their distinct event types.
func TestForwardingCapability(t *testing.T) {
	mock := newMockGmail()
	mock.setForwarding(utils.Dict{"forwardingEmail": "exfil@evil.test", "verificationStatus": "accepted"})
	mock.setAutoForwarding(utils.Dict{"enabled": true, "emailAddress": "exfil@evil.test", "disposition": "archive"})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectForwarding = true
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		return countByType(sink, eventTypeForwardingAddress) == 1 && countByType(sink, eventTypeAutoForwarding) == 1
	}, 5*time.Second, 20*time.Millisecond, "forwarding address and auto-forwarding should ship")
	require.Never(t, func() bool {
		return countByType(sink, eventTypeForwardingAddress) > 1 || countByType(sink, eventTypeAutoForwarding) > 1
	}, 300*time.Millisecond, 30*time.Millisecond, "unchanged forwarding state must not re-ship")

	fa := eventsByType(sink.snapshot(), eventTypeForwardingAddress)[0]
	assert.Equal(t, "exfil@evil.test", fa.FindOneString("forwardingEmail"))
}

// TestSendAsAndImapPopVacation verifies the send-as, imap, pop and vacation
// capabilities each ship their resource once.
func TestSendAsAndImapPopVacation(t *testing.T) {
	mock := newMockGmail()
	mock.setSendAs(utils.Dict{"sendAsEmail": "ceo@company.test", "displayName": "CEO", "verificationStatus": "accepted"})
	mock.setImap(utils.Dict{"enabled": true})
	mock.setPop(utils.Dict{"accessWindow": "allMail"})
	mock.setVacation(utils.Dict{"enableAutoReply": false})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectSendAs = true
	conf.CollectImapPop = true
	conf.CollectVacation = true
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		return countByType(sink, eventTypeSendAs) == 1 &&
			countByType(sink, eventTypeImap) == 1 &&
			countByType(sink, eventTypePop) == 1 &&
			countByType(sink, eventTypeVacation) == 1
	}, 5*time.Second, 20*time.Millisecond, "send-as, imap, pop and vacation should each ship once")

	sa := eventsByType(sink.snapshot(), eventTypeSendAs)[0]
	assert.Equal(t, "ceo@company.test", sa.FindOneString("sendAsEmail"))
}

// TestDelegatesCollection verifies a mailbox delegate is collected once under
// its event type.
func TestDelegatesCollection(t *testing.T) {
	mock := newMockGmail()
	mock.setDelegates(utils.Dict{"delegateEmail": "attacker@evil.test", "verificationStatus": "accepted"})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectDelegates = true
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return countByType(sink, eventTypeDelegate) == 1 },
		5*time.Second, 20*time.Millisecond, "the delegate should ship once")
	require.Never(t, func() bool { return countByType(sink, eventTypeDelegate) != 1 },
		300*time.Millisecond, 30*time.Millisecond, "an unchanged delegate must not re-ship")

	d := eventsByType(sink.snapshot(), eventTypeDelegate)[0]
	assert.Equal(t, "attacker@evil.test", d.FindOneString("delegateEmail"))
}

// TestDelegateUnavailableIsNonFatal verifies that a delegates endpoint that
// answers 403 (the feature is unavailable for the account type) is skipped
// without stopping the adapter -- a sibling capability keeps collecting.
func TestDelegateUnavailableIsNonFatal(t *testing.T) {
	mock := newMockGmail()
	mock.overrideStatus("settings/delegates", http.StatusForbidden)
	mock.setFilters(utils.Dict{"id": "f1", "action": utils.Dict{"forward": "x@evil.test"}})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectFilters = true
	conf.CollectDelegates = true
	adapter, chStopped, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return countByType(sink, eventTypeFilter) == 1 },
		5*time.Second, 20*time.Millisecond, "the filter capability should keep working")

	select {
	case <-chStopped:
		t.Fatal("a 403 on the delegates capability must not stop the adapter")
	case <-time.After(300 * time.Millisecond):
	}
	assert.Equal(t, 0, countByType(sink, eventTypeDelegate), "no delegate should ship when the endpoint 403s")
}

// TestHistoryCollection verifies history baselines silently on first run, then
// ships deletion/label-change records that occur after the baseline, exactly once.
func TestHistoryCollection(t *testing.T) {
	mock := newMockGmail()
	mock.setProfileHistoryID("1000") // baseline cursor

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectHistory = true
	adapter, _, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	// Let the baseline get established; nothing should ship yet.
	require.Never(t, func() bool { return countByType(sink, eventTypeHistory) > 0 },
		300*time.Millisecond, 30*time.Millisecond, "the baseline run must ship no history")

	// An attacker deletes the fraud thread and marks an alert read, after baseline.
	mock.addHistory(
		utils.Dict{"id": "1001", "messagesDeleted": []any{utils.Dict{"message": utils.Dict{"id": "m-fraud"}}}},
		utils.Dict{"id": "1002", "labelsRemoved": []any{utils.Dict{"message": utils.Dict{"id": "m-alert"}, "labelIds": []any{"UNREAD"}}}},
	)

	require.Eventually(t, func() bool { return countByType(sink, eventTypeHistory) == 2 },
		5*time.Second, 20*time.Millisecond, "the two post-baseline history records should ship")
	require.Never(t, func() bool { return countByType(sink, eventTypeHistory) != 2 },
		300*time.Millisecond, 30*time.Millisecond, "advancing the cursor must prevent re-shipping")

	recs := eventsByType(sink.snapshot(), eventTypeHistory)
	ids := map[string]bool{}
	for _, r := range recs {
		ids[r.FindOneString("id")] = true
	}
	assert.True(t, ids["1001"] && ids["1002"], "both history records should be present")
}

// TestHistoryCursorExpiryReBaselines verifies that a 404 from history.list (the
// cursor aged out of Gmail's retained history) makes the adapter re-baseline
// rather than stop.
func TestHistoryCursorExpiryReBaselines(t *testing.T) {
	mock := newMockGmail()
	mock.setProfileHistoryID("1000")
	mock.overrideStatus("history", http.StatusNotFound)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := becConfig(t, server.URL)
	conf.CollectHistory = true
	adapter, chStopped, err := newGmailAdapter(ctx, conf, staticSink(sink))
	require.NoError(t, err)
	defer adapter.Close()

	// First cycle baselines (cursor=1000). Add a record so the next list is
	// attempted; it 404s, the adapter forgets the cursor and re-baselines. It
	// must stay alive throughout.
	mock.addHistory(utils.Dict{"id": "1001", "messagesDeleted": []any{utils.Dict{"message": utils.Dict{"id": "m1"}}}})

	select {
	case <-chStopped:
		t.Fatal("a 404 (expired cursor) must not stop the adapter")
	case <-time.After(500 * time.Millisecond):
	}

	// Clear the override; after re-baselining, new records ship again.
	mock.overrideStatus("history", 0)
	mock.setProfileHistoryID("2000")
	mock.addHistory(utils.Dict{"id": "2001", "messagesDeleted": []any{utils.Dict{"message": utils.Dict{"id": "m2"}}}})
	require.Eventually(t, func() bool { return countByType(sink, eventTypeHistory) >= 1 },
		5*time.Second, 20*time.Millisecond, "history collection should resume after re-baselining")
}
