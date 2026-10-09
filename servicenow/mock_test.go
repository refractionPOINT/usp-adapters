package usp_servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the adapter end-to-end against a mock ServiceNow REST
// Table API, capturing the exact messages it ships so their content -- event
// type, timestamp and verbatim payload -- can be asserted.

// --- in-memory USP sink -----------------------------------------------------

// captureSink is an in-memory uspSink that records every shipped message.
type captureSink struct {
	mu       sync.Mutex
	messages []*protocol.DataMessage
}

func (s *captureSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, m)
	return nil
}

func (s *captureSink) Drain(time.Duration) error               { return nil }
func (s *captureSink) Close() ([]*protocol.DataMessage, error) { return nil, nil }

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

func (s *captureSink) snapshot() []*protocol.DataMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*protocol.DataMessage, len(s.messages))
	copy(out, s.messages)
	return out
}

// --- mock ServiceNow Table API ------------------------------------------------

// mockServiceNow is an in-memory stand-in for the ServiceNow REST Table API.
// It honours the Table API contract the adapter relies on:
//
//   - GET /api/now/v2/table/{tableName} with HTTP Basic authentication;
//   - sysparm_query with "^"-joined conditions: "field=value" equality,
//     "field>=value" comparison (chronological for the fixed-width
//     "yyyy-MM-dd HH:mm:ss" format), and an ORDERBY<field> clause;
//   - sysparm_offset / sysparm_limit pagination over the *matching* set;
//   - a {"result": [...]} envelope, an X-Total-Count header, and a Link
//     rel="next" header whenever more matching records remain;
//   - like the real API, ACL filtering (the aclHidden set) is applied *after*
//     sysparm_limit slicing, so a page can come back short -- or empty --
//     while the Link header still advertises a next page.
type mockServiceNow struct {
	mu        sync.Mutex
	username  string
	password  string
	tables    map[string][]utils.Dict // table name -> records
	aclHidden map[string]bool         // sys_ids removed from pages post-pagination
	requests  int
}

func newMockServiceNow(username, password string) *mockServiceNow {
	return &mockServiceNow{
		username:  username,
		password:  password,
		tables:    map[string][]utils.Dict{},
		aclHidden: map[string]bool{},
	}
}

func (m *mockServiceNow) setTable(table string, records []utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tables[table] = records
}

// appendRecord adds a record to a table, as the real platform would when a
// new audit row is inserted.
func (m *mockServiceNow) appendRecord(table string, record utils.Dict) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tables[table] = append(m.tables[table], record)
}

func (m *mockServiceNow) hideFromACL(sysID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aclHidden[sysID] = true
}

func (m *mockServiceNow) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

// queryCondition is one parsed "^"-separated element of a sysparm_query.
type queryCondition struct {
	field string
	op    string // "=" or ">="
	value string
}

// parseEncodedQuery splits a sysparm_query into its conditions and its
// ORDERBY field.
func parseEncodedQuery(q string) (conds []queryCondition, orderBy string) {
	for _, part := range strings.Split(q, "^") {
		if part == "" {
			continue
		}
		if f, ok := strings.CutPrefix(part, "ORDERBY"); ok {
			orderBy = f
			continue
		}
		if i := strings.Index(part, ">="); i > 0 {
			conds = append(conds, queryCondition{field: part[:i], op: ">=", value: part[i+2:]})
			continue
		}
		if i := strings.Index(part, "="); i > 0 {
			conds = append(conds, queryCondition{field: part[:i], op: "=", value: part[i+1:]})
		}
	}
	return conds, orderBy
}

func matchesConditions(record utils.Dict, conds []queryCondition) bool {
	for _, c := range conds {
		v := record.FindOneString(c.field)
		switch c.op {
		case "=":
			if v != c.value {
				return false
			}
		case ">=":
			// Lexicographic comparison is chronological for the fixed-width
			// "yyyy-MM-dd HH:mm:ss" format, which is how the real platform
			// compares date/time values too.
			if v < c.value {
				return false
			}
		}
	}
	return true
}

func (m *mockServiceNow) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requests++
		m.mu.Unlock()

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// The Table API authenticates with HTTP Basic auth. The error
		// envelope mirrors the platform's.
		user, pass, ok := r.BasicAuth()
		if !ok || user != m.username || pass != m.password {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"User Not Authenticated","detail":"Required to provide Auth information"},"status":"failure"}`))
			return
		}

		table, ok := strings.CutPrefix(r.URL.Path, "/api/now/v2/table/")
		if !ok || table == "" || strings.Contains(table, "/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"Invalid table","detail":null},"status":"failure"}`))
			return
		}

		q := r.URL.Query()
		conds, orderBy := parseEncodedQuery(q.Get("sysparm_query"))
		limit := 10000 // the platform's documented default sysparm_limit
		if v, err := strconv.Atoi(q.Get("sysparm_limit")); err == nil && v > 0 {
			limit = v
		}
		offset := 0
		if v, err := strconv.Atoi(q.Get("sysparm_offset")); err == nil && v > 0 {
			offset = v
		}

		m.mu.Lock()
		records := m.tables[table]
		matching := make([]utils.Dict, 0, len(records))
		for _, rec := range records {
			if matchesConditions(rec, conds) {
				matching = append(matching, rec)
			}
		}
		hidden := make(map[string]bool, len(m.aclHidden))
		for k, v := range m.aclHidden {
			hidden[k] = v
		}
		m.mu.Unlock()

		if orderBy != "" {
			sort.SliceStable(matching, func(i, j int) bool {
				return matching[i].FindOneString(orderBy) < matching[j].FindOneString(orderBy)
			})
		}

		total := len(matching)

		// Slice the page by offset/limit *before* ACL filtering, exactly like
		// the real platform ("This limit is applied before ACL evaluation").
		var page []utils.Dict
		if offset < total {
			end := offset + limit
			if end > total {
				end = total
			}
			page = matching[offset:end]
		}
		visible := make([]utils.Dict, 0, len(page))
		for _, rec := range page {
			if !hidden[rec.FindOneString("sys_id")] {
				visible = append(visible, rec)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Total-Count", strconv.Itoa(total))
		if offset+limit < total {
			next := *r.URL
			nq := next.Query()
			nq.Set("sysparm_offset", strconv.Itoa(offset+limit))
			next.RawQuery = nq.Encode()
			w.Header().Set("Link", fmt.Sprintf(`<%s>;rel="next"`, next.String()))
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"result": visible})
	}
}

// --- realistic record fixtures ------------------------------------------------

// snTime renders a time in the Table API's "yyyy-MM-dd HH:mm:ss" UTC format.
func snTime(t time.Time) string {
	return t.UTC().Format(serviceNowTimeLayout)
}

// realisticSysAudit returns a record shaped like a real sys_audit row as the
// Table API returns it: the documented audit columns, every value a plain
// string (sys_audit has no reference-link objects), UTC timestamps.
func realisticSysAudit(n int, createdOn string) utils.Dict {
	return utils.Dict{
		"sys_id":              fmt.Sprintf("b1c3d2e4f5a601001a2b3c4d5e6f%04x", n),
		"tablename":           "incident",
		"fieldname":           "assigned_to",
		"documentkey":         "9d385017c611228701d22104cc95c371",
		"user":                "jane.doe",
		"oldvalue":            "46d44a5dc0a8010e0000c8b06e0b1971",
		"newvalue":            "5137153cc611227c000bbd1bd8cd2007",
		"reason":              "",
		"record_checkpoint":   "7",
		"internal_checkpoint": "",
		"sys_created_on":      createdOn,
		"sys_created_by":      "jane.doe",
	}
}

// realisticSysEvent returns a record shaped like a sysevent (event log) row
// for a login event.
func realisticSysEvent(n int, name, createdOn string) utils.Dict {
	return utils.Dict{
		"sys_id":         fmt.Sprintf("e9f8a7b6c5d401001a2b3c4d5e6f%04x", n),
		"name":           name,
		"parm1":          "jane.doe",
		"parm2":          "203.0.113.10",
		"table":          "sys_user",
		"instance":       "",
		"state":          "processed",
		"process_on":     createdOn,
		"processed":      createdOn,
		"queue":          "",
		"uri":            "/login.do",
		"sys_created_on": createdOn,
		"sys_created_by": "system",
	}
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// --- tests ------------------------------------------------------------------

// TestMockSysAuditEndToEnd drives the adapter against the mock API and asserts
// the exact events shipped: event type, timestamp parsed from the record's
// sys_created_on, and the payload preserved verbatim.
func TestMockSysAuditEndToEnd(t *testing.T) {
	mock := newMockServiceNow("lc.collector", "s3cret")
	base := time.Now().UTC().Add(-time.Hour)
	want := []utils.Dict{
		realisticSysAudit(1, snTime(base)),
		realisticSysAudit(2, snTime(base.Add(1*time.Second))),
		realisticSysAudit(3, snTime(base.Add(2*time.Second))),
	}
	mock.setTable("sys_audit", want)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "lc.collector",
		Password:      "s3cret",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		PollInterval:  40 * time.Millisecond,
	}
	adapter, _, err := newServiceNowAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "expected all 3 audit records to ship")

	// Re-polling must not re-ship: the count stays at 3.
	require.Never(t, func() bool { return sink.count() != 3 },
		300*time.Millisecond, 30*time.Millisecond, "records were re-shipped on a later poll")

	byID := map[string]*protocol.DataMessage{}
	for _, msg := range sink.snapshot() {
		assert.Equal(t, "sys_audit", msg.EventType)
		require.NotNil(t, msg.JsonPayload)
		id, _ := msg.JsonPayload["sys_id"].(string)
		require.NotEmpty(t, id)
		byID[id] = msg
	}
	require.Len(t, byID, 3)

	for _, src := range want {
		id := src["sys_id"].(string)
		msg := byID[id]
		require.NotNil(t, msg, "record %s was not shipped", id)

		// The event timestamp is parsed from sys_created_on, as UTC.
		ts, perr := time.Parse(serviceNowTimeLayout, src["sys_created_on"].(string))
		require.NoError(t, perr)
		assert.Equal(t, uint64(ts.UnixMilli()), msg.TimestampMs,
			"event time should come from the record's sys_created_on")

		// The payload is shipped verbatim.
		assert.JSONEq(t, mustJSON(t, src), mustJSON(t, msg.JsonPayload),
			"shipped payload must match the original ServiceNow record")
	}
}

// TestMockNewRecordShippedOnce verifies a record inserted mid-run is shipped
// exactly once, and already-shipped records are never re-sent even though the
// inclusive checkpoint re-reads the boundary.
func TestMockNewRecordShippedOnce(t *testing.T) {
	mock := newMockServiceNow("u", "p")
	base := time.Now().UTC().Add(-time.Hour)
	mock.setTable("sys_audit", []utils.Dict{
		realisticSysAudit(1, snTime(base)),
		realisticSysAudit(2, snTime(base.Add(1*time.Second))),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		PollInterval:  30 * time.Millisecond,
	}
	adapter, _, err := newServiceNowAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 2 },
		5*time.Second, 20*time.Millisecond)

	// A new audit row is inserted, newer than everything shipped so far.
	mock.appendRecord("sys_audit", realisticSysAudit(3, snTime(time.Now().UTC())))

	require.Eventually(t, func() bool { return sink.count() == 3 },
		5*time.Second, 20*time.Millisecond, "the new record should ship")
	require.Never(t, func() bool { return sink.count() > 3 },
		300*time.Millisecond, 30*time.Millisecond)

	shippedPerID := map[string]int{}
	for _, msg := range sink.snapshot() {
		shippedPerID[msg.JsonPayload["sys_id"].(string)]++
	}
	for id, n := range shippedPerID {
		assert.Equal(t, 1, n, "record %s must ship exactly once", id)
	}
	assert.Len(t, shippedPerID, 3)
}

// TestMockPaginationFullDataset verifies a dataset larger than one page is
// walked completely -- following the Link rel="next" header -- and every
// record is shipped exactly once.
func TestMockPaginationFullDataset(t *testing.T) {
	const total = 250

	mock := newMockServiceNow("u", "p")
	base := time.Now().UTC().Add(-time.Hour)
	records := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		records[i] = realisticSysAudit(i, snTime(base.Add(time.Duration(i)*time.Second)))
	}
	mock.setTable("sys_audit", records)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		PageSize:      100, // 250 records => 3 pages
		PollInterval:  50 * time.Millisecond,
	}
	adapter, _, err := newServiceNowAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == total },
		10*time.Second, 25*time.Millisecond, "all paginated records should ship")
	require.Never(t, func() bool { return sink.count() != total },
		300*time.Millisecond, 30*time.Millisecond)

	ids := map[string]bool{}
	for _, msg := range sink.snapshot() {
		ids[msg.JsonPayload["sys_id"].(string)] = true
	}
	assert.Len(t, ids, total, "every distinct record should be shipped once")
}

// TestMockACLShortPageStillWalks reproduces the Table API's documented
// behaviour of applying sysparm_limit *before* ACL evaluation: the first page
// comes back shorter than the page size (ACL-hidden rows removed) while the
// Link header still advertises a next page. The adapter must keep walking and
// ship every visible record.
func TestMockACLShortPageStillWalks(t *testing.T) {
	const total = 20

	mock := newMockServiceNow("u", "p")
	base := time.Now().UTC().Add(-time.Hour)
	records := make([]utils.Dict, total)
	for i := 0; i < total; i++ {
		records[i] = realisticSysAudit(i, snTime(base.Add(time.Duration(i)*time.Second)))
	}
	mock.setTable("sys_audit", records)
	// Hide most of page 1 (records 0..9): it comes back with 2 visible rows
	// -- far short of the page size -- but more data exists on page 2.
	for i := 0; i < 8; i++ {
		mock.hideFromACL(records[i].FindOneString("sys_id"))
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		PageSize:      10, // 20 records => 2 pages
		PollInterval:  40 * time.Millisecond,
	}
	adapter, _, err := newServiceNowAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	// 20 records minus 8 ACL-hidden ones = 12 visible records, 10 of which
	// sit on page 2 -- only reachable if a short page does not end the walk.
	require.Eventually(t, func() bool { return sink.count() == 12 },
		5*time.Second, 20*time.Millisecond,
		"a short (ACL-filtered) page must not end the walk while Link advertises more")
	require.Never(t, func() bool { return sink.count() != 12 },
		300*time.Millisecond, 30*time.Millisecond)
}

// TestMockMultipleFeeds verifies that several feeds collect concurrently,
// each event is tagged with its feed's name, and a feed's encoded-query
// filter is applied.
func TestMockMultipleFeeds(t *testing.T) {
	mock := newMockServiceNow("u", "p")
	base := time.Now().UTC().Add(-time.Hour)
	mock.setTable("sys_audit", []utils.Dict{
		realisticSysAudit(1, snTime(base)),
		realisticSysAudit(2, snTime(base.Add(1*time.Second))),
	})
	mock.setTable("sysevent", []utils.Dict{
		realisticSysEvent(1, "login", snTime(base)),
		realisticSysEvent(2, "login.failed", snTime(base.Add(1*time.Second))),
		realisticSysEvent(3, "login", snTime(base.Add(2*time.Second))),
		// Filtered out by the feed's query: not a login event.
		realisticSysEvent(4, "metric.update", snTime(base.Add(3*time.Second))),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		PollInterval:  40 * time.Millisecond,
		Feeds: []ServiceNowFeed{
			{
				Name:  "sys_audit",
				Table: "sys_audit",
			},
			{
				Name:  "login_events",
				Table: "sysevent",
				Query: "name=login",
			},
			{
				Name:  "failed_logins",
				Table: "sysevent",
				Query: "name=login.failed",
			},
		},
	}
	adapter, _, err := newServiceNowAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return sink.count() == 5 },
		5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return sink.count() != 5 },
		300*time.Millisecond, 30*time.Millisecond)

	byType := map[string]int{}
	for _, msg := range sink.snapshot() {
		byType[msg.EventType]++
	}
	assert.Equal(t, map[string]int{"sys_audit": 2, "login_events": 2, "failed_logins": 1}, byType)
}

// TestMockRejectsBadCredentials verifies the adapter stops, and ships
// nothing, when the mock API rejects the supplied credentials.
func TestMockRejectsBadCredentials(t *testing.T) {
	mock := newMockServiceNow("u", "correct-password")
	mock.setTable("sys_audit", []utils.Dict{
		realisticSysAudit(1, snTime(time.Now().UTC())),
	})

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	sink := &captureSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "wrong-password",
		BaseURL:       server.URL,
		PollInterval:  50 * time.Millisecond,
	}
	adapter, chStopped, err := newServiceNowAdapter(ctx, conf, sink)
	require.NoError(t, err)
	defer adapter.Close()

	select {
	case <-chStopped:
		// Expected: rejected credentials stop the adapter.
	case <-time.After(3 * time.Second):
		t.Fatal("adapter should stop when the API rejects the credentials")
	}
	assert.Equal(t, 0, sink.count(), "nothing should ship when authentication fails")
	assert.GreaterOrEqual(t, mock.requestCount(), 1, "the adapter should have called the API")
}
