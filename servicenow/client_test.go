package usp_servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClientOptions returns ClientOptions wired for a sink (no real
// LimaCharlie connection) with the logging callbacks pointed at the test log.
func testClientOptions(t *testing.T) uspclient.ClientOptions {
	t.Helper()
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:     "json",
		TestSinkMode: true,
		DebugLog:     func(msg string) { t.Logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { t.Logf("WRN: %s", msg) },
		OnError:      func(err error) { t.Logf("ERR: %v", err) },
	}
}

// auditFeed is the single sys_audit feed used by integration tests.
func auditFeed() []ServiceNowFeed {
	return []ServiceNowFeed{{
		Name:  "sys_audit",
		Table: "sys_audit",
	}}
}

// auditItem builds a minimal sys_audit-shaped record.
func auditItem(id, createdOn string) map[string]interface{} {
	return map[string]interface{}{
		"sys_id":         id,
		"tablename":      "incident",
		"fieldname":      "state",
		"oldvalue":       "1",
		"newvalue":       "2",
		"sys_created_on": createdOn,
		"sys_created_by": "jane.doe",
	}
}

func resultEnvelope(items []map[string]interface{}) map[string]interface{} {
	if items == nil {
		items = []map[string]interface{}{}
	}
	return map[string]interface{}{"result": items}
}

// --- unit tests -------------------------------------------------------------

func TestExtractResult(t *testing.T) {
	t.Run("result envelope", func(t *testing.T) {
		items, err := extractResult([]byte(`{"result":[{"sys_id":"a"},{"sys_id":"b"}]}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("sys_id"))
		assert.Equal(t, "b", items[1].FindOneString("sys_id"))
	})

	t.Run("empty result array", func(t *testing.T) {
		items, err := extractResult([]byte(`{"result":[]}`))
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("missing result key errors", func(t *testing.T) {
		_, err := extractResult([]byte(`{"rows":[]}`))
		assert.Error(t, err)
	})

	t.Run("null result is an empty page", func(t *testing.T) {
		items, err := extractResult([]byte(`{"result": null}`))
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("result substring in a value does not mask a missing key", func(t *testing.T) {
		_, err := extractResult([]byte(`{"error":{"message":"no result for query","detail":"\"result\""},"status":"failure"}`))
		assert.Error(t, err)
	})

	t.Run("nested result key is not the envelope", func(t *testing.T) {
		_, err := extractResult([]byte(`{"data":{"result":[{"sys_id":"a"}]}}`))
		assert.Error(t, err)
	})

	t.Run("non-array result errors", func(t *testing.T) {
		_, err := extractResult([]byte(`{"result":{"sys_id":"a"}}`))
		assert.Error(t, err)
	})

	t.Run("empty body yields no items", func(t *testing.T) {
		items, err := extractResult([]byte("   "))
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("non-json errors", func(t *testing.T) {
		_, err := extractResult([]byte(`not json`))
		assert.Error(t, err)
	})

	t.Run("large integers keep precision", func(t *testing.T) {
		items, err := extractResult([]byte(`{"result":[{"big":123456789012345678}]}`))
		require.NoError(t, err)
		require.Len(t, items, 1)
		v, ok := items[0].GetInt("big")
		require.True(t, ok)
		assert.Equal(t, uint64(123456789012345678), v)
	})

	t.Run("non-object records are skipped, valid ones kept", func(t *testing.T) {
		items, err := extractResult([]byte(`{"result":[{"sys_id":"a"},"junk",123,null,{"sys_id":"b"}]}`))
		require.NoError(t, err)
		require.Len(t, items, 2)
		assert.Equal(t, "a", items[0].FindOneString("sys_id"))
		assert.Equal(t, "b", items[1].FindOneString("sys_id"))
	})
}

func TestRecordID(t *testing.T) {
	t.Run("uses configured id field", func(t *testing.T) {
		feed := ServiceNowFeed{IDField: "sys_id"}
		assert.Equal(t, "abc", recordID(feed, utils.Dict{"sys_id": "abc", "documentkey": "other"}))
	})

	t.Run("content hash fallback is stable and distinct", func(t *testing.T) {
		feed := ServiceNowFeed{IDField: "sys_id"}
		a1 := recordID(feed, utils.Dict{"foo": "bar"})
		a2 := recordID(feed, utils.Dict{"foo": "bar"})
		b := recordID(feed, utils.Dict{"foo": "baz"})
		assert.Equal(t, a1, a2)
		assert.NotEqual(t, a1, b)
		assert.Contains(t, a1, "sha256:")
	})
}

func TestResolveBaseURL(t *testing.T) {
	assert.Equal(t, "https://example.service-now.com",
		resolveBaseURL(ServiceNowConfig{Instance: "example"}))
	assert.Equal(t, "https://custom.example.com",
		resolveBaseURL(ServiceNowConfig{BaseURL: "https://custom.example.com/"}))
	// base_url wins over instance.
	assert.Equal(t, "https://custom.example.com",
		resolveBaseURL(ServiceNowConfig{Instance: "example", BaseURL: "https://custom.example.com"}))
}

func TestBuildParams(t *testing.T) {
	a := &ServiceNowAdapter{conf: ServiceNowConfig{PageSize: 50}}
	checkpoint := time.Date(2026, 6, 11, 9, 14, 33, 0, time.UTC)

	t.Run("incremental filter, order and pagination", func(t *testing.T) {
		feed := ServiceNowFeed{Table: "sys_audit", TimestampField: "sys_created_on"}
		params := a.buildParams(feed, checkpoint, 100)
		assert.Equal(t, "sys_created_on>=2026-06-11 09:14:33^ORDERBYsys_created_on^ORDERBYsys_id",
			params.Get("sysparm_query"))
		assert.Equal(t, "50", params.Get("sysparm_limit"))
		assert.Equal(t, "100", params.Get("sysparm_offset"))
		assert.Equal(t, "false", params.Get("sysparm_display_value"),
			"database (UTC) values must be requested, not display values")
		assert.Equal(t, "true", params.Get("sysparm_exclude_reference_link"))
		assert.Empty(t, params.Get("sysparm_fields"))
	})

	t.Run("feed query is ANDed in front of the time filter", func(t *testing.T) {
		feed := ServiceNowFeed{
			Table:          "sysevent",
			Query:          "name=login",
			TimestampField: "sys_created_on",
		}
		params := a.buildParams(feed, checkpoint, 0)
		assert.Equal(t, "name=login^sys_created_on>=2026-06-11 09:14:33^ORDERBYsys_created_on^ORDERBYsys_id",
			params.Get("sysparm_query"))
	})

	t.Run("id field is the pagination tiebreaker", func(t *testing.T) {
		feed := ServiceNowFeed{Table: "sysevent", TimestampField: "sys_created_on", IDField: "uniq"}
		params := a.buildParams(feed, checkpoint, 0)
		assert.True(t, strings.HasSuffix(params.Get("sysparm_query"), "^ORDERBYsys_created_on^ORDERBYuniq"),
			"got %q", params.Get("sysparm_query"))
	})

	t.Run("fields restriction is forwarded", func(t *testing.T) {
		feed := ServiceNowFeed{
			Table:          "sys_audit",
			Fields:         "sys_id,sys_created_on,fieldname",
			TimestampField: "sys_created_on",
		}
		params := a.buildParams(feed, checkpoint, 0)
		assert.Equal(t, "sys_id,sys_created_on,fieldname", params.Get("sysparm_fields"))
	})
}

func TestParseTimestamp(t *testing.T) {
	t.Run("table api format is parsed as UTC", func(t *testing.T) {
		ts, ok := parseTimestamp("2026-06-11 09:14:33")
		require.True(t, ok)
		assert.Equal(t, time.Date(2026, 6, 11, 9, 14, 33, 0, time.UTC), ts)
	})

	for _, c := range []string{
		"2026-06-11T09:14:33Z",
		"2026-06-11T09:14:33.123456Z",
	} {
		_, ok := parseTimestamp(c)
		assert.True(t, ok, "expected %q to parse", c)
	}
	_, ok := parseTimestamp("not a date")
	assert.False(t, ok)
}

func TestHasNextLink(t *testing.T) {
	h := http.Header{}
	assert.False(t, hasNextLink(h))

	h.Set("Link", `<https://x.service-now.com/api/now/v2/table/sys_audit?sysparm_offset=0&sysparm_limit=100>;rel="first"`)
	assert.False(t, hasNextLink(h))

	h.Set("Link", `<https://x.service-now.com/api/now/v2/table/sys_audit?sysparm_offset=100&sysparm_limit=100>;rel="next",<https://x.service-now.com/api/now/v2/table/sys_audit?sysparm_offset=400&sysparm_limit=100>;rel="last"`)
	assert.True(t, hasNextLink(h))

	h = http.Header{}
	h.Add("Link", `<https://x/api?sysparm_offset=0>;rel="prev"`)
	h.Add("Link", `<https://x/api?sysparm_offset=200>;rel="next"`)
	assert.True(t, hasNextLink(h))
}

func TestParseRetryAfter(t *testing.T) {
	assert.Equal(t, 30*time.Second, parseRetryAfter("30"))
	assert.Equal(t, time.Duration(0), parseRetryAfter(""))
	assert.Equal(t, time.Duration(0), parseRetryAfter("soon"))
	assert.Equal(t, time.Duration(0), parseRetryAfter("-5"))
}

func TestIsTransientError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		isTransient bool
	}{
		{"500", &HTTPError{StatusCode: 500}, true},
		{"503", &HTTPError{StatusCode: 503}, true},
		{"429", &HTTPError{StatusCode: 429}, true},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"403", &HTTPError{StatusCode: 403}, false},
		{"404", &HTTPError{StatusCode: 404}, false},
		{"400", &HTTPError{StatusCode: 400}, false},
		{"network", fmt.Errorf("failed to execute request %q: connection refused", "x"), true},
		{"context canceled", context.Canceled, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.isTransient, isTransientError(c.err))
		})
	}
}

func TestValidate(t *testing.T) {
	t.Run("requires username", func(t *testing.T) {
		c := ServiceNowConfig{ClientOptions: testClientOptions(t), Password: "p", Instance: "example"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires password", func(t *testing.T) {
		c := ServiceNowConfig{ClientOptions: testClientOptions(t), Username: "u", Instance: "example"}
		assert.Error(t, c.Validate())
	})

	t.Run("requires instance or base_url", func(t *testing.T) {
		c := ServiceNowConfig{ClientOptions: testClientOptions(t), Username: "u", Password: "p"}
		assert.Error(t, c.Validate())
	})

	t.Run("applies defaults and the default feed set", func(t *testing.T) {
		c := ServiceNowConfig{ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example"}
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultPageSize, c.PageSize)
		assert.Equal(t, defaultPollInterval, c.PollInterval)
		assert.Equal(t, defaultBackfill, c.Backfill)
		assert.Equal(t, defaultCheckpointLag, c.CheckpointLag)

		require.Len(t, c.Feeds, 1)
		f := c.Feeds[0]
		assert.Equal(t, "sys_audit", f.Name)
		assert.Equal(t, "sys_audit", f.Table)
		assert.Equal(t, defaultTimestampField, f.TimestampField)
		assert.Equal(t, defaultIDField, f.IDField)
		assert.Equal(t, defaultMaxPages, f.MaxPages)
	})

	t.Run("page size is capped at the platform maximum", func(t *testing.T) {
		c := ServiceNowConfig{ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example", PageSize: 50000}
		require.NoError(t, c.Validate())
		assert.Equal(t, maxPageSize, c.PageSize)
	})

	t.Run("feed name defaults to its table", func(t *testing.T) {
		c := ServiceNowConfig{
			ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example",
			Feeds: []ServiceNowFeed{{Table: "syslog_transaction"}},
		}
		require.NoError(t, c.Validate())
		assert.Equal(t, "syslog_transaction", c.Feeds[0].Name)
	})

	t.Run("rejects feed without table", func(t *testing.T) {
		c := ServiceNowConfig{
			ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example",
			Feeds: []ServiceNowFeed{{Name: "x"}},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects a fields list missing the timestamp or id column", func(t *testing.T) {
		for _, fields := range []string{
			"tablename,fieldname,newvalue", // missing both
			"sys_id,tablename",             // missing timestamp
			"sys_created_on,tablename",     // missing id
		} {
			c := ServiceNowConfig{
				ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example",
				Feeds: []ServiceNowFeed{{Table: "sys_audit", Fields: fields}},
			}
			assert.Error(t, c.Validate(), "fields=%q must be rejected", fields)
		}
	})

	t.Run("accepts a fields list carrying the timestamp and id columns", func(t *testing.T) {
		c := ServiceNowConfig{
			ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example",
			Feeds: []ServiceNowFeed{{Table: "sys_audit", Fields: "sys_id, sys_created_on, tablename"}},
		}
		assert.NoError(t, c.Validate())
	})

	t.Run("rejects duplicate feed names", func(t *testing.T) {
		c := ServiceNowConfig{
			ClientOptions: testClientOptions(t), Username: "u", Password: "p", Instance: "example",
			Feeds: []ServiceNowFeed{
				{Name: "x", Table: "sys_audit"},
				{Name: "x", Table: "sysevent"},
			},
		}
		assert.Error(t, c.Validate())
	})
}

// --- integration tests ------------------------------------------------------

// recordedRequest captures what the adapter sent on one API call.
type recordedRequest struct {
	path   string
	query  url.Values
	user   string
	pass   string
	method string
}

// recordingServer replies with a fixed dataset and records every request.
type recordingServer struct {
	mu       sync.Mutex
	requests []recordedRequest
	items    []map[string]interface{}
}

func (s *recordingServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		s.mu.Lock()
		s.requests = append(s.requests, recordedRequest{
			path:   r.URL.Path,
			query:  r.URL.Query(),
			user:   user,
			pass:   pass,
			method: r.Method,
		})
		items := s.items
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resultEnvelope(items))
	}
}

func (s *recordingServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *recordingServer) request(i int) recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[i]
}

// TestRequestShape verifies the adapter hits the v2 Table API endpoint with
// Basic auth and the expected query parameters.
func TestRequestShape(t *testing.T) {
	srv := &recordingServer{items: []map[string]interface{}{
		auditItem("a", "2026-06-11 09:14:33"),
	}}
	server := httptest.NewServer(srv.handler())
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "lc.collector",
		Password:      "s3cret",
		BaseURL:       server.URL,
		PollInterval:  200 * time.Millisecond,
		Feeds:         auditFeed(),
	}
	adapter, chStopped, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return srv.count() >= 1 },
		3*time.Second, 20*time.Millisecond, "adapter never called the API")

	select {
	case <-chStopped:
		t.Fatal("adapter stopped unexpectedly")
	default:
	}

	req := srv.request(0)
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/api/now/v2/table/sys_audit", req.path)
	assert.Equal(t, "lc.collector", req.user)
	assert.Equal(t, "s3cret", req.pass)
	assert.Equal(t, strconv.Itoa(defaultPageSize), req.query.Get("sysparm_limit"))
	assert.Equal(t, "0", req.query.Get("sysparm_offset"))
	assert.Equal(t, "false", req.query.Get("sysparm_display_value"))
	assert.Equal(t, "true", req.query.Get("sysparm_exclude_reference_link"))

	q := req.query.Get("sysparm_query")
	assert.Contains(t, q, "sys_created_on>=")
	assert.True(t, strings.HasSuffix(q, "^ORDERBYsys_created_on^ORDERBYsys_id"),
		"records must be requested oldest-first with an id tiebreaker, got %q", q)
}

// queryCheckpoint extracts and parses the checkpoint timestamp from a
// captured sysparm_query.
func queryCheckpoint(t *testing.T, q string) time.Time {
	t.Helper()
	start := strings.Index(q, "sys_created_on>=")
	require.GreaterOrEqual(t, start, 0, "no checkpoint filter in %q", q)
	rest := q[start+len("sys_created_on>="):]
	end := strings.Index(rest, "^")
	require.Greater(t, end, 0, "unterminated checkpoint filter in %q", q)
	ts, err := time.Parse(serviceNowTimeLayout, rest[:end])
	require.NoError(t, err, "unparseable checkpoint in %q", q)
	return ts
}

// TestCheckpointAdvances verifies a completed poll advances the incremental
// filter to now-CheckpointLag: past every record already collected (so an
// idle feed stops re-reading its boundary forever) but no further than the
// lag allows (so late-visible records are still caught).
func TestCheckpointAdvances(t *testing.T) {
	const lag = 30 * time.Minute

	testStart := time.Now().UTC()
	base := testStart.Add(-time.Hour).Truncate(time.Second)
	newest := base.Add(2 * time.Second)
	srv := &recordingServer{items: []map[string]interface{}{
		auditItem("a", base.Format(serviceNowTimeLayout)),
		auditItem("b", newest.Format(serviceNowTimeLayout)),
	}}
	server := httptest.NewServer(srv.handler())
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		CheckpointLag: lag,
		PollInterval:  40 * time.Millisecond,
		Feeds:         auditFeed(),
	}
	adapter, _, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return srv.count() >= 3 },
		5*time.Second, 20*time.Millisecond)
	require.NoError(t, adapter.Close())

	// Poll 1 starts from the backfill checkpoint.
	cp0 := queryCheckpoint(t, srv.request(0).query.Get("sysparm_query"))
	assert.True(t, cp0.Before(base), "poll 1 must start from the backfill checkpoint, got %v", cp0)

	// Later polls run from an advanced checkpoint: past the records already
	// collected (records are an hour old, far older than the lag), but
	// trailing the clock by at least CheckpointLag.
	for i := 1; i < 3; i++ {
		cp := queryCheckpoint(t, srv.request(i).query.Get("sysparm_query"))
		assert.True(t, cp.After(newest),
			"poll %d should have advanced past the collected records, got %v", i, cp)
		assert.False(t, cp.After(time.Now().UTC().Add(-lag).Add(2*time.Second)),
			"poll %d checkpoint %v must trail the clock by ~CheckpointLag", i, cp)
	}
}

// TestMaxPagesCapResumes verifies a poll stops at max_pages and that the next
// poll resumes from the advanced checkpoint instead of starting over.
func TestMaxPagesCapResumes(t *testing.T) {
	const pageSize = 2

	var mu sync.Mutex
	var requests []url.Values
	// Records are an hour old: newer than the 2h backfill checkpoint but
	// older than now-CheckpointLag, so a capped poll resumes from the newest
	// record processed (not from the lagged clock), on any day this test runs.
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.Query())
		n := len(requests)
		mu.Unlock()

		// Always serve a full page of ever-newer records and advertise more,
		// so only max_pages can end a poll's walk.
		items := []map[string]interface{}{
			auditItem(fmt.Sprintf("rec-%d-1", n), base.Add(time.Duration(2*n)*time.Second).Format(serviceNowTimeLayout)),
			auditItem(fmt.Sprintf("rec-%d-2", n), base.Add(time.Duration(2*n+1)*time.Second).Format(serviceNowTimeLayout)),
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s>;rel="next"`, r.URL.String()))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resultEnvelope(items))
	}))
	defer server.Close()

	var maxPagesWarned atomic.Bool
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) {
		t.Logf("WRN: %s", msg)
		if strings.Contains(msg, "max_pages") {
			maxPagesWarned.Store(true)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: opts,
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		Backfill:      2 * time.Hour,
		CheckpointLag: 1 * time.Minute,
		PageSize:      pageSize,
		PollInterval:  60 * time.Millisecond,
		Feeds: []ServiceNowFeed{
			{Name: "capped", Table: "sys_audit", MaxPages: 3},
		},
	}
	adapter, _, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, maxPagesWarned.Load, 5*time.Second, 20*time.Millisecond,
		"adapter should warn when max_pages is reached")
	// Wait for the second poll to start.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(requests) >= 4
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, adapter.Close())

	mu.Lock()
	defer mu.Unlock()

	// Poll 1 is exactly 3 pages (max_pages) at offsets 0, 2, 4.
	for i := 0; i < 3; i++ {
		assert.Equal(t, strconv.Itoa(i*pageSize), requests[i].Get("sysparm_offset"),
			"request %d offset", i)
	}

	// Poll 2 restarts pagination but from an advanced checkpoint: the newest
	// timestamp served during poll 1 (request 3 carried base+7s).
	assert.Equal(t, "0", requests[3].Get("sysparm_offset"), "a new poll restarts offsets")
	wantCheckpoint := base.Add(7 * time.Second).Format(serviceNowTimeLayout)
	assert.Equal(t,
		fmt.Sprintf("sys_created_on>=%s^ORDERBYsys_created_on^ORDERBYsys_id", wantCheckpoint),
		requests[3].Get("sysparm_query"),
		"the capped poll must advance the checkpoint to the last record processed")
}

// TestMaxPagesStuckWarns verifies the distinct loud warning when a capped
// poll cannot advance the checkpoint at all (more than max_pages*page_size
// records share one timestamp), instead of the misleading "will be collected
// next poll" message.
func TestMaxPagesStuckWarns(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Full pages of records all older than the initial checkpoint (the
		// mock ignores the filter), so maxSeen never moves; Link always
		// advertises more.
		items := []map[string]interface{}{
			auditItem("a", "2000-01-01 00:00:01"),
			auditItem("b", "2000-01-01 00:00:01"),
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s>;rel="next"`, r.URL.String()))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resultEnvelope(items))
	}))
	defer server.Close()

	var stuckWarned atomic.Bool
	opts := testClientOptions(t)
	opts.OnWarning = func(msg string) {
		t.Logf("WRN: %s", msg)
		if strings.Contains(msg, "without being able to advance") {
			stuckWarned.Store(true)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: opts,
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		PageSize:      2,
		PollInterval:  1 * time.Hour, // only the initial poll runs during the test
		Feeds: []ServiceNowFeed{
			{Name: "stuck", Table: "sys_audit", MaxPages: 2},
		},
	}
	adapter, _, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, stuckWarned.Load, 5*time.Second, 20*time.Millisecond,
		"adapter should warn loudly when the checkpoint cannot advance at the cap")
}

// TestTransientErrorRetry verifies the adapter retries 5xx responses rather
// than terminating, and that the failed poll does not advance the checkpoint.
func TestTransientErrorRetry(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"message":"internal error","detail":null},"status":"failure"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":[]}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions:  testClientOptions(t),
		Username:       "u",
		Password:       "p",
		BaseURL:        server.URL,
		PollInterval:   100 * time.Millisecond,
		RetryBaseDelay: 20 * time.Millisecond,
		MaxRetryDelay:  40 * time.Millisecond,
		Feeds:          auditFeed(),
	}
	adapter, chStopped, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	time.Sleep(500 * time.Millisecond)

	select {
	case <-chStopped:
		t.Fatal("adapter stopped on a transient error - it should have retried")
	default:
	}
	assert.GreaterOrEqual(t, requestCount.Load(), int32(3), "expected retries then success")
	assert.False(t, adapter.doStop.IsSet())
}

// TestUnauthorizedStops verifies rejected credentials (HTTP 401) stop the
// whole adapter.
func TestUnauthorizedStops(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"User Not Authenticated","detail":null},"status":"failure"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "bad",
		BaseURL:       server.URL,
		PollInterval:  100 * time.Millisecond,
		Feeds:         auditFeed(),
	}
	adapter, chStopped, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	select {
	case <-chStopped:
		// Expected: rejected credentials terminate the adapter.
	case <-time.After(3 * time.Second):
		t.Fatal("adapter should have stopped on HTTP 401")
	}
	assert.True(t, adapter.doStop.IsSet())
}

// TestForbiddenKeepsAdapterAlive verifies a 403 -- ServiceNow's per-table ACL
// denial -- does not stop the adapter: the feed ships nothing but keeps
// retrying on the poll interval (the ACL may be fixed live, and other feeds
// must not be killed by one table's missing role).
func TestForbiddenKeepsAdapterAlive(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"Insufficient rights to query records","detail":null},"status":"failure"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions: testClientOptions(t),
		Username:      "u",
		Password:      "p",
		BaseURL:       server.URL,
		PollInterval:  50 * time.Millisecond,
		Feeds:         auditFeed(),
	}
	adapter, chStopped, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	// The feed must keep polling across intervals (one request per poll: the
	// 403 is permanent, not retried within a poll).
	require.Eventually(t, func() bool { return requestCount.Load() >= 3 },
		5*time.Second, 20*time.Millisecond, "the feed should keep retrying every poll_interval")

	select {
	case <-chStopped:
		t.Fatal("a per-table 403 must not stop the adapter")
	default:
	}
	assert.False(t, adapter.doStop.IsSet())
}

// TestRetryAfterIsHonored verifies a 429's Retry-After delay is respected
// (the second attempt does not fire before the requested delay elapses).
func TestRetryAfterIsHonored(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"Rate limit exceeded","detail":null},"status":"failure"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":[]}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conf := ServiceNowConfig{
		ClientOptions:  testClientOptions(t),
		Username:       "u",
		Password:       "p",
		BaseURL:        server.URL,
		PollInterval:   1 * time.Hour, // only the initial poll runs during the test
		RetryBaseDelay: 10 * time.Millisecond,
		MaxRetryDelay:  20 * time.Millisecond,
		Feeds:          auditFeed(),
	}
	adapter, _, err := NewServiceNowAdapter(ctx, conf)
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(times) >= 2
	}, 5*time.Second, 20*time.Millisecond, "expected a retry after the 429")

	mu.Lock()
	defer mu.Unlock()
	gap := times[1].Sub(times[0])
	assert.GreaterOrEqual(t, gap, 1*time.Second,
		"the retry must wait at least the Retry-After delay, waited %v", gap)
}
