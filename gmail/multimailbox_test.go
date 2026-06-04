package usp_gmail

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises multi-mailbox collection: the service-account flow with a
// static list of subjects, Admin SDK Directory API auto-discovery (including
// dynamic add/remove), per-mailbox sensor identity, and the validation rules
// that gate the new modes.

// --- mock Directory API -----------------------------------------------------

// directoryUsersHandler serves the Admin SDK users.list endpoint, paginating at
// directoryPageSize and honoring directoryStatus.
func (m *mockGmail) directoryUsersHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.authorize(w, r) {
			return
		}
		m.mu.Lock()
		m.directoryRequests++
		status := m.directoryStatus
		users := append([]discoveredUser(nil), m.directoryUsers...)
		pageSize := m.directoryPageSize
		m.mu.Unlock()

		if status != 0 {
			writeJSON(w, status, capErrorBody(status))
			return
		}

		offset := 0
		if pt := r.URL.Query().Get("pageToken"); pt != "" {
			offset, _ = strconv.Atoi(pt)
		}
		if offset > len(users) {
			offset = len(users)
		}
		end := len(users)
		if pageSize > 0 && offset+pageSize < end {
			end = offset + pageSize
		}
		resp := map[string]any{"users": users[offset:end]}
		if end < len(users) {
			resp["nextPageToken"] = strconv.Itoa(end)
		}
		writeJSON(w, http.StatusOK, mustJSONString(resp))
	}
}

func (m *mockGmail) setDirectoryUsers(u ...discoveredUser) {
	m.mu.Lock()
	m.directoryUsers = u
	m.mu.Unlock()
}

func activeUser(email string) discoveredUser { return discoveredUser{PrimaryEmail: email} }
func suspendedUser(email string) discoveredUser {
	return discoveredUser{PrimaryEmail: email, Suspended: true}
}

// --- per-mailbox capturing sink hub -----------------------------------------

// sinkHub hands each mailbox its own captureSink and records the ClientOptions
// (sensor identity) the adapter derived for it.
type sinkHub struct {
	mu    sync.Mutex
	sinks map[string]*captureSink
	opts  map[string]uspclient.ClientOptions
}

func newSinkHub() *sinkHub {
	return &sinkHub{sinks: map[string]*captureSink{}, opts: map[string]uspclient.ClientOptions{}}
}

func (h *sinkHub) factory() sinkFactory {
	return func(_ context.Context, opts uspclient.ClientOptions, mailbox string) (uspSink, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		s := h.sinks[mailbox]
		if s == nil {
			s = &captureSink{}
			h.sinks[mailbox] = s
		}
		h.opts[mailbox] = opts
		return s, nil
	}
}

func (h *sinkHub) sink(mailbox string) *captureSink {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sinks[mailbox]
}

func (h *sinkHub) options(mailbox string) (uspclient.ClientOptions, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o, ok := h.opts[mailbox]
	return o, ok
}

func (h *sinkHub) countByType(mailbox, evtType string) int {
	s := h.sink(mailbox)
	if s == nil {
		return 0
	}
	return countByType(s, evtType)
}

// saConfig is a service-account config pointed at the mock, with fast cadences.
func saConfig(t *testing.T, baseURL, saJSON string) GmailConfig {
	t.Helper()
	return GmailConfig{
		ClientOptions:             testClientOptions(t),
		ServiceAccountCredentials: saJSON,
		BaseURL:                   baseURL,
		TokenURL:                  baseURL + "/token",
		DirectoryBaseURL:          baseURL,
		InitialLookback:           24 * time.Hour,
		PollInterval:              40 * time.Millisecond,
		DiscoveryInterval:         40 * time.Millisecond,
	}
}

// --- tests ------------------------------------------------------------------

// TestStaticSubjectsFanOut verifies that a static list of subjects produces one
// collector and one sensor per mailbox, each impersonating its own subject and
// each shipping the mailbox's messages to its own sensor with a per-mailbox
// sensor identity.
func TestStaticSubjectsFanOut(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("m1", "t1", recentMs(30*time.Minute), "alice@partner.test", "Invoice"))
	mock.addMessage(realisticMessage("m2", "t2", recentMs(20*time.Minute), "bob@partner.test", "Wire"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	subjects := []string{"ceo@corp.test", "cfo@corp.test", "it@corp.test"}
	conf := saConfig(t, server.URL, saJSON)
	conf.Subjects = subjects
	conf.ClientOptions.SensorSeedKey = "corp"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	// Each mailbox ships both messages to its own sensor.
	for _, s := range subjects {
		s := s
		require.Eventually(t, func() bool { return hub.countByType(s, eventTypeMessage) == 2 },
			5*time.Second, 20*time.Millisecond, "mailbox %s should ship both messages", s)
	}
	require.Never(t, func() bool {
		for _, s := range subjects {
			if hub.countByType(s, eventTypeMessage) != 2 {
				return true
			}
		}
		return false
	}, 300*time.Millisecond, 30*time.Millisecond, "no mailbox should re-ship")

	// Each mailbox got its own sensor identity derived from its address.
	for _, s := range subjects {
		opts, ok := hub.options(s)
		require.True(t, ok, "expected a sensor for %s", s)
		assert.Equal(t, "corp/"+s, opts.SensorSeedKey, "sensor seed key must be per-mailbox")
		assert.Equal(t, s, opts.Hostname, "sensor hostname must be the mailbox address")
	}

	// Every subject was impersonated via its own delegated assertion.
	mock.mu.Lock()
	defer mock.mu.Unlock()
	for _, s := range subjects {
		assert.GreaterOrEqual(t, mock.impersonatedSubjects[s], 1, "subject %s must have been impersonated", s)
	}
}

// TestSingleSubjectKeepsConfiguredSensor verifies that a single explicit mailbox
// (not multi-mailbox) keeps the operator's configured sensor identity verbatim,
// rather than deriving a per-mailbox one.
func TestSingleSubjectKeepsConfiguredSensor(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "alice@partner.test", "Hi"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.Subject = "solo@corp.test"
	conf.ClientOptions.SensorSeedKey = "configured-seed"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return hub.countByType("solo@corp.test", eventTypeMessage) == 1 },
		5*time.Second, 20*time.Millisecond)

	opts, ok := hub.options("solo@corp.test")
	require.True(t, ok)
	assert.Equal(t, "configured-seed", opts.SensorSeedKey, "a single mailbox keeps the configured seed key")
	assert.Equal(t, "", opts.Hostname, "a single mailbox does not get a derived hostname")
}

// TestDiscoveryEnumeratesAndCollects verifies that discovery enumerates the
// domain's mailboxes, starts a collector per active mailbox (skipping suspended
// accounts), and impersonates the admin for the Directory call.
func TestDiscoveryEnumeratesAndCollects(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "x@partner.test", "Hello"))
	mock.setDirectoryUsers(
		activeUser("ann@corp.test"),
		activeUser("ben@corp.test"),
		suspendedUser("gone@corp.test"),
	)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.DiscoverMailboxes = true
	conf.AdminSubject = "admin@corp.test"
	conf.ClientOptions.SensorSeedKey = "corp"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		return hub.countByType("ann@corp.test", eventTypeMessage) == 1 &&
			hub.countByType("ben@corp.test", eventTypeMessage) == 1
	}, 5*time.Second, 20*time.Millisecond, "both active mailboxes should be discovered and collected")

	// The suspended account must never get a collector.
	require.Never(t, func() bool {
		active := adapter.activeMailboxes()
		return active["gone@corp.test"]
	}, 400*time.Millisecond, 40*time.Millisecond, "suspended mailbox must be skipped")

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.GreaterOrEqual(t, mock.impersonatedSubjects["admin@corp.test"], 1, "the Directory call must impersonate the admin")
	assert.GreaterOrEqual(t, mock.impersonatedSubjects["ann@corp.test"], 1)
	assert.GreaterOrEqual(t, mock.impersonatedSubjects["ben@corp.test"], 1)
	assert.Equal(t, 0, mock.impersonatedSubjects["gone@corp.test"], "the suspended mailbox must never be impersonated")
}

// TestDiscoveryDynamicAddRemove verifies that a mailbox provisioned after startup
// gets a collector on the next discovery pass, and a mailbox deprovisioned later
// has its collector stopped and torn down.
func TestDiscoveryDynamicAddRemove(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "x@partner.test", "Hello"))
	mock.setDirectoryUsers(activeUser("ann@corp.test"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.DiscoverMailboxes = true
	conf.AdminSubject = "admin@corp.test"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return adapter.activeMailboxes()["ann@corp.test"] },
		5*time.Second, 20*time.Millisecond, "the initial mailbox should be collected")

	// A new mailbox is provisioned.
	mock.setDirectoryUsers(activeUser("ann@corp.test"), activeUser("ben@corp.test"))
	require.Eventually(t, func() bool {
		return adapter.activeMailboxes()["ben@corp.test"] && hub.countByType("ben@corp.test", eventTypeMessage) == 1
	}, 5*time.Second, 20*time.Millisecond, "the newly-provisioned mailbox should be discovered and collected")

	// The new mailbox is deprovisioned.
	mock.setDirectoryUsers(activeUser("ann@corp.test"))
	require.Eventually(t, func() bool { return !adapter.activeMailboxes()["ben@corp.test"] },
		5*time.Second, 20*time.Millisecond, "the deprovisioned mailbox's collector should be torn down")

	// Ann keeps collecting throughout.
	assert.True(t, adapter.activeMailboxes()["ann@corp.test"], "the surviving mailbox must keep collecting")
}

// TestDiscoveryNeverRemovesStaticSubject verifies that a mailbox named explicitly
// in the config is collected and never removed, even when discovery returns an
// empty domain.
func TestDiscoveryNeverRemovesStaticSubject(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "x@partner.test", "Hello"))
	// Discovery returns no users at all.

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.Subject = "keep@corp.test"
	conf.DiscoverMailboxes = true
	conf.AdminSubject = "admin@corp.test"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return hub.countByType("keep@corp.test", eventTypeMessage) == 1 },
		5*time.Second, 20*time.Millisecond, "the static subject should be collected")
	require.Never(t, func() bool { return !adapter.activeMailboxes()["keep@corp.test"] },
		500*time.Millisecond, 40*time.Millisecond, "discovery must never remove a static subject")
}

// TestDiscoveryPagination verifies discovery walks every page of users.list.
func TestDiscoveryPagination(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.directoryPageSize = 2 // force three pages for five users
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "x@partner.test", "Hello"))
	users := []discoveredUser{}
	for i := 0; i < 5; i++ {
		users = append(users, activeUser("u"+strconv.Itoa(i)+"@corp.test"))
	}
	mock.setDirectoryUsers(users...)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.DiscoverMailboxes = true
	conf.AdminSubject = "admin@corp.test"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		active := adapter.activeMailboxes()
		for _, u := range users {
			if !active[u.PrimaryEmail] {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond, "all five paginated mailboxes should be discovered")
}

// TestDiscoveryFailureKeepsStaticMailboxes verifies that a failing Directory API
// (e.g. the admin lacks the directory scope) does not stop the adapter: the
// explicitly-configured mailboxes keep collecting.
func TestDiscoveryFailureKeepsStaticMailboxes(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.directoryStatus = http.StatusForbidden // discovery enumeration fails
	mock.addMessage(realisticMessage("m1", "t1", recentMs(10*time.Minute), "x@partner.test", "Hello"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.Subject = "keep@corp.test"
	conf.DiscoverMailboxes = true
	conf.AdminSubject = "admin@corp.test"

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, chStopped, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool { return hub.countByType("keep@corp.test", eventTypeMessage) == 1 },
		5*time.Second, 20*time.Millisecond, "the static subject collects despite discovery failing")

	select {
	case <-chStopped:
		t.Fatal("a Directory API failure must not stop the adapter")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestPerMailboxDedupeIsolation verifies that two mailboxes holding an item with
// identical content (here, the same message id served to both) each ship it --
// one mailbox's dedupe state must not suppress another's.
func TestPerMailboxDedupeIsolation(t *testing.T) {
	saJSON, pub := generateServiceAccount(t)

	mock := newMockGmail()
	mock.saPublicKey = pub
	mock.addMessage(realisticMessage("shared", "t1", recentMs(10*time.Minute), "a@partner.test", "Same"))

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	conf := saConfig(t, server.URL, saJSON)
	conf.Subjects = []string{"a@corp.test", "b@corp.test"}
	// Share a single deduper across mailboxes to prove keys are mailbox-namespaced.
	dd, err := utils.NewLocalDeduper(time.Hour, 24*time.Hour)
	require.NoError(t, err)
	defer dd.Close()
	conf.Deduper = dd

	hub := newSinkHub()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter, _, err := newGmailAdapter(ctx, conf, hub.factory())
	require.NoError(t, err)
	defer adapter.Close()

	require.Eventually(t, func() bool {
		return hub.countByType("a@corp.test", eventTypeMessage) == 1 &&
			hub.countByType("b@corp.test", eventTypeMessage) == 1
	}, 5*time.Second, 20*time.Millisecond, "both mailboxes must ship the identical message to their own sensor")
	require.Never(t, func() bool {
		return hub.countByType("a@corp.test", eventTypeMessage) > 1 ||
			hub.countByType("b@corp.test", eventTypeMessage) > 1
	}, 300*time.Millisecond, 30*time.Millisecond, "neither mailbox should re-ship")
}

// TestValidateMultiMailbox covers the validation rules gating the new modes.
func TestValidateMultiMailbox(t *testing.T) {
	base := func() GmailConfig {
		return GmailConfig{
			ClientOptions:             testClientOptions(t),
			ServiceAccountCredentials: `{"client_email":"x"}`,
		}
	}

	t.Run("service account with only subjects is valid", func(t *testing.T) {
		c := base()
		c.Subjects = []string{"a@x.test", "b@x.test"}
		require.NoError(t, c.Validate())
	})

	t.Run("discover_mailboxes requires admin_subject", func(t *testing.T) {
		c := base()
		c.DiscoverMailboxes = true
		assert.Error(t, c.Validate())
		c.AdminSubject = "admin@x.test"
		assert.NoError(t, c.Validate())
	})

	t.Run("service account with no mailbox source is rejected", func(t *testing.T) {
		c := base() // no subject, no subjects, no discovery
		assert.Error(t, c.Validate())
	})

	t.Run("refresh-token flow rejects multi-mailbox fields", func(t *testing.T) {
		c := GmailConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "id", ClientSecret: "secret", RefreshToken: "rt",
			Subjects: []string{"a@x.test"},
		}
		assert.Error(t, c.Validate())
	})

	t.Run("refresh-token flow rejects admin_subject", func(t *testing.T) {
		c := GmailConfig{
			ClientOptions: testClientOptions(t),
			ClientID:      "id", ClientSecret: "secret", RefreshToken: "rt",
			AdminSubject: "admin@x.test",
		}
		assert.Error(t, c.Validate())
	})

	t.Run("rejects customer and domain together", func(t *testing.T) {
		c := base()
		c.Subject = "a@x.test"
		c.Customer = "my_customer"
		c.Domain = "x.test"
		assert.Error(t, c.Validate())
	})

	t.Run("discovery defaults the customer when neither customer nor domain is set", func(t *testing.T) {
		c := base()
		c.DiscoverMailboxes = true
		c.AdminSubject = "admin@x.test"
		require.NoError(t, c.Validate())
		assert.Equal(t, defaultCustomer, c.Customer)
	})
}
