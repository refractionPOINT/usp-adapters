// Package usp_gmail implements a USP adapter that collects incoming email and
// Business-Email-Compromise signals from one or many Gmail mailboxes via the
// Gmail REST API.
//
// Each mailbox is collected independently and shipped to its own LimaCharlie
// sensor (the sensor seed key and hostname are derived from the mailbox address).
// The adapter polls users.messages.list on a rolling time window (default query
// "in:inbox"), fetches each newly-seen message with users.messages.get, and ships
// it tagged as a "gmail_message" event. A deduper keyed on the immutable Gmail
// message id (namespaced by mailbox) guarantees each message ships exactly once
// even though overlapping windows re-list recent messages.
//
// Authentication:
//
//   - OAuth 2.0 refresh token: a single mailbox. Provide client_id,
//     client_secret and refresh_token.
//   - Service account with domain-wide delegation: one or many Google Workspace
//     mailboxes. Provide the service account JSON (inline or via a file) and the
//     mailbox(es) to impersonate, as either:
//   - subject:  a single mailbox, or
//   - subjects: an explicit static list of mailboxes, and/or
//   - discover_mailboxes: enumerate the domain's mailboxes via the Admin SDK
//     Directory API (requires admin_subject and the
//     admin.directory.user.readonly scope in the delegation).
//
// References:
//   - messages.list: https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list
//   - messages.get:  https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get
//   - directory:     https://developers.google.com/admin-sdk/directory/reference/rest/v1/users/list
//   - search syntax: https://support.google.com/mail/answer/7190
//   - OAuth/service accounts: https://developers.google.com/identity/protocols/oauth2/service-account
package usp_gmail

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultUserID               = "me"
	defaultQuery                = "in:inbox"
	defaultFormat               = "full"
	defaultMaxResults           = 100
	maxAllowedResults           = 500 // Gmail's per-page ceiling for messages.list.
	defaultPollInterval         = 5 * time.Minute
	defaultSettingsPollInterval = 15 * time.Minute
	defaultOverlap              = 2 * time.Minute
	defaultDedupeTTL            = 7 * 24 * time.Hour
	dedupeBucketWindow          = 1 * time.Hour

	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	shipTimeout = 10 * time.Second

	// maxPagesPerPoll caps the number of list pages walked in a single poll, a
	// safety net against a misbehaving nextPageToken that never empties.
	maxPagesPerPoll = 10000

	// defaultMaxConcurrentPolls bounds how many mailboxes hit the Gmail API at
	// the same instant, so a large domain does not blow the per-project quota.
	defaultMaxConcurrentPolls = 10

	// defaultDiscoveryInterval is how often the Directory API is re-enumerated to
	// pick up newly-provisioned (or deprovisioned) mailboxes.
	defaultDiscoveryInterval = 1 * time.Hour

	// defaultCustomer is the Directory API customer alias for "the account that
	// made the request".
	defaultCustomer = "my_customer"

	// adminDirectoryUserReadonlyScope is the delegated scope the discovery flow
	// needs to enumerate the domain's mailboxes.
	adminDirectoryUserReadonlyScope = "https://www.googleapis.com/auth/admin.directory.user.readonly"

	// Event types. Each capability ships under its own type so detections can be
	// written against the specific signal.
	eventTypeMessage           = "gmail_message"            // incoming-mail telemetry
	eventTypeFilter            = "gmail_filter"             // a mail filter / rule
	eventTypeForwardingAddress = "gmail_forwarding_address" // a verified forwarding destination
	eventTypeAutoForwarding    = "gmail_auto_forwarding"    // account-wide auto-forward setting
	eventTypeSendAs            = "gmail_send_as"            // a send-as alias / "from" identity
	eventTypeDelegate          = "gmail_delegate"           // a mailbox delegate (Workspace only)
	eventTypeImap              = "gmail_imap"               // IMAP access setting
	eventTypePop               = "gmail_pop"                // POP access setting
	eventTypeVacation          = "gmail_vacation"           // vacation responder setting
	eventTypeHistory           = "gmail_history"            // a mailbox change record (deletions, label changes)
)

// validFormats is the set of accepted users.messages.get format values.
var validFormats = map[string]struct{}{
	"minimal":  {},
	"full":     {},
	"raw":      {},
	"metadata": {},
}

// GmailConfig is the adapter configuration.
type GmailConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// --- Authentication: refresh-token flow (single mailbox) ---------------

	// ClientID and ClientSecret identify the OAuth application (Google Cloud
	// console > APIs & Services > Credentials).
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"client_secret" yaml:"client_secret"`

	// RefreshToken is a long-lived OAuth 2.0 refresh token for the mailbox
	// owner, obtained from the authorization-code flow.
	RefreshToken string `json:"refresh_token" yaml:"refresh_token"`

	// --- Authentication: service-account flow (Workspace delegation) -------

	// ServiceAccountCredentials is the service account JSON key, inline.
	ServiceAccountCredentials string `json:"service_account_credentials" yaml:"service_account_credentials"`

	// ServiceAccountFile is a path to the service account JSON key file. Used
	// when ServiceAccountCredentials is empty.
	ServiceAccountFile string `json:"service_account_file" yaml:"service_account_file"`

	// Subject is a single mailbox owner the service account impersonates via
	// domain-wide delegation (e.g. "user@yourdomain.com").
	Subject string `json:"subject" yaml:"subject"`

	// --- Multi-mailbox (service-account flow only) -------------------------

	// Subjects is an explicit static list of mailboxes to collect, each
	// impersonated independently and shipped to its own sensor. May be combined
	// with Subject and with DiscoverMailboxes (the union is collected).
	Subjects []string `json:"subjects" yaml:"subjects"`

	// DiscoverMailboxes enables automatic enumeration of the Workspace domain's
	// mailboxes via the Admin SDK Directory API, re-run on DiscoveryInterval so
	// newly-provisioned mailboxes are picked up and deprovisioned ones dropped.
	// Requires AdminSubject and that domain-wide delegation grants the
	// admin.directory.user.readonly scope.
	DiscoverMailboxes bool `json:"discover_mailboxes" yaml:"discover_mailboxes"`

	// AdminSubject is the admin user the service account impersonates for the
	// Directory API enumeration (the Gmail collection still impersonates each
	// discovered mailbox). Required when DiscoverMailboxes is set.
	AdminSubject string `json:"admin_subject" yaml:"admin_subject"`

	// Customer is the Directory API customer id (default "my_customer"). Mutually
	// exclusive with Domain.
	Customer string `json:"customer" yaml:"customer"`

	// Domain restricts discovery to a single domain of a multi-domain Workspace.
	Domain string `json:"domain" yaml:"domain"`

	// DiscoveryQuery is an optional Directory API user search filter, passed
	// through verbatim as the users.list "query" parameter (e.g.
	// "orgUnitPath='/Finance'" or "isSuspended=false"). See
	// https://developers.google.com/admin-sdk/directory/v1/guides/search-users
	// for the exact syntax (note that values like an org-unit path are quoted).
	DiscoveryQuery string `json:"discovery_query" yaml:"discovery_query"`

	// DiscoveryInterval is how often discovery re-enumerates. Default 1 hour.
	DiscoveryInterval time.Duration `json:"discovery_interval" yaml:"discovery_interval"`

	// IncludeSuspended collects suspended mailboxes too. By default discovery
	// skips suspended accounts.
	IncludeSuspended bool `json:"include_suspended" yaml:"include_suspended"`

	// --- Capabilities ------------------------------------------------------
	//
	// Each capability is an independent collector that ships its own event type.
	// They are opt-in: if NONE of these are set, the adapter defaults to message
	// telemetry only (CollectMessages). The configuration-state capabilities
	// (filters, forwarding, send-as, delegates, imap/pop, vacation) emit
	// change-only events -- an item is shipped when it first appears or its
	// content changes, suppressed otherwise -- which surfaces the
	// persistence/exfiltration changes typical of Business Email Compromise.

	// CollectMessages ships incoming email as "gmail_message" telemetry. This is
	// the default when no capability is selected.
	CollectMessages bool `json:"collect_messages" yaml:"collect_messages"`

	// CollectFilters ships mail filters/rules as "gmail_filter". BEC actors create
	// rules that auto-delete or auto-forward mail, or hide replies about
	// invoices/wires so the victim never sees them.
	CollectFilters bool `json:"collect_filters" yaml:"collect_filters"`

	// CollectForwarding ships forwarding addresses ("gmail_forwarding_address")
	// and the account-wide auto-forwarding setting ("gmail_auto_forwarding"). A
	// classic mail-exfiltration vector.
	CollectForwarding bool `json:"collect_forwarding" yaml:"collect_forwarding"`

	// CollectSendAs ships send-as aliases as "gmail_send_as". An added "from"
	// identity is an impersonation/persistence signal.
	CollectSendAs bool `json:"collect_send_as" yaml:"collect_send_as"`

	// CollectDelegates ships mailbox delegates as "gmail_delegate". Granting a
	// delegate is a persistence mechanism. Google exposes this only to service
	// accounts with domain-wide delegation (Workspace); it is unavailable on
	// consumer accounts and such failures are logged and skipped, not fatal.
	CollectDelegates bool `json:"collect_delegates" yaml:"collect_delegates"`

	// CollectImapPop ships the IMAP ("gmail_imap") and POP ("gmail_pop") access
	// settings. Enabling these allows bulk mailbox download via a desktop client,
	// bypassing browser-session controls.
	CollectImapPop bool `json:"collect_imap_pop" yaml:"collect_imap_pop"`

	// CollectVacation ships the vacation-responder setting as "gmail_vacation".
	CollectVacation bool `json:"collect_vacation" yaml:"collect_vacation"`

	// CollectHistory ships mailbox change records as "gmail_history": message
	// deletions and label changes (e.g. marking security alerts read or trashing
	// the fraud thread), which is how an intruder covers their tracks. Uses
	// users.history.list, whose history is retained for roughly a week.
	CollectHistory bool `json:"collect_history" yaml:"collect_history"`

	// SettingsPollInterval is the cadence for the configuration-state capabilities
	// (filters, forwarding, send-as, delegates, imap/pop, vacation). These change
	// rarely, so they poll on a slower clock than messages. Default 15 minutes.
	// History and messages poll on PollInterval.
	SettingsPollInterval time.Duration `json:"settings_poll_interval" yaml:"settings_poll_interval"`

	// --- Collection knobs --------------------------------------------------

	// UserID is the mailbox path segment for the refresh-token flow. Default "me"
	// (the authenticated user). Ignored for the service-account flow, where each
	// mailbox is reached as "me" under its own impersonation.
	UserID string `json:"user_id" yaml:"user_id"`

	// Query is the Gmail search query selecting which messages to collect.
	// Default "in:inbox". A time bound (after:<epoch>) is appended automatically
	// per poll; do not include one here.
	Query string `json:"query" yaml:"query"`

	// Scopes overrides the OAuth scopes requested for Gmail collection. Default:
	// gmail.readonly. (Discovery uses the admin.directory.user.readonly scope
	// separately.)
	Scopes []string `json:"scopes" yaml:"scopes"`

	// Format selects how much of each message to fetch: minimal, full, raw or
	// metadata. Default "full".
	Format string `json:"format" yaml:"format"`

	// MetadataHeaders restricts the headers returned when Format is "metadata".
	MetadataHeaders []string `json:"metadata_headers" yaml:"metadata_headers"`

	// LabelIDs restricts the listing to messages carrying all of these labels.
	LabelIDs []string `json:"label_ids" yaml:"label_ids"`

	// IncludeSpamTrash includes SPAM and TRASH messages in the listing.
	IncludeSpamTrash bool `json:"include_spam_trash" yaml:"include_spam_trash"`

	// MaxResults is the page size for messages.list. Default 100, capped at 500.
	MaxResults int `json:"max_results" yaml:"max_results"`

	// PollInterval is the wait between list polls. Default 5 minutes.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// MaxConcurrentPolls bounds how many mailboxes poll the Gmail API at once.
	// Default 10. Only meaningful with many mailboxes.
	MaxConcurrentPolls int `json:"max_concurrent_polls" yaml:"max_concurrent_polls"`

	// Overlap extends each poll's window backwards past the previous poll so a
	// slipped cycle or a late-indexed message leaves no gap; the deduper
	// suppresses the resulting re-listing. Default 2 minutes.
	Overlap time.Duration `json:"overlap" yaml:"overlap"`

	// InitialLookback, when > 0, makes the first poll reach back this far to
	// backfill recent mail on startup. Default 0: collection starts from the
	// moment the mailbox's collector launches (minus Overlap).
	InitialLookback time.Duration `json:"initial_lookback" yaml:"initial_lookback"`

	// DedupeTTL is how long a message id is remembered to suppress re-shipping
	// across overlapping windows. Default 7 days.
	DedupeTTL time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

	// Retry tuning for transient API failures.
	RetryBaseDelay   time.Duration `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay    time.Duration `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts int           `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// --- Test / embedder seams ---------------------------------------------

	// BaseURL fully overrides the Gmail API root (e.g. for testing).
	BaseURL string `json:"base_url" yaml:"base_url"`

	// TokenURL overrides the OAuth token endpoint (e.g. for testing).
	TokenURL string `json:"token_url" yaml:"token_url"`

	// DirectoryBaseURL overrides the Admin SDK Directory API root (e.g. for
	// testing).
	DirectoryBaseURL string `json:"directory_base_url" yaml:"directory_base_url"`

	// Deduper, when set, replaces the built-in in-memory deduper. Shared across
	// all mailboxes (keys are mailbox-namespaced). Not settable through a config
	// file; it is a seam for tests and embedders.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

// usesServiceAccount reports whether the config selects the service-account flow.
func (c *GmailConfig) usesServiceAccount() bool {
	return c.ServiceAccountCredentials != "" || c.ServiceAccountFile != ""
}

// usesDiscovery reports whether Directory-API mailbox discovery is enabled.
func (c *GmailConfig) usesDiscovery() bool {
	return c.DiscoverMailboxes
}

// isMultiMailbox reports whether more than one mailbox may be collected, in which
// case each mailbox gets a per-mailbox-derived sensor identity. A single explicit
// mailbox keeps the operator's configured sensor identity verbatim.
func (c *GmailConfig) isMultiMailbox() bool {
	return len(c.Subjects) > 0 || c.DiscoverMailboxes
}

// anyCapabilityEnabled reports whether the operator turned on at least one
// collector.
func (c *GmailConfig) anyCapabilityEnabled() bool {
	return c.CollectMessages || c.CollectHistory || c.anySettingsCapabilityEnabled()
}

// anySettingsCapabilityEnabled reports whether any configuration-state capability
// (the ones that poll on SettingsPollInterval) is enabled.
func (c *GmailConfig) anySettingsCapabilityEnabled() bool {
	return c.CollectFilters || c.CollectForwarding || c.CollectSendAs ||
		c.CollectDelegates || c.CollectImapPop || c.CollectVacation
}

func (c *GmailConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	hasRefresh := c.ClientID != "" || c.ClientSecret != "" || c.RefreshToken != ""
	hasServiceAccount := c.usesServiceAccount()

	switch {
	case hasServiceAccount && hasRefresh:
		return errors.New("provide either the service-account credentials or the refresh-token credentials, not both")
	case hasServiceAccount:
		if c.ServiceAccountCredentials != "" && c.ServiceAccountFile != "" {
			return errors.New("provide service_account_credentials or service_account_file, not both")
		}
		if c.Customer != "" && c.Domain != "" {
			return errors.New("provide customer or domain, not both")
		}
		hasSubjects := strings.TrimSpace(c.Subject) != "" || len(nonEmpty(c.Subjects)) > 0
		if !hasSubjects && !c.DiscoverMailboxes {
			return errors.New("service account flow requires at least one mailbox: set subject, subjects, or enable discover_mailboxes")
		}
		if c.DiscoverMailboxes && strings.TrimSpace(c.AdminSubject) == "" {
			return errors.New("discover_mailboxes requires admin_subject (an admin user to impersonate for the Directory API)")
		}
	case hasRefresh:
		if c.ClientID == "" {
			return errors.New("missing client_id")
		}
		if c.ClientSecret == "" {
			return errors.New("missing client_secret")
		}
		if c.RefreshToken == "" {
			return errors.New("missing refresh_token")
		}
		if c.isMultiMailbox() || strings.TrimSpace(c.AdminSubject) != "" {
			return errors.New("multi-mailbox (subjects / discover_mailboxes / admin_subject) requires the service-account flow")
		}
	default:
		return errors.New("missing credentials: set either a refresh token (client_id, client_secret, refresh_token) or a service account (service_account_credentials/service_account_file + subject/subjects/discover_mailboxes)")
	}

	// Capabilities are opt-in. If the operator selected none, fall back to
	// message telemetry only.
	if !c.anyCapabilityEnabled() {
		c.CollectMessages = true
	}

	if c.UserID == "" {
		c.UserID = defaultUserID
	}
	if c.Query == "" {
		c.Query = defaultQuery
	}
	if len(c.Scopes) == 0 {
		c.Scopes = []string{gmailReadonlyScope}
	}
	if c.Format == "" {
		c.Format = defaultFormat
	}
	if _, ok := validFormats[c.Format]; !ok {
		return fmt.Errorf("invalid format %q (want one of minimal, full, raw, metadata)", c.Format)
	}
	if c.MaxResults <= 0 {
		c.MaxResults = defaultMaxResults
	}
	if c.MaxResults > maxAllowedResults {
		c.MaxResults = maxAllowedResults
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.SettingsPollInterval <= 0 {
		c.SettingsPollInterval = defaultSettingsPollInterval
	}
	if c.MaxConcurrentPolls <= 0 {
		c.MaxConcurrentPolls = defaultMaxConcurrentPolls
	}
	if c.DiscoveryInterval <= 0 {
		c.DiscoveryInterval = defaultDiscoveryInterval
	}
	if c.DiscoverMailboxes && c.Customer == "" && c.Domain == "" {
		c.Customer = defaultCustomer
	}
	if c.Overlap < 0 {
		c.Overlap = 0
	}
	if c.Overlap == 0 {
		c.Overlap = defaultOverlap
	}
	if c.InitialLookback < 0 {
		c.InitialLookback = 0
	}
	if c.DedupeTTL <= 0 {
		c.DedupeTTL = defaultDedupeTTL
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = defaultRetryBaseDelay
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = defaultMaxRetryDelay
	}
	if c.MaxRetryAttempts <= 0 {
		c.MaxRetryAttempts = defaultMaxRetryAttempts
	}
	return nil
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink; *uspclient.Client
// satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// sinkFactory builds the LimaCharlie sink (sensor) for one mailbox. Production
// uses defaultSinkFactory; tests inject a capturing factory.
type sinkFactory func(ctx context.Context, opts uspclient.ClientOptions, mailbox string) (uspSink, error)

func defaultSinkFactory(ctx context.Context, opts uspclient.ClientOptions, _ string) (uspSink, error) {
	return uspclient.NewClient(ctx, opts)
}

// mailboxTarget identifies one mailbox to collect and how to reach it.
type mailboxTarget struct {
	address string // mailbox address; identifies the sensor and namespaces dedupe keys
	subject string // service-account subject to impersonate; "" for the refresh-token flow
	userID  string // Gmail API path userId
}

// GmailAdapter orchestrates per-mailbox collectors and (optionally) Directory-API
// mailbox discovery.
type GmailAdapter struct {
	conf GmailConfig
	ctx  context.Context

	usesSA bool
	saKey  *serviceAccountKey
	saRSA  *rsa.PrivateKey

	baseURL  string
	tokenURL string

	authHTTP *http.Client // shared by all token sources
	apiHTTP  *http.Client // shared by all Gmail clients

	newSink   sinkFactory
	pollSlots chan struct{} // bounded poll-concurrency across all mailboxes

	// apiRootCtx parents every Gmail and Directory API request (but not the
	// sinks, which use ctx directly so they can still drain after apiCancel).
	// Close cancels it to abort in-flight requests promptly.
	apiRootCtx context.Context
	apiCancel  context.CancelFunc

	directory *directoryClient // nil unless discovery is enabled

	mu          sync.Mutex
	collectors  map[string]*mailboxCollector // keyed by mailbox address
	staticAddrs map[string]bool              // mailboxes from config; discovery never removes these

	doStop    *utils.Event // stops the discovery loop; gates Close
	wgSenders sync.WaitGroup
	chStopped chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// NewGmailAdapter creates a Gmail adapter wired to LimaCharlie.
func NewGmailAdapter(ctx context.Context, conf GmailConfig) (*GmailAdapter, chan struct{}, error) {
	return newGmailAdapter(ctx, conf, defaultSinkFactory)
}

// newGmailAdapter is the implementation behind NewGmailAdapter. newSink is the
// seam tests use to capture shipped events per mailbox.
func newGmailAdapter(ctx context.Context, conf GmailConfig, newSink sinkFactory) (*GmailAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	apiRootCtx, apiCancel := context.WithCancel(ctx)
	a := &GmailAdapter{
		conf:       conf,
		ctx:        ctx,
		usesSA:     conf.usesServiceAccount(),
		baseURL:    resolveBaseURL(conf),
		tokenURL:   resolveTokenURL(conf),
		authHTTP:   newAuthHTTPClient(),
		apiHTTP:    newAPIHTTPClient(),
		newSink:    newSink,
		pollSlots:  make(chan struct{}, conf.MaxConcurrentPolls),
		apiRootCtx: apiRootCtx,
		apiCancel:  apiCancel,
		collectors: map[string]*mailboxCollector{},
		doStop:     utils.NewEvent(),
		chStopped:  make(chan struct{}),
	}

	if a.usesSA {
		key, rsaKey, err := a.parseServiceAccount()
		if err != nil {
			a.apiCancel()
			a.apiHTTP.CloseIdleConnections()
			a.authHTTP.CloseIdleConnections()
			return nil, nil, err
		}
		a.saKey, a.saRSA = key, rsaKey
	}

	// Start the static mailboxes (the single refresh-token mailbox, or the
	// explicit service-account subject/subjects). Spread their first polls across
	// one PollInterval so they do not all fire together.
	static := a.staticTargets()
	a.staticAddrs = make(map[string]bool, len(static))
	for _, t := range static {
		a.staticAddrs[t.address] = true
	}
	for i, t := range static {
		var delay time.Duration
		if len(static) > 1 {
			delay = time.Duration(i) * (conf.PollInterval / time.Duration(len(static)))
		}
		if err := a.startCollector(t, delay); err != nil {
			a.shutdownStartedCollectors()
			a.apiCancel()
			a.apiHTTP.CloseIdleConnections()
			a.authHTTP.CloseIdleConnections()
			return nil, nil, err
		}
	}

	// Discovery runs as a sender so the wait group never drains to zero while it
	// is still looking for mailboxes.
	if a.conf.usesDiscovery() {
		dir, err := a.newDirectoryClient()
		if err != nil {
			a.shutdownStartedCollectors()
			a.apiCancel()
			a.apiHTTP.CloseIdleConnections()
			a.authHTTP.CloseIdleConnections()
			return nil, nil, err
		}
		a.directory = dir
		a.wgSenders.Add(1)
		go a.runDiscovery()
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// staticTargets enumerates the mailboxes known at startup (before discovery).
func (a *GmailAdapter) staticTargets() []mailboxTarget {
	if !a.usesSA {
		addr := canonicalMailbox(a.conf.UserID)
		return []mailboxTarget{{address: addr, subject: "", userID: a.conf.UserID}}
	}
	seen := map[string]bool{}
	var out []mailboxTarget
	add := func(s string) {
		s = canonicalMailbox(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, mailboxTarget{address: s, subject: s, userID: defaultUserID})
	}
	add(a.conf.Subject)
	for _, s := range a.conf.Subjects {
		add(s)
	}
	return out
}

// canonicalMailbox normalizes a mailbox address to a stable key. Workspace
// addresses are case-insensitive, so two casings of the same mailbox must map to
// one collector / one sensor / one dedupe namespace.
func canonicalMailbox(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// parseServiceAccount loads and parses the configured service-account key.
func (a *GmailAdapter) parseServiceAccount() (*serviceAccountKey, *rsa.PrivateKey, error) {
	raw := []byte(a.conf.ServiceAccountCredentials)
	if len(raw) == 0 {
		data, err := os.ReadFile(a.conf.ServiceAccountFile)
		if err != nil {
			return nil, nil, fmt.Errorf("gmail: failed to read service_account_file %q: %v", a.conf.ServiceAccountFile, err)
		}
		raw = data
	}
	key, rsaKey, err := parseServiceAccountKey(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("gmail: %v", err)
	}
	return key, rsaKey, nil
}

// tokenSourceForGmail builds a token source for collecting one mailbox.
func (a *GmailAdapter) tokenSourceForGmail(t mailboxTarget) *tokenSource {
	if a.usesSA {
		return newServiceAccountSource(a.saKey, a.saRSA, t.subject, a.conf.Scopes, a.tokenURL, a.authHTTP)
	}
	return newRefreshTokenSource(a.tokenURL, a.conf.ClientID, a.conf.ClientSecret, a.conf.RefreshToken, a.authHTTP)
}

// sensorOptions derives the LimaCharlie ClientOptions for one mailbox. In
// multi-mailbox mode the sensor seed key and hostname are derived from the
// mailbox address so each mailbox maps to its own sensor; for a single explicit
// mailbox the operator's configured identity is used verbatim.
func (a *GmailAdapter) sensorOptions(mailbox string) uspclient.ClientOptions {
	opts := a.conf.ClientOptions
	if a.conf.isMultiMailbox() {
		opts.SensorSeedKey = mailboxSeedKey(a.conf.ClientOptions.SensorSeedKey, mailbox)
		opts.Hostname = mailbox
	}
	return opts
}

// mailboxSeedKey derives a stable, per-mailbox sensor seed key.
func mailboxSeedKey(base, mailbox string) string {
	if base == "" {
		return mailbox
	}
	return base + "/" + mailbox
}

// startCollector creates and starts a collector for one mailbox, if not already
// running. It is safe for concurrent use (discovery calls it).
func (a *GmailAdapter) startCollector(t mailboxTarget, startDelay time.Duration) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.doStop.IsSet() {
		return nil // adapter is closing; do not spin up new work
	}
	if _, exists := a.collectors[t.address]; exists {
		return nil
	}

	ts := a.tokenSourceForGmail(t)
	client := NewGmailClient(a.baseURL, t.userID, ts, a.apiHTTP)

	sink, err := a.newSink(a.ctx, a.sensorOptions(t.address), t.address)
	if err != nil {
		return fmt.Errorf("gmail: sensor init for mailbox %q: %v", t.address, err)
	}

	deduper := a.conf.Deduper
	ownsDeduper := false
	if deduper == nil {
		window := dedupeBucketWindow
		if window > a.conf.DedupeTTL {
			window = a.conf.DedupeTTL
		}
		d, derr := utils.NewLocalDeduper(window, a.conf.DedupeTTL)
		if derr != nil {
			_ = sink.Drain(time.Second)
			_, _ = sink.Close()
			return fmt.Errorf("gmail: deduper for mailbox %q: %v", t.address, derr)
		}
		deduper = d
		ownsDeduper = true
	}

	now := time.Now()
	since := now
	if a.conf.InitialLookback > 0 {
		since = now.Add(-a.conf.InitialLookback)
	}

	// The API context descends from the adapter's apiRootCtx so both this
	// collector's requestStop and the adapter's Close can abort an in-flight Gmail
	// request; the sink, by contrast, was created with the raw adapter context so
	// cleanup can still drain it after the request context is cancelled.
	apiCtx, apiCancel := context.WithCancel(a.apiRootCtx)

	c := &mailboxCollector{
		a:           a,
		mailbox:     t.address,
		tag:         "gmail[" + t.address + "]",
		client:      client,
		ts:          ts,
		uspClient:   sink,
		apiCtx:      apiCtx,
		apiCancel:   apiCancel,
		deduper:     deduper,
		ownsDeduper: ownsDeduper,
		since:       since,
		startDelay:  startDelay,
		stop:        utils.NewEvent(),
		stopCh:      make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	a.collectors[t.address] = c
	a.wgSenders.Add(1)
	go c.run()
	return nil
}

// removeCollector stops and tears down the collector for a mailbox that is no
// longer present (deprovisioned). It blocks until the collector's loop has
// exited and its sensor has drained.
func (a *GmailAdapter) removeCollector(address string) {
	a.mu.Lock()
	c, ok := a.collectors[address]
	if ok {
		delete(a.collectors, address)
	}
	a.mu.Unlock()
	if !ok {
		return
	}
	c.requestStop()
	<-c.stopped
	if err := c.cleanup(); err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("gmail: draining sensor for %q: %v", address, err))
	}
	a.conf.ClientOptions.DebugLog("gmail: stopped collecting deprovisioned mailbox " + address)
}

// activeMailboxes returns the set of mailboxes currently being collected.
func (a *GmailAdapter) activeMailboxes() map[string]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]bool, len(a.collectors))
	for addr := range a.collectors {
		out[addr] = true
	}
	return out
}

// shutdownStartedCollectors stops and tears down every collector started so far.
// Used to unwind a partially-constructed adapter.
func (a *GmailAdapter) shutdownStartedCollectors() {
	a.mu.Lock()
	cs := make([]*mailboxCollector, 0, len(a.collectors))
	for _, c := range a.collectors {
		cs = append(cs, c)
	}
	a.collectors = map[string]*mailboxCollector{}
	a.mu.Unlock()

	for _, c := range cs {
		c.requestStop()
	}
	for _, c := range cs {
		<-c.stopped
	}
	cleanupCollectors(cs)
}

// Close stops the adapter and all mailbox collectors. It is idempotent.
func (a *GmailAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("gmail: closing")
		a.doStop.Set() // stop the discovery loop

		a.mu.Lock()
		cs := make([]*mailboxCollector, 0, len(a.collectors))
		for _, c := range a.collectors {
			cs = append(cs, c)
		}
		a.mu.Unlock()

		for _, c := range cs {
			c.requestStop()
		}
		a.apiCancel()      // abort any in-flight Gmail/Directory request (e.g. a discovery enumeration)
		a.wgSenders.Wait() // all collector loops and the discovery loop have exited

		// Drain the sensors concurrently: each Drain can wait up to a minute, and
		// a large domain may have many mailboxes, so a serial loop could take far
		// too long to shut down.
		a.closeErr = cleanupCollectors(cs)

		if a.directory != nil {
			a.directory.Close()
		}
		a.apiHTTP.CloseIdleConnections()
		a.authHTTP.CloseIdleConnections()
	})
	return a.closeErr
}

// cleanupCollectors drains and closes a set of collectors concurrently (bounded
// fan-out), returning the first error. Each collector's cleanup is idempotent.
func cleanupCollectors(cs []*mailboxCollector) error {
	if len(cs) == 0 {
		return nil
	}
	const maxConcurrentDrains = 16
	sem := make(chan struct{}, maxConcurrentDrains)
	errs := make([]error, len(cs))
	var wg sync.WaitGroup
	for i, c := range cs {
		wg.Add(1)
		go func(i int, c *mailboxCollector) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			errs[i] = c.cleanup()
		}(i, c)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// resolveBaseURL computes the Gmail API root from the config.
func resolveBaseURL(conf GmailConfig) string {
	if conf.BaseURL != "" {
		return strings.TrimRight(conf.BaseURL, "/")
	}
	return gmailAPIBaseURL
}

// resolveTokenURL computes the OAuth token endpoint from the config.
func resolveTokenURL(conf GmailConfig) string {
	if conf.TokenURL != "" {
		return conf.TokenURL
	}
	return googleTokenEndpoint
}

// nonEmpty returns the input with blank entries removed.
func nonEmpty(in []string) []string {
	var out []string
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}
