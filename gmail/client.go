// Package usp_gmail implements a USP adapter that collects incoming email as
// telemetry from a Gmail mailbox via the Gmail REST API.
//
// It polls users.messages.list on a rolling time window (default query
// "in:inbox"), fetches each newly-seen message with users.messages.get, and
// ships it to LimaCharlie tagged as a "gmail_message" event. A deduper keyed on
// the immutable Gmail message id guarantees each message ships exactly once even
// though overlapping windows re-list recent messages.
//
// Two authentication modes are supported:
//
//   - OAuth 2.0 refresh token: for collecting a single user's mailbox. Provide
//     client_id, client_secret and refresh_token.
//   - Service account with domain-wide delegation: for a Google Workspace, where
//     a service account impersonates a mailbox owner. Provide the service
//     account JSON (inline or via a file) and the subject to impersonate.
//
// References:
//   - messages.list: https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list
//   - messages.get:  https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get
//   - search syntax: https://support.google.com/mail/answer/7190
//   - OAuth/service accounts: https://developers.google.com/identity/protocols/oauth2/service-account
package usp_gmail

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
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

	// Subject is the mailbox owner the service account impersonates via
	// domain-wide delegation (e.g. "user@yourdomain.com"). Required for the
	// service-account flow.
	Subject string `json:"subject" yaml:"subject"`

	// --- Capabilities ------------------------------------------------------
	//
	// Each capability is an independent collector that ships its own event type.
	// They are opt-in: if NONE of these are set, the adapter defaults to message
	// telemetry only (CollectMessages), preserving the original behavior. The
	// configuration-state capabilities (filters, forwarding, send-as, delegates,
	// imap/pop, vacation) emit change-only events -- an item is shipped when it
	// first appears or its content changes, suppressed otherwise -- which surfaces
	// the persistence/exfiltration changes typical of Business Email Compromise.

	// CollectMessages ships incoming email as "gmail_message" telemetry. This is
	// the original behavior and the default when no capability is selected.
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

	// UserID is the mailbox to read. Default "me" (the authenticated or
	// impersonated user). May be an email address.
	UserID string `json:"user_id" yaml:"user_id"`

	// Query is the Gmail search query selecting which messages to collect.
	// Default "in:inbox". A time bound (after:<epoch>) is appended automatically
	// per poll; do not include one here.
	Query string `json:"query" yaml:"query"`

	// Scopes overrides the OAuth scopes requested. Default: gmail.readonly.
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

	// Overlap extends each poll's window backwards past the previous poll so a
	// slipped cycle or a late-indexed message leaves no gap; the deduper
	// suppresses the resulting re-listing. Default 2 minutes.
	Overlap time.Duration `json:"overlap" yaml:"overlap"`

	// InitialLookback, when > 0, makes the first poll reach back this far to
	// backfill recent mail on startup. Default 0: collection starts from the
	// moment the adapter launches (minus Overlap).
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

	// Deduper, when set, replaces the built-in in-memory deduper. Not settable
	// through a config file; it is a seam for tests and embedders.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

// usesServiceAccount reports whether the config selects the service-account flow.
func (c *GmailConfig) usesServiceAccount() bool {
	return c.ServiceAccountCredentials != "" || c.ServiceAccountFile != ""
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
		if strings.TrimSpace(c.Subject) == "" {
			return errors.New("service account flow requires subject (the mailbox user to impersonate via domain-wide delegation)")
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
	default:
		return errors.New("missing credentials: set either a refresh token (client_id, client_secret, refresh_token) or a service account (service_account_credentials/service_account_file + subject)")
	}

	// Capabilities are opt-in. If the operator selected none, fall back to the
	// original behavior: message telemetry only.
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

// GmailAdapter polls a Gmail mailbox and ships incoming messages to LimaCharlie.
type GmailAdapter struct {
	conf        GmailConfig
	uspClient   uspSink
	client      *GmailClient
	ts          *tokenSource
	deduper     utils.Deduper
	ownsDeduper bool

	// since is the high-water mark of the message collection window. It advances
	// to the poll's end time after every fully-successful message poll.
	since time.Time

	// lastSettings is when the configuration-state capabilities last ran; they
	// poll on the slower SettingsPollInterval clock. Zero means "never" (due).
	lastSettings time.Time

	// lastHistoryID is the users.history.list cursor: history is collected from
	// this id forward. Empty until a baseline is established from the profile.
	lastHistoryID string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewGmailAdapter creates a Gmail adapter wired to LimaCharlie.
func NewGmailAdapter(ctx context.Context, conf GmailConfig) (*GmailAdapter, chan struct{}, error) {
	return newGmailAdapter(ctx, conf, nil)
}

// newGmailAdapter is the implementation behind NewGmailAdapter. When sink is
// non-nil it is used in place of a real LimaCharlie client -- the seam tests use
// to capture shipped events.
func newGmailAdapter(ctx context.Context, conf GmailConfig, sink uspSink) (*GmailAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &GmailAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	now := time.Now()
	a.since = now
	if conf.InitialLookback > 0 {
		a.since = now.Add(-conf.InitialLookback)
	}

	// Overlapping windows re-list recent messages, so a deduper is required to
	// ship each message exactly once.
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("gmail: deduper: %v", err)
		}
		a.deduper = deduper
		a.ownsDeduper = true
	}

	ts, err := buildTokenSource(conf)
	if err != nil {
		if a.ownsDeduper {
			a.deduper.Close()
		}
		return nil, nil, err
	}
	a.ts = ts

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			if a.ownsDeduper {
				a.deduper.Close()
			}
			a.ts.Close()
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.client = NewGmailClient(resolveBaseURL(conf), conf.UserID, a.ts, newAPIHTTPClient())

	a.chStopped = make(chan struct{})
	a.wgSenders.Add(1)
	go a.run()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// buildTokenSource constructs the token source for the configured auth flow.
func buildTokenSource(conf GmailConfig) (*tokenSource, error) {
	tokenURL := resolveTokenURL(conf)
	httpClient := newAuthHTTPClient()

	if conf.usesServiceAccount() {
		raw := []byte(conf.ServiceAccountCredentials)
		if len(raw) == 0 {
			data, err := os.ReadFile(conf.ServiceAccountFile)
			if err != nil {
				return nil, fmt.Errorf("gmail: failed to read service_account_file %q: %v", conf.ServiceAccountFile, err)
			}
			raw = data
		}
		key, rsaKey, err := parseServiceAccountKey(raw)
		if err != nil {
			return nil, fmt.Errorf("gmail: %v", err)
		}
		return newServiceAccountSource(key, rsaKey, conf.Subject, conf.Scopes, tokenURL, httpClient), nil
	}

	return newRefreshTokenSource(tokenURL, conf.ClientID, conf.ClientSecret, conf.RefreshToken, httpClient), nil
}

// Close stops the adapter. It is idempotent.
func (a *GmailAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("gmail: closing")
		a.doStop.Set()
		a.wgSenders.Wait()
		err1 := a.uspClient.Drain(1 * time.Minute)
		_, err2 := a.uspClient.Close()
		a.client.Close()
		a.ts.Close()
		if a.ownsDeduper {
			a.deduper.Close()
		}
		if err1 != nil {
			a.closeErr = err1
		} else {
			a.closeErr = err2
		}
	})
	return a.closeErr
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

// run polls Gmail forever, until the adapter is asked to stop.
func (a *GmailAdapter) run() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("gmail: collection stopped")

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		a.runCycle()
	}
}

// runCycle runs one tick of every enabled capability. Messages and history poll
// every cycle; the configuration-state capabilities poll on their slower
// SettingsPollInterval clock. A capability failing does not abort the others.
func (a *GmailAdapter) runCycle() {
	if a.conf.CollectMessages {
		a.pollMessages()
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.CollectHistory {
		a.pollHistory()
	}
	if a.doStop.IsSet() {
		return
	}
	if a.conf.anySettingsCapabilityEnabled() && a.settingsDue(time.Now()) {
		a.pollSettings()
		a.lastSettings = time.Now()
	}
}

// settingsDue reports whether the configuration-state capabilities should run
// this cycle: always on the first pass, then once per SettingsPollInterval.
func (a *GmailAdapter) settingsDue(now time.Time) bool {
	return a.lastSettings.IsZero() || now.Sub(a.lastSettings) >= a.conf.SettingsPollInterval
}

// pollMessages performs one message collection cycle over the rolling window:
// list message ids, fetch each not-yet-seen message, and ship it. On success the
// window's high-water mark advances; on any error it does not, so the next poll
// re-covers the same range (the deduper prevents re-shipping).
func (a *GmailAdapter) pollMessages() {
	end := time.Now()
	start := a.since.Add(-a.conf.Overlap)
	query := buildQuery(a.conf.Query, start)

	pageToken := ""
	nFetched, nShipped := 0, 0
	for page := 0; page < maxPagesPerPoll; page++ {
		if a.doStop.IsSet() {
			return
		}

		raw, ok := a.list(query, pageToken)
		if !ok {
			return
		}
		list, err := parseListMessages(raw)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("gmail: list parse error: %v", err))
			return
		}

		for _, ref := range list.Messages {
			if a.doStop.IsSet() {
				return
			}
			if ref.ID == "" {
				continue
			}

			// Fetch before consulting the deduper: a fetch can fail and we must
			// not mark a message seen unless it actually shipped. The overlap
			// re-lists at most a couple of minutes of recent ids, so the
			// redundant fetches are bounded.
			msg, status := a.fetch(ref.ID)
			switch status {
			case fetchSkip:
				continue
			case fetchAbort:
				return
			}
			nFetched++

			if a.deduper.CheckAndAdd(ref.ID) {
				continue
			}
			if !a.ship(ref.ID, msg) {
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
	a.since = end
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"gmail: poll complete (fetched=%d shipped=%d) up to %s",
		nFetched, nShipped, end.UTC().Format(time.RFC3339)))
}

// list issues one messages.list request with transient-retry backoff. The bool
// is false when the poll should be abandoned (a permanent error, or the adapter
// is stopping).
func (a *GmailAdapter) list(query, pageToken string) ([]byte, bool) {
	raw, err := a.withRetry("messages.list", func() ([]byte, error) {
		return a.client.ListMessages(a.ctx, listMessagesParams{
			query:            query,
			pageToken:        pageToken,
			maxResults:       a.conf.MaxResults,
			includeSpamTrash: a.conf.IncludeSpamTrash,
			labelIDs:         a.conf.LabelIDs,
		})
	})
	if err != nil {
		a.handlePermanent("messages.list", err)
		return nil, false
	}
	return raw, true
}

// fetchResult tells poll what to do after a messages.get attempt.
type fetchResult int

const (
	fetchOK    fetchResult = iota // message fetched, proceed to dedupe/ship
	fetchSkip                     // message gone (404); skip it, keep polling
	fetchAbort                    // unrecoverable; abandon this poll
)

// fetch retrieves one message with transient-retry backoff and classifies the
// outcome. A 404 (the message was deleted between the list and the get) is
// benign and skipped; auth/permission failures abort and stop the adapter.
func (a *GmailAdapter) fetch(id string) (utils.Dict, fetchResult) {
	raw, err := a.withRetry("messages.get", func() ([]byte, error) {
		return a.client.GetMessage(a.ctx, id, a.conf.Format, a.conf.MetadataHeaders)
	})
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("gmail: message %s vanished before fetch; skipping", id))
			return nil, fetchSkip
		}
		a.handlePermanent("messages.get", err)
		return nil, fetchAbort
	}

	msg, err := utils.UnmarshalCleanJSON(string(raw))
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("gmail: failed to parse message %s: %v", id, err))
		return nil, fetchAbort
	}
	return utils.Dict(msg), fetchOK
}

// withRetry runs fn, retrying transient errors with exponential backoff. It
// returns the final result, or the final (non-transient or attempts-exhausted)
// error for the caller to classify. It does not set doStop.
func (a *GmailAdapter) withRetry(label string, fn func() ([]byte, error)) ([]byte, error) {
	var raw []byte
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, context.Canceled
		}

		raw, err = fn()
		if err == nil {
			return raw, nil
		}
		if !isTransientError(err) {
			return nil, err
		}
		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.MaxRetryDelay {
			delay = a.conf.MaxRetryDelay
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"gmail: %s transient error (attempt %d/%d), retrying in %v: %v",
			label, attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, context.Canceled
		}
	}
	return nil, fmt.Errorf("%s failed after %d attempts: %w", label, a.conf.MaxRetryAttempts, err)
}

// handlePermanent logs a non-recoverable error and, when it is a credential
// failure, stops the adapter so it does not spin uselessly every interval. A
// credential problem needs operator attention; other permanent errors (e.g. a
// bad query) abandon this poll but keep the adapter alive to retry next interval.
func (a *GmailAdapter) handlePermanent(label string, err error) {
	if errors.Is(err, context.Canceled) {
		// Adapter is stopping; not an error to surface.
		return
	}

	var tokenErr *TokenError
	var httpErr *HTTPError
	if errors.As(err, &tokenErr) ||
		(errors.As(err, &httpErr) &&
			(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden)) {
		a.conf.ClientOptions.OnError(fmt.Errorf("gmail: %s failed: %v", label, err))
		a.conf.ClientOptions.OnError(fmt.Errorf(
			"gmail: credentials rejected. Verify the OAuth client / refresh token (or service "+
				"account key and that domain-wide delegation grants the %q scope for subject %q), "+
				"and that the Gmail API is enabled for the project", strings.Join(a.conf.Scopes, ","), a.conf.Subject))
		a.doStop.Set()
		return
	}
	a.conf.ClientOptions.OnError(fmt.Errorf("gmail: %s failed: %v", label, err))
}

// ship forwards a single incoming message to LimaCharlie. It returns false if
// the adapter should stop (an unrecoverable shipping error).
func (a *GmailAdapter) ship(id string, msg utils.Dict) bool {
	return a.shipEvent(eventTypeMessage, msg, eventTime(msg))
}

// shipEvent forwards one event of the given type to LimaCharlie. It returns
// false if the adapter should stop (an unrecoverable shipping error).
func (a *GmailAdapter) shipEvent(evtType string, payload utils.Dict, tsMs uint64) bool {
	dataMsg := &protocol.DataMessage{
		JsonPayload: payload,
		EventType:   evtType,
		TimestampMs: tsMs,
	}
	if err := a.uspClient.Ship(dataMsg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("gmail: stream falling behind")
			err = a.uspClient.Ship(dataMsg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("gmail: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
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
