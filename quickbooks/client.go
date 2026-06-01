// Package usp_quickbooks implements a USP adapter that collects change/activity
// data from QuickBooks Online via the Accounting API's ChangeDataCapture (CDC)
// operation.
//
// A note on scope: QuickBooks Online's user-attributed "Audit Log" (the UI
// feature showing who did what) is NOT exposed through any public API. The
// authoritative programmatic way to track activity is ChangeDataCapture, which
// returns the full current state of every entity that changed since a given
// timestamp -- including deletions (status="Deleted"). This adapter polls CDC
// on a rolling window and ships each changed entity to LimaCharlie, tagged with
// its entity type. It captures *what changed and when*, not the acting user.
//
// References:
//   - CDC: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/changedatacapture
//   - OAuth 2.0: https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0
package usp_quickbooks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultPollInterval     = 5 * time.Minute
	defaultOverlap          = 5 * time.Minute
	defaultDedupeTTL        = 7 * 24 * time.Hour
	dedupeBucketWindow      = 1 * time.Hour
	defaultMinorVersion     = "75"
	defaultMaxRetryAttempts = 3
	defaultRetryBaseDelay   = 5 * time.Second
	defaultMaxRetryDelay    = 30 * time.Second

	shipTimeout = 10 * time.Second

	// cdcMaxLookback is the furthest back a CDC request may reach. QuickBooks
	// rejects changedSince values older than 30 days; the adapter clamps to a
	// hair under that to stay clear of the boundary.
	cdcMaxLookback = 29 * 24 * time.Hour

	// cdcObjectCap is the maximum number of objects a single CDC response can
	// carry. Crossing it means changes were silently dropped from the window.
	cdcObjectCap = 1000

	// Production / sandbox API roots and the OAuth token endpoint.
	prodBaseURL    = "https://quickbooks.api.intuit.com"
	sandboxBaseURL = "https://sandbox-quickbooks.api.intuit.com"
	tokenEndpoint  = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
)

// defaultEntities is the out-of-the-box CDC entity set. It covers the core
// transactional and name-list objects whose changes constitute an activity
// trail for a QuickBooks company. Override via the `entities` config to widen
// or narrow it.
var defaultEntities = []string{
	"Account",
	"Bill",
	"BillPayment",
	"CreditMemo",
	"Customer",
	"Employee",
	"Estimate",
	"Invoice",
	"Item",
	"JournalEntry",
	"Payment",
	"Purchase",
	"SalesReceipt",
	"Vendor",
	"VendorCredit",
}

// envelopeKeys are the per-QueryResponse keys that are not entity arrays and
// must be skipped when walking a CDC response block.
var envelopeKeys = map[string]struct{}{
	"startPosition": {},
	"maxResults":    {},
	"totalCount":    {},
}

// QuickBooksConfig is the adapter configuration.
type QuickBooksConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// OAuth 2.0 application credentials (Intuit developer portal > your app >
	// Keys & credentials).
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"client_secret" yaml:"client_secret"`

	// RefreshToken is a long-lived OAuth 2.0 refresh token obtained from the
	// authorization-code flow. The adapter exchanges it for short-lived access
	// tokens and adopts any rotated refresh token returned by Intuit.
	RefreshToken string `json:"refresh_token" yaml:"refresh_token"`

	// RealmID is the QuickBooks company id (a.k.a. realmId), returned on the
	// OAuth callback and present in every API path.
	RealmID string `json:"realm_id" yaml:"realm_id"`

	// Entities is the CDC entity list to poll. Defaults to defaultEntities.
	Entities []string `json:"entities" yaml:"entities"`

	// Sandbox selects the sandbox API root instead of production. Ignored when
	// BaseURL is set.
	Sandbox bool `json:"sandbox" yaml:"sandbox"`

	// BaseURL fully overrides the API root (e.g. for testing). When set,
	// Sandbox is ignored.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// TokenURL overrides the OAuth token endpoint (e.g. for testing).
	TokenURL string `json:"token_url" yaml:"token_url"`

	// MinorVersion pins the Accounting API schema minor version. Default "75".
	MinorVersion string `json:"minor_version" yaml:"minor_version"`

	// PollInterval is the wait between CDC polls. Default 5 minutes.
	PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`

	// Overlap extends each poll's window backwards past the previous poll so a
	// slipped or delayed cycle leaves no gap; the deduper suppresses the
	// resulting re-fetches. Default 5 minutes.
	Overlap time.Duration `json:"overlap" yaml:"overlap"`

	// InitialLookback, when > 0, makes the first poll reach back this far
	// (capped at 30 days) to backfill recent changes on startup. Default 0:
	// collection starts from the moment the adapter launches.
	InitialLookback time.Duration `json:"initial_lookback" yaml:"initial_lookback"`

	// DedupeTTL is how long a change's identity is remembered to suppress
	// re-shipping it across overlapping windows. Default 7 days.
	DedupeTTL time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

	// Retry tuning for transient API failures.
	RetryBaseDelay   time.Duration `json:"retry_base_delay" yaml:"retry_base_delay"`
	MaxRetryDelay    time.Duration `json:"max_retry_delay" yaml:"max_retry_delay"`
	MaxRetryAttempts int           `json:"max_retry_attempts" yaml:"max_retry_attempts"`

	// Deduper, when set, replaces the built-in in-memory deduper. Not settable
	// through a config file; it is a seam for tests and embedders.
	Deduper utils.Deduper `json:"-" yaml:"-"`
}

func (c *QuickBooksConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.RefreshToken == "" {
		return errors.New("missing refresh_token")
	}
	if strings.TrimSpace(c.RealmID) == "" {
		return errors.New("missing realm_id")
	}

	if len(c.Entities) == 0 {
		c.Entities = append([]string(nil), defaultEntities...)
	} else {
		cleaned := c.Entities[:0]
		seen := map[string]struct{}{}
		for _, e := range c.Entities {
			e = strings.TrimSpace(e)
			if e == "" {
				continue
			}
			if _, ok := seen[e]; ok {
				continue
			}
			seen[e] = struct{}{}
			cleaned = append(cleaned, e)
		}
		if len(cleaned) == 0 {
			return errors.New("entities is set but contains no usable entity names")
		}
		c.Entities = cleaned
	}

	if c.MinorVersion == "" {
		c.MinorVersion = defaultMinorVersion
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.Overlap < 0 {
		c.Overlap = 0
	}
	if c.Overlap == 0 {
		c.Overlap = defaultOverlap
	}
	if c.InitialLookback > cdcMaxLookback {
		c.InitialLookback = cdcMaxLookback
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

// QuickBooksAdapter polls QuickBooks Online CDC and ships changed entities to
// LimaCharlie.
type QuickBooksAdapter struct {
	conf        QuickBooksConfig
	uspClient   uspSink
	client      *QuickBooksClient
	deduper     utils.Deduper
	ownsDeduper bool

	// since is the high-water mark of the change window. It advances to the
	// poll's end time after every successful poll.
	since time.Time

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewQuickBooksAdapter creates a QuickBooks adapter wired to LimaCharlie.
func NewQuickBooksAdapter(ctx context.Context, conf QuickBooksConfig) (*QuickBooksAdapter, chan struct{}, error) {
	return newQuickBooksAdapter(ctx, conf, nil)
}

// newQuickBooksAdapter is the implementation behind NewQuickBooksAdapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newQuickBooksAdapter(ctx context.Context, conf QuickBooksConfig, sink uspSink) (*QuickBooksAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &QuickBooksAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	now := time.Now()
	a.since = now
	if conf.InitialLookback > 0 {
		a.since = now.Add(-conf.InitialLookback)
	}

	// CDC re-reports a change for the whole overlap window, so a deduper is
	// required to ship each change exactly once.
	a.deduper = conf.Deduper
	if a.deduper == nil {
		window := dedupeBucketWindow
		if window > conf.DedupeTTL {
			window = conf.DedupeTTL
		}
		deduper, err := utils.NewLocalDeduper(window, conf.DedupeTTL)
		if err != nil {
			return nil, nil, fmt.Errorf("quickbooks: deduper: %v", err)
		}
		a.deduper = deduper
		a.ownsDeduper = true
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			if a.ownsDeduper {
				a.deduper.Close()
			}
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.client = NewQuickBooksClient(
		resolveBaseURL(conf),
		resolveTokenURL(conf),
		conf.ClientID,
		conf.ClientSecret,
		conf.RealmID,
		conf.MinorVersion,
		conf.RefreshToken,
	)
	// Surface a rotated refresh token so an operator can update their stored
	// config; the value itself is a secret, so only its rotation is logged.
	a.client.onRefreshToken = func(string) {
		a.conf.ClientOptions.OnWarning("quickbooks: Intuit rotated the refresh token; " +
			"the new value is held in memory for this run. Persist it from your OAuth " +
			"store -- the value configured here remains valid until its own expiry.")
	}

	a.chStopped = make(chan struct{})
	a.wgSenders.Add(1)
	go a.run()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

// Close stops the adapter. It is idempotent.
func (a *QuickBooksAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("quickbooks: closing")
		a.doStop.Set()
		a.wgSenders.Wait()
		err1 := a.uspClient.Drain(1 * time.Minute)
		_, err2 := a.uspClient.Close()
		a.client.Close()
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

// resolveBaseURL computes the API root from the config.
func resolveBaseURL(conf QuickBooksConfig) string {
	if conf.BaseURL != "" {
		return strings.TrimRight(conf.BaseURL, "/")
	}
	if conf.Sandbox {
		return sandboxBaseURL
	}
	return prodBaseURL
}

// resolveTokenURL computes the OAuth token endpoint from the config.
func resolveTokenURL(conf QuickBooksConfig) string {
	if conf.TokenURL != "" {
		return conf.TokenURL
	}
	return tokenEndpoint
}

// run polls QuickBooks CDC forever, until the adapter is asked to stop.
func (a *QuickBooksAdapter) run() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("quickbooks: collection stopped")

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		isFirstRun = false
		a.poll()
	}
}

// poll performs one CDC request over the rolling window and ships every changed
// entity that has not been seen before. On success the window's high-water mark
// advances; on failure it does not, so the next poll re-covers the same range.
func (a *QuickBooksAdapter) poll() {
	end := time.Now()
	start := a.since.Add(-a.conf.Overlap)
	// Never reach past CDC's 30-day horizon.
	if floor := end.Add(-cdcMaxLookback); start.Before(floor) {
		start = floor
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"quickbooks: change window clamped to CDC's 30-day limit (since %s); "+
				"changes older than that cannot be retrieved", start.UTC().Format(time.RFC3339)))
	}

	entities := strings.Join(a.conf.Entities, ",")
	raw, ok := a.fetch(entities, start)
	if !ok {
		return
	}

	changes, err := parseCDCResponse(raw)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("quickbooks: response parse error: %v", err))
		return
	}
	if len(changes) >= cdcObjectCap {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"quickbooks: CDC returned %d objects, at or above the %d-object cap -- some "+
				"changes in this window may have been dropped. Shorten poll_interval or "+
				"narrow entities.", len(changes), cdcObjectCap))
	}

	nShipped := 0
	for _, ch := range changes {
		if a.doStop.IsSet() {
			return
		}
		if a.deduper.CheckAndAdd(a.dedupeKey(ch)) {
			continue
		}
		if !a.ship(ch) {
			return
		}
		nShipped++
	}

	// Only advance the high-water mark once the poll fully succeeded.
	a.since = end
	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"quickbooks: poll complete (changed=%d shipped=%d) up to %s",
		len(changes), nShipped, end.UTC().Format(time.RFC3339)))
}

// fetch performs one CDC request, retrying transient failures with exponential
// backoff. The bool is false when the poll should be abandoned (a permanent
// error, or the adapter is stopping).
func (a *QuickBooksAdapter) fetch(entities string, changedSince time.Time) ([]byte, bool) {
	var raw []byte
	var err error
	for attempt := 0; attempt < a.conf.MaxRetryAttempts; attempt++ {
		if a.doStop.IsSet() {
			return nil, false
		}

		raw, err = a.client.GetCDC(a.ctx, entities, changedSince)
		if err == nil {
			return raw, true
		}

		if !isTransientError(err) {
			a.conf.ClientOptions.OnError(fmt.Errorf("quickbooks: CDC request failed: %v", err))
			// A credential problem needs operator attention, so stop the adapter
			// rather than spin uselessly every interval. This covers a permanently
			// failing token refresh (e.g. a revoked/expired refresh token, bad
			// client_id/secret -> 400 invalid_grant), and a CDC call that still
			// returns 401/403 after a forced token refresh (wrong scope/realm).
			// Other permanent errors (e.g. a 400 for a bad entity name) abandon
			// this poll but keep the adapter alive to retry on the next interval.
			var tokenErr *TokenError
			var httpErr *HTTPError
			if errors.As(err, &tokenErr) ||
				(errors.As(err, &httpErr) &&
					(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden)) {
				a.conf.ClientOptions.OnError(fmt.Errorf(
					"quickbooks: credentials rejected. Verify client_id/client_secret, that the "+
						"refresh_token is still valid and authorized for realm_id %q, and that the "+
						"app has the com.intuit.quickbooks.accounting scope.", a.conf.RealmID))
				a.doStop.Set()
			}
			return nil, false
		}

		if attempt+1 >= a.conf.MaxRetryAttempts {
			break
		}
		delay := a.conf.RetryBaseDelay * time.Duration(1<<attempt)
		if delay > a.conf.MaxRetryDelay {
			delay = a.conf.MaxRetryDelay
		}
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"quickbooks: transient error (attempt %d/%d), retrying in %v: %v",
			attempt+1, a.conf.MaxRetryAttempts, delay, err))
		if a.doStop.WaitFor(delay) {
			return nil, false
		}
	}
	a.conf.ClientOptions.OnError(fmt.Errorf(
		"quickbooks: CDC request failed after %d attempts: %v", a.conf.MaxRetryAttempts, err))
	return nil, false
}

// ship forwards a single change to LimaCharlie. It returns false if the adapter
// should stop (an unrecoverable shipping error).
func (a *QuickBooksAdapter) ship(ch cdcChange) bool {
	msg := &protocol.DataMessage{
		JsonPayload: ch.entity,
		EventType:   ch.entityType,
		TimestampMs: a.eventTime(ch),
	}
	if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("quickbooks: stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("quickbooks: Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// eventTime extracts the change's event time from MetaData.LastUpdatedTime,
// falling back to now when it is absent or unparseable.
func (a *QuickBooksAdapter) eventTime(ch cdcChange) uint64 {
	if raw := ch.entity.FindOneString("MetaData/LastUpdatedTime"); raw != "" {
		if t, err := time.Parse(time.RFC3339, strings.TrimSpace(raw)); err == nil {
			return uint64(t.UnixMilli())
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf(
			"quickbooks: %s/%s unparseable LastUpdatedTime %q", ch.entityType, ch.id(), raw))
	}
	return uint64(time.Now().UnixMilli())
}

// dedupeKey returns a stable deduplication key for a change. CDC re-reports the
// same change across overlapping windows; keying on entity type + id + the
// last-updated time ships each distinct revision exactly once while still
// emitting a new event when the same object changes again.
func (a *QuickBooksAdapter) dedupeKey(ch cdcChange) string {
	lut := ch.entity.FindOneString("MetaData/LastUpdatedTime")
	rev := lut
	if rev == "" {
		// Fall back to the SyncToken (bumped on every write) then a content
		// hash, so a change with no timestamp still ships at most once.
		rev = ch.entity.FindOneString("SyncToken")
	}
	if rev == "" {
		b, _ := json.Marshal(ch.entity)
		sum := sha256.Sum256(b)
		rev = "sha256:" + hex.EncodeToString(sum[:])
	}
	return ch.entityType + "|" + ch.id() + "|" + rev
}

// cdcChange is one entity that changed, paired with its entity type.
type cdcChange struct {
	entityType string
	entity     utils.Dict
}

func (c cdcChange) id() string {
	return c.entity.FindOneString("Id")
}

// cdcEnvelope mirrors the top-level shape of a CDC response.
//
//	{ "CDCResponse": [ { "QueryResponse": [ { "<Entity>": [...], ... } ] } ],
//	  "time": "..." }
//
// A request error instead returns a Fault object, which we surface as an error.
type cdcEnvelope struct {
	CDCResponse []struct {
		QueryResponse []map[string]json.RawMessage `json:"QueryResponse"`
	} `json:"CDCResponse"`
	Fault json.RawMessage `json:"Fault"`
	Time  string          `json:"time"`
}

// parseCDCResponse flattens a CDC response into a deterministic slice of
// changes. Entity blocks are grouped under each QueryResponse element keyed by
// entity type; non-array envelope fields (startPosition/maxResults) are
// skipped. Records are returned in a stable order (entity type, then their
// order within the block) so polls are reproducible.
func parseCDCResponse(raw []byte) ([]cdcChange, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, nil
	}

	var env cdcEnvelope
	if err := json.Unmarshal([]byte(trimmed), &env); err != nil {
		return nil, fmt.Errorf("invalid CDC JSON: %v", err)
	}
	if len(env.Fault) > 0 {
		return nil, fmt.Errorf("QuickBooks API fault: %s", string(env.Fault))
	}

	var changes []cdcChange
	for _, cdc := range env.CDCResponse {
		for _, block := range cdc.QueryResponse {
			// Sort entity keys for a deterministic emission order.
			keys := make([]string, 0, len(block))
			for k := range block {
				if _, skip := envelopeKeys[k]; skip {
					continue
				}
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, entityType := range keys {
				var arr []json.RawMessage
				if err := json.Unmarshal(block[entityType], &arr); err != nil {
					// Not an array (e.g. a scalar count field we don't know);
					// skip rather than fail the whole response.
					continue
				}
				for _, r := range arr {
					m, err := utils.UnmarshalCleanJSON(string(r))
					if err != nil || len(m) == 0 {
						continue
					}
					changes = append(changes, cdcChange{
						entityType: entityType,
						entity:     utils.Dict(m),
					})
				}
			}
		}
	}
	return changes, nil
}
