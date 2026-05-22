package usp_halopsa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// The HaloPSA adapter is intentionally generic: it polls any HaloPSA REST list
// endpoint and ships every record into LimaCharlie in its original JSON shape.
// The default configuration targets the audit log (the "/Audit" endpoint), but
// pointing it at another endpoint (Tickets, Actions, ...) only requires
// changing configuration, not code.
//
// HaloPSA reference:
//   - Authentication (OAuth2 client credentials):
//     https://haloacademy.halopsa.com/apidoc/authentication/client
//   - API reference / Swagger:
//     https://haloacademy.halopsa.com/apidoc

const (
	defaultEndpoint     = "Audit"
	defaultIDField      = "id"
	defaultScope        = "all"
	defaultPageSize     = 100
	defaultPollInterval = 60 // seconds
	defaultTokenTTL     = 3600

	recordCountField = "record_count"
	maxErrBodyLen    = 512
)

// HaloPSAConfig holds the configuration for one HaloPSA adapter instance.
type HaloPSAConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// InstanceURL is the base URL of the HaloPSA instance, e.g.
	// "https://example.halopsa.com". The authorisation server and the
	// resource (API) server are derived from it unless overridden below.
	InstanceURL string `json:"instance_url" yaml:"instance_url"`
	// AuthURL optionally overrides the authorisation server URL (the part
	// before "/token"). Defaults to "<instance_url>/auth".
	AuthURL string `json:"auth_url" yaml:"auth_url"`
	// APIURL optionally overrides the resource (API) server URL.
	// Defaults to "<instance_url>/api".
	APIURL string `json:"api_url" yaml:"api_url"`

	// ClientID and ClientSecret identify the HaloPSA API application
	// (configured under Configuration > Integrations > HaloPSA API).
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"client_secret" yaml:"client_secret"`
	// Tenant is the optional tenant identifier required by some hosted
	// HaloPSA deployments. Leave empty for self-hosted instances.
	Tenant string `json:"tenant" yaml:"tenant"`
	// Scope is the OAuth2 scope requested for the token. Defaults to "all".
	Scope string `json:"scope" yaml:"scope"`

	// Endpoint is the API resource to poll, e.g. "Audit" (the default),
	// "Tickets", "Actions", etc. Any list endpoint exposing a monotonically
	// increasing integer id can be ingested.
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	// DataField is the response field holding the array of records. When
	// empty (the default) the first array-valued field is auto-detected.
	DataField string `json:"data_field" yaml:"data_field"`
	// IDField is the record field used as the incremental cursor. It must be
	// a monotonically increasing integer. Defaults to "id".
	IDField string `json:"id_field" yaml:"id_field"`
	// ExtraParams are additional static query string parameters added to
	// every request (e.g. {"excludesys": "true"}). They override the
	// adapter's own defaults, so ordering/filtering can be tuned per
	// endpoint without code changes.
	ExtraParams map[string]string `json:"extra_params" yaml:"extra_params"`

	// PageSize is the number of records requested per page. Defaults to 100.
	PageSize int `json:"page_size" yaml:"page_size"`
	// PollInterval is the delay in seconds between polls. Defaults to 60.
	PollInterval int `json:"poll_interval" yaml:"poll_interval"`
}

func (c *HaloPSAConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.InstanceURL == "" && c.AuthURL == "" {
		return errors.New("missing instance_url (or auth_url)")
	}
	if c.InstanceURL == "" && c.APIURL == "" {
		return errors.New("missing instance_url (or api_url)")
	}
	return nil
}

func trimURL(u string) string {
	return strings.TrimRight(strings.TrimSpace(u), "/")
}

// tokenURL returns the full OAuth2 token endpoint, including the optional
// tenant query parameter.
func (c *HaloPSAConfig) tokenURL() string {
	authBase := trimURL(c.AuthURL)
	if authBase == "" {
		authBase = trimURL(c.InstanceURL) + "/auth"
	}
	u := authBase + "/token"
	if c.Tenant != "" {
		u += "?tenant=" + url.QueryEscape(c.Tenant)
	}
	return u
}

// resourceBaseURL returns the resource (API) server base URL, with no
// trailing slash.
func (c *HaloPSAConfig) resourceBaseURL() string {
	apiBase := trimURL(c.APIURL)
	if apiBase == "" {
		apiBase = trimURL(c.InstanceURL) + "/api"
	}
	return apiBase
}

type HaloPSAAdapter struct {
	conf       HaloPSAConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	accessToken string
	tokenExpiry time.Time

	// initialized is false until the baseline cursor has been established.
	initialized bool
	// lastMaxID is the incremental cursor: the highest id already shipped.
	lastMaxID uint64
}

func NewHaloPSAAdapter(ctx context.Context, conf HaloPSAConfig) (*HaloPSAAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	// Apply defaults.
	if conf.Endpoint == "" {
		conf.Endpoint = defaultEndpoint
	}
	if conf.IDField == "" {
		conf.IDField = defaultIDField
	}
	if conf.Scope == "" {
		conf.Scope = defaultScope
	}
	if conf.PageSize <= 0 {
		conf.PageSize = defaultPageSize
	}
	if conf.PollInterval <= 0 {
		conf.PollInterval = defaultPollInterval
	}

	a := &HaloPSAAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("halopsa: polling %s/%s every %ds",
		a.conf.resourceBaseURL(), strings.Trim(a.conf.Endpoint, "/"), a.conf.PollInterval))

	a.wgSenders.Add(1)
	go a.fetchEvents()

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *HaloPSAAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("halopsa: closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}
	return err2
}

func (a *HaloPSAAdapter) fetchEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("halopsa: event fetch loop exiting")

	interval := time.Duration(a.conf.PollInterval) * time.Second

	// The first poll establishes the baseline cursor without shipping, so
	// the adapter starts from "now" rather than replaying the full history.
	if items, err := a.poll(); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("halopsa: initial poll: %v", err))
	} else {
		a.shipItems(items)
	}

	for !a.doStop.WaitFor(interval) {
		items, err := a.poll()
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("halopsa: poll: %v", err))
			continue
		}
		if !a.shipItems(items) {
			return
		}
	}
}

// poll fetches the new records since the last successful poll. On the very
// first call it only establishes the baseline cursor and returns no records.
func (a *HaloPSAAdapter) poll() ([]utils.Dict, error) {
	token, err := a.getToken()
	if err != nil {
		return nil, err
	}

	fetchPage := func(pageNo int) ([]utils.Dict, int, error) {
		return a.requestPage(token, pageNo)
	}

	if !a.initialized {
		items, _, err := fetchPage(1)
		if err != nil {
			return nil, err
		}
		a.lastMaxID = maxID(items, a.conf.IDField)
		a.initialized = true
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("halopsa: baseline established at %s=%d",
			a.conf.IDField, a.lastMaxID))
		return nil, nil
	}

	items, newMax, err := collectNewItems(fetchPage, a.conf.IDField, a.lastMaxID, a.doStop.IsSet)
	if err != nil {
		return nil, err
	}
	a.lastMaxID = newMax
	if len(items) > 0 {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("halopsa: collected %d new records (cursor %s=%d)",
			len(items), a.conf.IDField, newMax))
	}
	return items, nil
}

// shipItems ships every item to LimaCharlie. It returns false if a fatal
// error occurred and the fetch loop should terminate.
func (a *HaloPSAAdapter) shipItems(items []utils.Dict) bool {
	for _, item := range items {
		msg := &protocol.DataMessage{
			JsonPayload: item,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
		err := a.uspClient.Ship(msg, 10*time.Second)
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			a.doStop.Set()
			return false
		}
	}
	return true
}

// getToken returns a valid OAuth2 access token, fetching a new one when the
// cached token is missing or about to expire.
func (a *HaloPSAAdapter) getToken() (string, error) {
	if a.accessToken != "" && time.Until(a.tokenExpiry) > time.Minute {
		return a.accessToken, nil
	}

	a.conf.ClientOptions.DebugLog("halopsa: fetching access token")

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", a.conf.ClientID)
	form.Set("client_secret", a.conf.ClientSecret)
	form.Set("scope", a.conf.Scope)

	req, err := http.NewRequest("POST", a.conf.tokenURL(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint %s: %s", resp.Status, truncate(body))
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", fmt.Errorf("token response: %v", err)
	}
	if tok.AccessToken == "" {
		return "", errors.New("token response missing access_token")
	}

	expiresIn := tok.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = defaultTokenTTL
	}
	a.accessToken = tok.AccessToken
	a.tokenExpiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
	return a.accessToken, nil
}

// requestPage fetches a single page of records from the configured endpoint.
func (a *HaloPSAAdapter) requestPage(token string, pageNo int) ([]utils.Dict, int, error) {
	reqURL := a.conf.resourceBaseURL() + "/" + strings.Trim(a.conf.Endpoint, "/")

	q := url.Values{}
	q.Set("pageinate", "true")
	q.Set("page_size", strconv.Itoa(a.conf.PageSize))
	q.Set("page_no", strconv.Itoa(pageNo))
	// Newest-first ordering lets the incremental poll stop as soon as it
	// reaches a record it has already seen.
	q.Set("order", a.conf.IDField)
	q.Set("orderdesc", "true")
	// Caller-supplied parameters win over the adapter's defaults.
	for k, v := range a.conf.ExtraParams {
		q.Set(k, v)
	}

	req, err := http.NewRequest("GET", reqURL+"?"+q.Encode(), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("reading response: %v", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		// Force a token refresh on the next poll.
		a.accessToken = ""
		return nil, 0, fmt.Errorf("halopsa api 401 unauthorized: %s", truncate(body))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("halopsa api %s: %s", resp.Status, truncate(body))
	}

	return parseListResponse(body, a.conf.DataField)
}

// parseListResponse extracts the array of records and the total record count
// from a HaloPSA list response. HaloPSA list endpoints normally return an
// envelope of the form {"record_count": N, "<resource>": [...]}, but some
// return a bare JSON array, so both shapes are supported.
func parseListResponse(body []byte, dataField string) ([]utils.Dict, int, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, 0, nil
	}

	if trimmed[0] == '[' {
		var arr []utils.Dict
		if err := json.Unmarshal(trimmed, &arr); err != nil {
			return nil, 0, fmt.Errorf("invalid json array: %v", err)
		}
		return arr, len(arr), nil
	}

	var resp utils.Dict
	if err := json.Unmarshal(trimmed, &resp); err != nil {
		return nil, 0, fmt.Errorf("invalid json: %v", err)
	}
	recordCount := int(resp.FindOneInt(recordCountField))
	return extractItems(resp, dataField), recordCount, nil
}

// extractItems returns the array of records from a HaloPSA list envelope.
// When dataField is set it is used directly; otherwise the first array-valued
// field is used, which makes the adapter work against any list endpoint
// without per-endpoint configuration.
func extractItems(resp utils.Dict, dataField string) []utils.Dict {
	if dataField != "" {
		items, _ := resp.GetListOfDict(dataField)
		return items
	}
	keys := resp.Keys()
	sort.Strings(keys)
	for _, k := range keys {
		if k == recordCountField {
			continue
		}
		if items, ok := resp.GetListOfDict(k); ok {
			return items
		}
	}
	return nil
}

// collectNewItems walks the endpoint pages (newest-first) via fetchPage and
// returns every record whose id is greater than lastMaxID, ordered oldest
// first. It also returns the new high-water-mark id to use as the next cursor.
//
// In steady state this only fetches the first page, since it stops as soon as
// it reaches an already-seen id. It keeps paging only when catching up after
// downtime.
func collectNewItems(
	fetchPage func(pageNo int) ([]utils.Dict, int, error),
	idField string,
	lastMaxID uint64,
	stop func() bool,
) ([]utils.Dict, uint64, error) {
	var collected []utils.Dict
	seen := map[uint64]struct{}{}
	newMax := lastMaxID
	totalSeen := 0

	for pageNo := 1; ; pageNo++ {
		if stop != nil && stop() {
			break
		}

		items, recordCount, err := fetchPage(pageNo)
		if err != nil {
			return nil, lastMaxID, err
		}
		// An empty page marks the end of the result set.
		if len(items) == 0 {
			break
		}
		totalSeen += len(items)

		reachedKnown := false
		for _, item := range items {
			id, ok := item.GetInt(idField)
			if !ok {
				// No usable cursor id: include the record rather than
				// risk dropping data. Endpoints used with this adapter
				// should expose a monotonic integer id field.
				collected = append(collected, item)
				continue
			}
			if id <= lastMaxID {
				reachedKnown = true
				continue
			}
			if _, dup := seen[id]; dup {
				// Records can shift between pages while new ones are
				// inserted; skip anything already collected this poll.
				continue
			}
			seen[id] = struct{}{}
			collected = append(collected, item)
			if id > newMax {
				newMax = id
			}
		}

		if reachedKnown {
			break
		}
		// Stop once the whole result set has been walked. This is based on
		// the record count rather than the page length so it stays correct
		// even when the server clamps page_size to a lower maximum.
		if recordCount > 0 && totalSeen >= recordCount {
			break
		}
	}

	// Pages are walked newest-first; deliver records oldest-first.
	for i, j := 0, len(collected)-1; i < j; i, j = i+1, j-1 {
		collected[i], collected[j] = collected[j], collected[i]
	}
	return collected, newMax, nil
}

// maxID returns the highest id found among items, or 0 if none have one.
func maxID(items []utils.Dict, idField string) uint64 {
	var m uint64
	for _, item := range items {
		if id, ok := item.GetInt(idField); ok && id > m {
			m = id
		}
	}
	return m
}

// truncate shortens a response body for safe inclusion in error messages.
func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > maxErrBodyLen {
		return s[:maxErrBodyLen] + "...(truncated)"
	}
	return s
}
