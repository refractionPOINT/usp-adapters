package usp_ms_graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const scope = "https://graph.microsoft.com/.default"
const URLPrefix = "https://graph.microsoft.com/v1.0/"
const defaultLoginEndpoint = "https://login.microsoftonline.com"
const defaultPollInterval = 30 * time.Second
const defaultTimestampField = "createdDateTime"

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type MsGraphAdapter struct {
	conf       MsGraphConfig
	uspClient  uspSink
	httpClient *http.Client

	endpoint      string
	loginEndpoint string
	pollInterval  time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

type MsGraphConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	URL           string                  `json:"url" yaml:"url"`

	// LoginEndpoint overrides the Azure AD authority used for the OAuth2
	// client_credentials token exchange. Empty means the public cloud
	// endpoint (https://login.microsoftonline.com).
	LoginEndpoint string `json:"login_endpoint" yaml:"login_endpoint"`

	// GraphEndpoint overrides the Microsoft Graph API root the relative URL
	// is appended to. Empty means the public cloud v1.0 root
	// (https://graph.microsoft.com/v1.0/).
	GraphEndpoint string `json:"graph_endpoint" yaml:"graph_endpoint"`

	// TimestampField is the datetime property the polled collection is
	// filtered and cursored on. Empty means "createdDateTime", which fits
	// collections like auditLogs/signIns or security/alerts; collections such
	// as auditLogs/directoryAudits use "activityDateTime" instead.
	TimestampField string `json:"timestamp_field,omitempty" yaml:"timestamp_field,omitempty"`

	// PollInterval is the wait between polls of the Graph endpoint. It is
	// not settable through a config file; it exists as a seam for tests.
	PollInterval time.Duration `json:"-" yaml:"-"`
}

func (c *MsGraphConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	return nil
}

func NewMsGraphAdapter(ctx context.Context, conf MsGraphConfig) (*MsGraphAdapter, chan struct{}, error) {
	return newMsGraphAdapter(ctx, conf, nil)
}

// newMsGraphAdapter is the implementation behind NewMsGraphAdapter. When sink
// is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newMsGraphAdapter(ctx context.Context, conf MsGraphConfig, sink uspSink) (*MsGraphAdapter, chan struct{}, error) {
	a := &MsGraphAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	a.ctx, a.cancel = context.WithCancel(ctx)

	a.loginEndpoint = strings.TrimSuffix(conf.LoginEndpoint, "/")
	if a.loginEndpoint == "" {
		a.loginEndpoint = defaultLoginEndpoint
	}
	a.pollInterval = conf.PollInterval
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			a.cancel()
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.httpClient = &http.Client{
		Timeout: 10 * time.Second,
		// Refuse redirects before either bearer tokens or client credentials can
		// be forwarded. Continuations are validated separately against the origin.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	a.chStopped = make(chan struct{})

	graphRoot := a.conf.GraphEndpoint
	if graphRoot == "" {
		graphRoot = URLPrefix
	}
	if !strings.HasSuffix(graphRoot, "/") {
		graphRoot += "/"
	}

	// Strip the / prefix if in the URL.
	url := strings.TrimPrefix(a.conf.URL, "/")

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch alerts from %s", graphRoot+url))

	a.wgSenders.Add(1)
	go a.fetchEvents(graphRoot + url)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *MsGraphAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.cancel()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *MsGraphAdapter) fetchToken() (string, error) {

	tokenURL := fmt.Sprintf("%s/%s/oauth2/v2.0/token", a.loginEndpoint, a.conf.TenantID)
	payload := url.Values{"client_id": {a.conf.ClientID}, "scope": {scope}, "grant_type": {"client_credentials"}, "client_secret": {a.conf.ClientSecret}}.Encode()

	req, err := http.NewRequestWithContext(a.ctx, "POST", tokenURL, bytes.NewBufferString(payload))
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint returned HTTP %d", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("no bearer token returned: %s", err)
	}

	accessToken, ok := result["access_token"].(string)
	if !ok || accessToken == "" {
		return "", errors.New("no bearer token returned")
	}

	return accessToken, nil

}

// graphCursor is committed only after every page has been shipped. The inclusive
// boundary retains all IDs at the maximum parsed instant, independent of order.
type graphCursor struct {
	since time.Time
	ids   map[string]bool
}

type graphRecordKey struct {
	id      string
	instant time.Time
}

func (a *MsGraphAdapter) fetchEvents(eventsURL string) {
	defer a.wgSenders.Done()
	cursor := graphCursor{since: time.Now().UTC(), ids: map[string]bool{}}
	// Retain successful shipments while retrying an incomplete scan, including
	// older records that would otherwise be skipped by a premature watermark.
	delivered := map[graphRecordKey]bool{}
	for {
		timer := time.NewTimer(a.pollInterval)
		select {
		case <-a.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		next, err := a.poll(eventsURL, cursor, delivered)
		if err != nil {
			if a.ctx.Err() != nil || errors.Is(err, uspclient.ErrorClosed) {
				return
			}
			a.conf.ClientOptions.OnError(err)
			continue
		}
		cursor = next
		delivered = map[graphRecordKey]bool{}
	}
}

func (a *MsGraphAdapter) timestampField() string {
	if a.conf.TimestampField != "" {
		return a.conf.TimestampField
	}
	return defaultTimestampField
}

// poll streams pages, but keeps the committed watermark unchanged on any
// request, decoding, validation or shipping failure. delivered is retry state.
func (a *MsGraphAdapter) poll(eventsURL string, cursor graphCursor, delivered map[graphRecordKey]bool) (graphCursor, error) {
	origin, err := url.Parse(eventsURL)
	if err != nil {
		return cursor, err
	}
	if origin.Host == "" || origin.User != nil || (origin.Scheme != "https" && origin.Scheme != "http") {
		return cursor, errors.New("invalid Graph endpoint")
	}
	query, err := url.ParseQuery(origin.RawQuery)
	if err != nil {
		return cursor, errors.New("invalid Graph query")
	}
	filter := a.timestampField() + " ge " + cursor.since.UTC().Format(time.RFC3339Nano)
	if existing := query.Get("$filter"); existing != "" {
		filter = "(" + existing + ") and (" + filter + ")"
	}
	query.Set("$filter", filter)
	first := *origin
	first.RawQuery = query.Encode()
	current := &first
	visited := map[string]bool{}
	token := ""
	candidate := graphCursor{since: cursor.since, ids: map[string]bool{}}
	for id := range cursor.ids {
		candidate.ids[id] = true
	}
	for {
		if a.ctx.Err() != nil {
			return cursor, a.ctx.Err()
		}
		if visited[current.String()] {
			return cursor, errors.New("Graph continuation cycle")
		}
		visited[current.String()] = true
		body, err := a.requestPage(current.String(), &token)
		if err != nil {
			return cursor, err
		}
		var page struct {
			Value json.RawMessage `json:"value"`
			Next  string          `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return cursor, errors.New("invalid Graph page JSON")
		}
		var records []map[string]interface{}
		if len(page.Value) == 0 || string(page.Value) == "null" {
			return cursor, errors.New("missing Graph value array")
		}
		if err := json.Unmarshal(page.Value, &records); err != nil {
			return cursor, errors.New("invalid Graph value array")
		}
		for _, record := range records {
			id, _ := record["id"].(string)
			stamp, _ := record[a.timestampField()].(string)
			instant, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil || id == "" {
				return cursor, errors.New("Graph record requires an ID and valid timestamp")
			}
			instant = instant.UTC()
			if instant.Before(cursor.since) {
				continue
			}
			key := graphRecordKey{id: id, instant: instant}
			if !(instant.Equal(cursor.since) && cursor.ids[id]) && !delivered[key] {
				if a.ctx.Err() != nil {
					return cursor, a.ctx.Err()
				}
				msg := &protocol.DataMessage{JsonPayload: record, TimestampMs: uint64(time.Now().UnixMilli())}
				err = a.uspClient.Ship(msg, 10*time.Second)
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, time.Hour)
				}
				if err != nil {
					return cursor, fmt.Errorf("Ship(): %w", err)
				}
				delivered[key] = true
			}
			if instant.After(candidate.since) {
				candidate.since = instant
				candidate.ids = map[string]bool{}
			}
			if instant.Equal(candidate.since) {
				candidate.ids[id] = true
			}
		}
		if page.Next == "" {
			return candidate, nil
		}
		link, err := url.Parse(page.Next)
		if err != nil {
			return cursor, errors.New("invalid Graph continuation")
		}
		link = current.ResolveReference(link)
		if !strings.EqualFold(link.Scheme, origin.Scheme) || !strings.EqualFold(link.Host, origin.Host) || link.User != nil || link.Fragment != "" {
			return cursor, errors.New("Graph continuation must use the configured origin")
		}
		// nextLink is opaque: never reconstruct its query or add the filter again.
		current = link
	}
}

func (a *MsGraphAdapter) requestPage(pageURL string, token *string) ([]byte, error) {
	for attempt := 1; attempt <= 3; attempt++ {
		var err error
		if *token == "" {
			*token, err = a.fetchToken()
		}
		if err != nil {
			if attempt == 3 {
				return nil, fmt.Errorf("error fetching token after 3 attempts: %w", err)
			}
		} else {
			req, err := http.NewRequestWithContext(a.ctx, "GET", pageURL, nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+*token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := a.httpClient.Do(req)
			if err != nil {
				return nil, errors.New("Graph request failed")
			}
			body, readErr := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, errors.New("error reading Graph response")
			}
			if resp.StatusCode == http.StatusOK {
				return body, nil
			}
			if (resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusGatewayTimeout) || attempt == 3 {
				return nil, fmt.Errorf("Microsoft API returned HTTP %d; verify permissions and Microsoft API status", resp.StatusCode)
			}
		}
		// Cancellable backoff also bounds shutdown during authentication failures.
		timer := time.NewTimer(time.Duration(attempt) * time.Second)
		select {
		case <-a.ctx.Done():
			timer.Stop()
			return nil, a.ctx.Err()
		case <-timer.C:
		}
	}
	return nil, errors.New("Graph retries exhausted")
}
