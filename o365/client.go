package usp_o365

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/gjson"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// defaultPollInterval is how long fetchEvents waits between polling cycles
// once all immediately-available pages have been consumed.
const defaultPollInterval = 5 * time.Minute

const (
	// httpTimeout bounds each request to the Management API.
	httpTimeout = 2 * time.Minute

	// defaultRetryBaseDelay and defaultRetryMaxDelay bound the exponential
	// backoff between retries of a request that failed transiently.
	defaultRetryBaseDelay = 1 * time.Second
	defaultRetryMaxDelay  = 5 * time.Minute

	// defaultMaxRetryAfter caps how long a server-provided Retry-After can
	// idle a collection loop.
	defaultMaxRetryAfter = 15 * time.Minute

	// defaultFailureEscalation is how long a collection loop may keep failing
	// with transient errors (throttling, 5xx, network), without downloading
	// any content, before the failure is reported with OnError instead of
	// OnWarning.
	defaultFailureEscalation = 30 * time.Minute

	// defaultContentRetryLimit is how long one content blob is retried within
	// a poll before the adapter moves on to the rest of the page. The blob
	// isn't recorded as done, so the next poll tries it again.
	defaultContentRetryLimit = 10 * time.Minute

	// Bound per-collector bookkeeping even if a listing contains many failures.
	maxLeftBlobs = 4096
)

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type listItem struct {
	ContentType       string `json:"contentType,omitempty"`
	ContentID         string `json:"contentId,omitempty"`
	ContentURI        string `json:"contentUri,omitempty"`
	ContentCreated    string `json:"contentCreated,omitempty"`
	ContentExpiration string `json:"contentExpiration,omitempty"`
}

// Office 365 Management API endpoints for different cloud environments
// Reference: https://docs.microsoft.com/en-us/office/office-365-management-api/office-365-management-apis-overview#office-365-management-apis-overview
var URL = map[string]string{
	"enterprise":   "https://manage.office.com/api/v1.0/",
	"gcc-gov":      "https://manage-gcc.office.com/api/v1.0/",
	"gcc-high-gov": "https://manage.office365.us/api/v1.0/",
	"dod-gov":      "https://manage.protection.apps.mil/api/v1.0/",
}

// OAuth2 token endpoints for different cloud environments
// References:
// - Commercial/GCC: https://docs.microsoft.com/en-us/azure/active-directory/develop/authentication-national-clouds#azure-ad-authentication-endpoints
// - GCC High/DoD: https://docs.microsoft.com/en-us/azure/azure-government/compare-azure-government-global-azure#azure-active-directory-premium-p1-and-p2
// - Government clouds: https://docs.microsoft.com/en-us/graph/deployments#microsoft-graph-and-graph-explorer-service-root-endpoints
var TokenURL = map[string]string{
	"enterprise":   "https://login.windows.net/",
	"gcc-gov":      "https://login.windows.net/",
	"gcc-high-gov": "https://login.microsoftonline.us/",
	"dod-gov":      "https://login.microsoftonline.us/",
}

// Resource scopes for OAuth2 authentication, must match the target API environment
// Reference: https://docs.microsoft.com/en-us/office/office-365-management-api/get-started-with-office-365-management-apis#specify-the-permissions-your-app-requires-to-access-the-office-365-management-apis
var ResourceScope = map[string]string{
	"enterprise":   "https://manage.office.com",
	"gcc-gov":      "https://manage.office.com",
	"gcc-high-gov": "https://manage.office365.us",
	"dod-gov":      "https://manage.protection.apps.mil",
}

type Office365Adapter struct {
	conf       Office365Config
	uspClient  uspSink
	httpClient *http.Client
	transport  *http.Transport

	endpoint     string
	endpointType string

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	// stopCtx is cancelled by Close so that in-flight requests end at once.
	stopCtx    context.Context
	cancelStop context.CancelFunc

	// checker is conf.Deduper when it can check a key without recording it.
	// Content is then recorded as done only once it was shipped.
	checker utils.CheckingDeduper

	ctx context.Context
}

type Office365Config struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Domain        string                  `json:"domain" yaml:"domain"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	PublisherID   string                  `json:"publisher_id" yaml:"publisher_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`
	Endpoint      string                  `json:"endpoint" yaml:"endpoint"`
	ContentTypes  string                  `json:"content_types" yaml:"content_types"`
	StartTime     string                  `json:"start_time" yaml:"start_time"`

	// TokenURL, when set, fully overrides the OAuth2 token endpoint URL. When
	// empty (the default), the token endpoint is derived from Endpoint /
	// Domain / TenantID as usual. It exists for tests and for private cloud
	// deployments fronting their own AAD endpoint.
	TokenURL string `json:"token_url" yaml:"token_url"`

	Deduper utils.Deduper `json:"-" yaml:"-"`

	// PollInterval is how long to wait between polling cycles. It is not
	// settable through a config file; it exists as a seam for tests. Defaults
	// to 5 minutes.
	PollInterval time.Duration `json:"-" yaml:"-"`

	// Retry timing; zero means the default. Tests shorten them.
	retryBaseDelay    time.Duration
	retryMaxDelay     time.Duration
	maxRetryAfter     time.Duration
	failureEscalation time.Duration
	contentRetryLimit time.Duration
}

func (c *Office365Config) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Domain == "" {
		return errors.New("missing domain")
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.PublisherID == "" {
		return errors.New("missing publisher_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.Endpoint == "" {
		return errors.New("missing endpoint")
	}
	_, ok := URL[c.Endpoint]
	if !isURLEndpoint(c.Endpoint) && !ok {
		return fmt.Errorf("invalid endpoint, not https or in %v", URL)
	}
	return nil
}

// isURLEndpoint reports whether the configured endpoint is an explicit URL
// rather than one of the named environments. http is accepted (in addition to
// https) so local mocks/tests can stand in for the Management API.
func isURLEndpoint(endpoint string) bool {
	return strings.HasPrefix(endpoint, "https://") || strings.HasPrefix(endpoint, "http://")
}

func NewOffice365Adapter(ctx context.Context, conf Office365Config) (*Office365Adapter, chan struct{}, error) {
	return newOffice365Adapter(ctx, conf, nil)
}

// newOffice365Adapter is the implementation behind NewOffice365Adapter. When
// sink is non-nil it is used in place of a real LimaCharlie client -- the seam
// tests use to capture shipped events.
func newOffice365Adapter(ctx context.Context, conf Office365Config, sink uspSink) (*Office365Adapter, chan struct{}, error) {
	var err error

	if conf.PollInterval <= 0 {
		conf.PollInterval = defaultPollInterval
	}
	if conf.retryBaseDelay <= 0 {
		conf.retryBaseDelay = defaultRetryBaseDelay
	}
	if conf.retryMaxDelay <= 0 {
		conf.retryMaxDelay = defaultRetryMaxDelay
	}
	if conf.maxRetryAfter <= 0 {
		conf.maxRetryAfter = defaultMaxRetryAfter
	}
	if conf.failureEscalation <= 0 {
		conf.failureEscalation = defaultFailureEscalation
	}
	if conf.contentRetryLimit <= 0 {
		conf.contentRetryLimit = defaultContentRetryLimit
	}

	// If no deduper is provided, use a local one.
	if conf.Deduper == nil {
		conf.Deduper, err = utils.NewLocalDeduper(1*time.Hour, 24*time.Hour)
		if err != nil {
			return nil, nil, err
		}
	}

	a := &Office365Adapter{
		conf:   conf,
		ctx:    context.Background(),
		doStop: utils.NewEvent(),
	}
	a.stopCtx, a.cancelStop = context.WithCancel(context.Background())
	if cd, ok := conf.Deduper.(utils.CheckingDeduper); ok {
		a.checker = cd
	} else {
		a.conf.ClientOptions.DebugLog("deduper can't check without recording: content is recorded as done before it is downloaded")
	}

	if isURLEndpoint(conf.Endpoint) {
		a.endpoint = conf.Endpoint
		a.endpointType = "custom"
	} else if v, ok := URL[conf.Endpoint]; ok {
		a.endpoint = v
		a.endpointType = conf.Endpoint
	} else {
		return nil, nil, fmt.Errorf("not a valid api endpoint: %s", conf.Endpoint)
	}

	if sink != nil {
		a.uspClient = sink
	} else {
		a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
	}

	// Retain proxy settings, idle expiry and TLS defaults. DialContext also
	// lets Close cancel a connection attempt before the dial timeout.
	a.transport = http.DefaultTransport.(*http.Transport).Clone()
	a.transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	a.httpClient = &http.Client{Timeout: httpTimeout, Transport: a.transport}

	a.chStopped = make(chan struct{})

	if err := a.updateBearerToken(); err != nil {
		return nil, nil, err
	}

	// Set sane default for the content types.
	if a.conf.ContentTypes == "" {
		a.conf.ContentTypes = "Audit.AzureActiveDirectory,Audit.Exchange,Audit.SharePoint,Audit.General,DLP.All"
	}

	nCollecting := 0
	for _, ct := range strings.Split(a.conf.ContentTypes, ",") {
		ct = strings.TrimSpace(ct)
		if ct == "" {
			continue
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch %s events", ct))

		url := fmt.Sprintf("%s%s/activity/feed/subscriptions/start?contentType=%s&PublisherIdentifier=%s", a.endpoint, a.conf.TenantID, ct, a.conf.PublisherID)
		sub, err := a.makeOneRegistrationRequest(url)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to register subscription to %s: %v", url, err))
			a.Close()
			return nil, nil, fmt.Errorf("failed to register subscription to %s: %v", url, err)
		}
		if len(sub) != 0 {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("subscription created: %+v", sub))
		}

		nCollecting++

		a.wgSenders.Add(1)
		url = fmt.Sprintf("%s%s/activity/feed/subscriptions/content?contentType=%s&PublisherIdentifier=%s", a.endpoint, a.conf.TenantID, ct, a.conf.PublisherID)
		go a.fetchEvents(url)
	}

	if nCollecting == 0 {
		a.Close()
		return nil, nil, errors.New("no content types specified")
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *Office365Adapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.cancelStop()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	// oauth2.Transport doesn't forward CloseIdleConnections to its base.
	a.transport.CloseIdleConnections()

	a.conf.Deduper.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *Office365Adapter) fetchEvents(url string) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("fetching of events exiting")

	fs := &failureState{}
	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.PollInterval) {
		now := time.Now().UTC()
		start := a.conf.StartTime
		if !isFirstRun || start == "" {
			start = now.Add(-3 * time.Hour).Format("2006-01-02T15:04:05")
		}
		end := now.Format("2006-01-02T15:04:05")
		isFirstRun = false

		if a.pollOnce(fmt.Sprintf("%s&startTime=%s&endTime=%s", url, start, end), fs) {
			fs.reset()
		}
	}
}

// pollOnce lists every page of the window and ships the content of every blob
// not already done. It returns false when transient failures remain, so only
// those failures keep the continuous-outage clock running across polls.
func (a *Office365Adapter) pollOnce(pageURL string, fs *failureState) bool {
	a.expireLeftBlobs(fs, time.Now())
	complete := true
	for pageURL != "" {
		body, header, err := a.getWithRetry(pageURL, "list api", fs, 0)
		if err != nil {
			if err != errStopped {
				a.conf.ClientOptions.OnError(fmt.Errorf("office365 list api: %v", err))
			}
			return false
		}

		items := []listItem{}
		if err := json.Unmarshal(body, &items); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 list api invalid json: %v", err))
			return false
		}
		pageURL = header.Get("NextPageUri")

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("listed %d events (with page: %v)", len(items), pageURL != ""))

		pageComplete, abort := a.processItems(items, fs)
		if abort {
			return false
		}
		complete = complete && pageComplete
	}
	return complete
}

// processItems downloads and ships the content blobs of one listing page. With
// a CheckingDeduper, a blob is recorded as done only once all of its events
// were shipped, so a blob interrupted by an error or by Close is fetched again
// by the next poll, or by the next instance sharing the deduper. complete is
// false when transient retries were exhausted; abort is true when the poll
// must stop. Other unprocessed blobs have a separate failure clock.
func (a *Office365Adapter) processItems(items []listItem, fs *failureState) (complete bool, abort bool) {
	nFetched := 0
	nShipped := 0
	nSkipped := 0
	nLeft := 0
	transientLeft := false
	defer func() {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetched %d, shipped %d: skipped: %d left unprocessed: %d", nFetched, nShipped, nSkipped, nLeft))
	}()

	for _, item := range items {
		if a.doStop.IsSet() {
			return false, true
		}
		if a.isDone(item.ContentID) {
			delete(fs.leftBlobs, item.ContentID)
			nSkipped++
			continue
		}

		content, _, err := a.getWithRetry(item.ContentURI, "content api", fs, a.conf.contentRetryLimit)
		if err != nil {
			if err == errStopped {
				return false, true
			}
			var ae *apiError
			if errors.As(err, &ae) && (ae.gone || ae.transient) {
				if ae.transient {
					transientLeft = true
				} else {
					delete(fs.leftBlobs, item.ContentID)
				}
				a.leaveBlob(item, fs, ae.transient, err)
				nLeft++
				continue
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 content api: %v", err))
			return false, true
		}
		if !gjson.ValidBytes(content) {
			a.leaveBlob(item, fs, true, errors.New("content api returned invalid json"))
			nLeft++
			continue
		}
		// Content is flowing again: the failure clock restarts.
		fs.reset()

		shipped, skipped, ok := a.shipContent(content)
		nShipped += shipped
		nSkipped += skipped
		if !ok {
			return false, true
		}
		a.markDone(item.ContentID)
		delete(fs.leftBlobs, item.ContentID)
		nFetched++
	}
	return !transientLeft, false
}

// leaveBlob reports a blob left unprocessed. Invalid JSON and exhausted
// transient retries also get an independent clock: other successful blobs
// must not hide one that keeps failing until it ages out of the listing.
func (a *Office365Adapter) leaveBlob(item listItem, fs *failureState, track bool, err error) {
	if a.checker == nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 content api: blob was recorded as seen before download and cannot be retried on the next poll with this deduper: %v", err))
		return
	}
	a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 content api, leaving blob for the next poll: %v", err))
	if !track {
		return
	}
	now := time.Now()
	left, exists := fs.leftBlobs[item.ContentID]
	if !exists {
		if len(fs.leftBlobs) >= maxLeftBlobs {
			a.reportLeftBlobs(fs, now, "too many unprocessed content blobs to track; some content may age out before recovery")
			return
		}
		if fs.leftBlobs == nil {
			fs.leftBlobs = make(map[string]leftBlob)
		}
		left = leftBlob{since: now, expires: now.Add(3 * time.Hour)}
		// Once outside the rolling listing window or vendor retention, the
		// entry can't be retried here. Missing dates use the bounded fallback.
		if created, err := time.Parse(time.RFC3339, item.ContentCreated); err == nil {
			left.expires = created.Add(3 * time.Hour)
		}
		if expiry, err := time.Parse(time.RFC3339, item.ContentExpiration); err == nil && expiry.Before(left.expires) {
			left.expires = expiry
		}
		fs.leftBlobs[item.ContentID] = left
	}
	if now.Sub(left.since) >= a.conf.failureEscalation {
		a.reportLeftBlobs(fs, now, "content blob remains unprocessed despite repeated polls")
	}
}

func (a *Office365Adapter) expireLeftBlobs(fs *failureState, now time.Time) {
	for key, left := range fs.leftBlobs {
		if !now.Before(left.expires) {
			// A blob first seen near the end of its window can expire before
			// the escalation period. Don't silently forget that lost retry.
			a.reportLeftBlobs(fs, now, "unprocessed content blob left the listing window or expired before recovery")
			delete(fs.leftBlobs, key)
			continue
		}
		if now.Sub(left.since) >= a.conf.failureEscalation {
			a.reportLeftBlobs(fs, now, "content blob remains unprocessed despite repeated polls")
		}
	}
}

func (a *Office365Adapter) reportLeftBlobs(fs *failureState, now time.Time, message string) {
	// Aggregate overdue/capacity reports once per period, rather than one
	// OnError per blob or poll when an on-prem host keeps running.
	if !fs.lastBlobError.IsZero() && now.Sub(fs.lastBlobError) < a.conf.failureEscalation {
		return
	}
	fs.lastBlobError = now
	a.conf.ClientOptions.OnError(fmt.Errorf("office365 content api: %s", message))
}

// shipContent ships every event of a content blob. There is apparently no
// standard deduplication key in these logs and MS makes no guarantees of
// uniqueness, so we take a best stab by using the ID per event, recorded only
// once the event was shipped. It returns false if shipping failed, in which
// case the adapter is stopping.
func (a *Office365Adapter) shipContent(content []byte) (int, int, bool) {
	nShipped := 0
	nSkipped := 0
	ok := true
	gjson.ParseBytes(content).ForEach(func(_, event gjson.Result) bool {
		ID := event.Get("Id").String()
		if ID != "" && a.isDone(ID) {
			nSkipped++
			return true
		}
		msg := &protocol.DataMessage{
			TextPayload: event.Raw,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				if err != uspclient.ErrorClosed {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
				a.doStop.Set()
				ok = false
				return false
			}
		}
		if ID != "" {
			a.markDone(ID)
		}
		nShipped++
		return true
	})
	return nShipped, nSkipped, ok
}

// isDone reports whether a content blob or event was already shipped. With a
// deduper that can't check without recording (CheckingDeduper), the key is
// recorded here, before the work is done, as it always was.
func (a *Office365Adapter) isDone(key string) bool {
	if a.checker != nil {
		return a.checker.Check(key)
	}
	return a.conf.Deduper.CheckAndAdd(key)
}

// markDone records a content blob or event as shipped. Shipped means accepted
// by the USP client, which delivers it from its buffer: what a Close drops
// from that buffer is not re-fetched.
func (a *Office365Adapter) markDone(key string) {
	if a.checker != nil {
		// Overlapping instances can duplicate work between Check and this
		// commit. Preserving unshipped data takes priority over that race.
		a.checker.CheckAndAdd(key)
	}
}

func (a *Office365Adapter) updateBearerToken() error {
	// Determine the correct OAuth2 token URL and resource scope based on the endpoint type
	// This is critical for government clouds to avoid "Confidential Client is not supported in Cross Cloud request" errors
	// Reference: https://docs.microsoft.com/en-us/azure/azure-government/compare-azure-government-global-azure#guidance-for-developers
	var tokenURL, resourceScope string

	if a.endpointType == "custom" {
		// For custom endpoints, default to enterprise settings
		tokenURL = fmt.Sprintf("https://login.windows.net/%s/oauth2/token?api-version=1.0", a.conf.Domain)
		resourceScope = "https://manage.office.com"
	} else if baseTokenURL, ok := TokenURL[a.endpointType]; ok {
		// Use the correct OAuth2 endpoint for the cloud environment
		if a.endpointType == "gcc-high-gov" || a.endpointType == "dod-gov" {
			// For GCC High and DoD, use tenant ID and v2.0 endpoint
			tokenURL = fmt.Sprintf("%s%s/oauth2/v2.0/token", baseTokenURL, a.conf.TenantID)
		} else {
			// For enterprise and GCC, use domain and v1.0 endpoint
			tokenURL = fmt.Sprintf("%s%s/oauth2/token?api-version=1.0", baseTokenURL, a.conf.Domain)
		}
		resourceScope = ResourceScope[a.endpointType]
	} else {
		// Fallback to enterprise settings
		tokenURL = fmt.Sprintf("https://login.windows.net/%s/oauth2/token?api-version=1.0", a.conf.Domain)
		resourceScope = "https://manage.office.com"
	}

	// An explicit token_url overrides whatever was derived above.
	if a.conf.TokenURL != "" {
		tokenURL = a.conf.TokenURL
	}

	var conf *clientcredentials.Config

	// GCC High and DoD use v2.0 endpoints which require 'scope' parameter instead of 'resource'
	if a.endpointType == "gcc-high-gov" || a.endpointType == "dod-gov" {
		conf = &clientcredentials.Config{
			ClientID:     a.conf.ClientID,
			ClientSecret: a.conf.ClientSecret,
			TokenURL:     tokenURL,
			Scopes:       []string{resourceScope + "/.default"},
		}
	} else {
		// Enterprise and regular GCC use v1.0 endpoints with 'resource' parameter
		conf = &clientcredentials.Config{
			ClientID:     a.conf.ClientID,
			ClientSecret: a.conf.ClientSecret,
			TokenURL:     tokenURL,
			EndpointParams: url.Values{
				"resource": []string{resourceScope},
			},
		}
	}

	// Token requests and API requests go through the client built by the
	// constructor, keeping its timeouts.
	// Token requests are also cancelled by Close.
	client := conf.Client(context.WithValue(a.stopCtx, oauth2.HTTPClient, a.httpClient))
	client.Timeout = httpTimeout
	a.httpClient = client

	return nil
}

func (a *Office365Adapter) makeOneRegistrationRequest(url string) (utils.Dict, error) {
	// Prepare the request.
	req, err := http.NewRequest("POST", url, &bytes.Buffer{})
	if err != nil {
		a.doStop.Set()
		a.conf.ClientOptions.OnError(fmt.Errorf("http.NewRequest(): %v", err))
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Issue the request.
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("http.Client.Do(): %v", err))
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusBadRequest && strings.Contains(string(body), "already enabled") {
		return nil, nil
	}

	// Evaluate if success.
	if resp.StatusCode != http.StatusOK {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 start api non-200: %s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)))
		return nil, errors.New(resp.Status)
	}

	// Parse the response.
	respData := utils.Dict{}
	if err := json.Unmarshal(body, &respData); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("office365 start api invalid json: %v (%s)", err, string(body)))
		return nil, fmt.Errorf("office365 start api invalid json: %v (%s)", err, string(body))
	}

	return respData, nil
}

// errStopped is returned by requests interrupted because the adapter is
// stopping.
var errStopped = errors.New("adapter stopping")

// apiError is a failed Management API request.
type apiError struct {
	err error
	// transient is set for throttling (429), server errors (5xx) and network
	// errors: retrying the same request can succeed.
	transient bool
	// retryAfter is the delay the server asked for, 0 if none.
	retryAfter time.Duration
	// gone is set when the content doesn't exist or has expired.
	gone bool
}

func (e *apiError) Error() string { return e.err.Error() }

// failureState tracks how long a collection loop has been failing. It is
// cleared when a content blob is downloaded or a whole poll completes, not by
// a successful listing alone, so a content outage still escalates.
type failureState struct {
	since         time.Time
	leftBlobs     map[string]leftBlob
	lastBlobError time.Time
}

type leftBlob struct {
	since   time.Time
	expires time.Time
}

// failingFor marks the loop as failing and returns for how long it has been.
func (f *failureState) failingFor(now time.Time) time.Duration {
	if f.since.IsZero() {
		f.since = now
	}
	return now.Sub(f.since)
}

func (f *failureState) restart(now time.Time) { f.since = now }

func (f *failureState) reset() { f.since = time.Time{} }

// getWithRetry issues a GET until it succeeds, fails with a non-transient
// error, the adapter stops (errStopped), or, when giveUpAfter isn't 0, it has
// failed transiently for giveUpAfter (the last error is returned). Transient
// failures are retried with exponential backoff and jitter, honouring
// Retry-After. They are reported with OnWarning until the loop has been
// failing for failureEscalation, then with OnError: OnError stops a hosted
// adapter, so raising it on the first 429 would restart a healthy adapter,
// while an outage that lasts must still be reported.
func (a *Office365Adapter) getWithRetry(url string, what string, fs *failureState, giveUpAfter time.Duration) ([]byte, http.Header, error) {
	var firstFailure time.Time
	for attempt := 1; ; attempt++ {
		if a.doStop.IsSet() {
			return nil, nil, errStopped
		}
		body, header, err := a.get(url)
		var ae *apiError
		if err == nil || !errors.As(err, &ae) || !ae.transient {
			return body, header, err
		}

		wait := a.backoff(attempt, ae.retryAfter)
		now := time.Now()
		if firstFailure.IsZero() {
			firstFailure = now
		}
		escalated := false
		if d := fs.failingFor(now); d >= a.conf.failureEscalation {
			a.conf.ClientOptions.OnError(fmt.Errorf("office365 %s failing for %s: %v", what, d.Round(time.Second), err))
			// Start a new period so a host that keeps the adapter running
			// after OnError isn't flooded with errors.
			fs.restart(now)
			escalated = true
		}
		if giveUpAfter > 0 && now.Sub(firstFailure) >= giveUpAfter {
			return nil, nil, err
		}
		if !escalated {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("office365 %s failed, retrying in %s: %v", what, wait.Round(time.Millisecond), err))
		}
		if a.doStop.WaitFor(wait) {
			return nil, nil, errStopped
		}
	}
}

// backoff returns how long to wait before retrying after the n-th consecutive
// transient failure of a request: an exponential delay with jitter, so that
// adapters failing together don't retry in lockstep, or the server's
// Retry-After when it is longer.
func (a *Office365Adapter) backoff(n int, retryAfter time.Duration) time.Duration {
	d := a.conf.retryBaseDelay
	for i := 1; i < n && d < a.conf.retryMaxDelay; i++ {
		d *= 2
	}
	if d > a.conf.retryMaxDelay {
		d = a.conf.retryMaxDelay
	}
	d = d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
	if retryAfter > a.conf.maxRetryAfter {
		retryAfter = a.conf.maxRetryAfter
	}
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

// get issues one GET to the Management API and classifies its failure.
func (a *Office365Adapter) get(url string) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(a.stopCtx, "GET", url, nil)
	if err != nil {
		return nil, nil, &apiError{err: fmt.Errorf("http.NewRequest(): %v", err)}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		if a.doStop.IsSet() {
			return nil, nil, errStopped
		}
		return nil, nil, &apiError{
			err:       fmt.Errorf("http.Client.Do(): %v", err),
			transient: !isCredentialError(err),
		}
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		if a.doStop.IsSet() {
			return nil, nil, errStopped
		}
		return nil, nil, &apiError{err: fmt.Errorf("failed to read response body: %v", err), transient: true}
	}

	if resp.StatusCode == http.StatusOK {
		return body, resp.Header, nil
	}
	return nil, nil, &apiError{
		err:        fmt.Errorf("%s\nREQUEST: %s\nRESPONSE: %s", resp.Status, url, string(body)),
		transient:  resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
		retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		gone:       isContentGone(resp.StatusCode, body),
	}
}

// isCredentialError reports whether a request failed because the token
// endpoint refused the credentials (an expired secret, a deleted app...), which
// retrying won't fix. Throttling or an error on the token endpoint's side is
// transient.
func isCredentialError(err error) bool {
	var re *oauth2.RetrieveError
	if !errors.As(err, &re) || re.Response == nil {
		return false
	}
	code := re.Response.StatusCode
	return code >= 400 && code < 500 && code != http.StatusTooManyRequests
}

// isContentGone reports whether a content request failed because the blob
// doesn't exist or has expired (AF20050, AF20051, AF20052).
func isContentGone(status int, body []byte) bool {
	if status == http.StatusNotFound || status == http.StatusGone {
		return true
	}
	if status != http.StatusBadRequest {
		return false
	}
	code := gjson.GetBytes(body, "error.code").String()
	return code == "AF20050" || code == "AF20051" || code == "AF20052"
}

// parseRetryAfter parses a Retry-After header given either as seconds or as an
// HTTP date. An absent or unparseable value yields 0.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
