package usp_defender

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// environment describes one Microsoft national cloud deployment. Keeping the
// three values that must move together (alerts endpoint, token host and scope)
// in a single struct makes a partial/desynced configuration impossible -- the
// alertsURL, tokenHost and scope for an environment can never disagree.
type environment struct {
	// alertsURL is the MS Graph security alerts_v2 endpoint.
	alertsURL string
	// tokenHost is the Azure AD token host; the full token endpoint is
	// <tokenHost>/<tenant_id>/oauth2/v2.0/token.
	tokenHost string
	// scope is the OAuth2 scope, which must match the MS Graph service root.
	scope string
}

// environments maps an endpoint name to its Microsoft national cloud
// deployment. gcc-gov (US Government GCC / moderate) uses the worldwide
// endpoints -- identical to enterprise -- and is kept as a named option for
// parity with the o365 adapter, which exposes the same four names.
// Reference: https://learn.microsoft.com/en-us/graph/deployments
var environments = map[string]environment{
	"enterprise": {
		alertsURL: "https://graph.microsoft.com/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.com",
		scope:     "https://graph.microsoft.com/.default",
	},
	"gcc-gov": {
		alertsURL: "https://graph.microsoft.com/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.com",
		scope:     "https://graph.microsoft.com/.default",
	},
	"gcc-high-gov": {
		alertsURL: "https://graph.microsoft.us/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.us",
		scope:     "https://graph.microsoft.us/.default",
	},
	"dod-gov": {
		alertsURL: "https://dod-graph.microsoft.us/v1.0/security/alerts_v2",
		tokenHost: "https://login.microsoftonline.us",
		scope:     "https://dod-graph.microsoft.us/.default",
	},
}

// defaultEndpoint is the environment used when Endpoint is left empty.
const defaultEndpoint = "enterprise"

const (
	// defaultTokenURLTemplate is the Azure AD token endpoint template,
	// parameterized by the token host and tenant id.
	defaultTokenURLTemplate = "%s/%s/oauth2/v2.0/token"

	// defaultPollInterval is how long the adapter waits between polls of the
	// alerts endpoint.
	defaultPollInterval = 30 * time.Second
)

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	utils.CheckpointClient
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type DefenderAdapter struct {
	conf       DefenderConfig
	uspClient  uspSink
	httpClient *http.Client

	endpoint     string
	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx           context.Context
	cancel        context.CancelFunc
	tracker       *utils.ACKTracker
	generation    *defenderGeneration
	closeOnce     sync.Once
	closeErr      error
	runtime       defenderRuntime
	startedAt     int64
	collectionURL *url.URL
}

type DefenderConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`

	// Endpoint selects the Microsoft national cloud deployment. Valid values:
	// "enterprise" (default, global/commercial), "gcc-gov" (US Government GCC /
	// moderate), "gcc-high-gov" (US Government GCC High / L4) and "dod-gov"
	// (US Government DoD / L5). An empty value defaults to "enterprise", so
	// existing configs keep talking to the commercial cloud unchanged.
	// Reference: https://learn.microsoft.com/en-us/graph/deployments
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// TokenURL overrides the Azure AD token endpoint derived from Endpoint and
	// TenantID (e.g. https://login.microsoftonline.com/<tenant_id>/oauth2/v2.0/token
	// for the enterprise endpoint). It only overrides the token URL; the OAuth2
	// scope still follows Endpoint, so a token_url pointed at a gov host without
	// also setting endpoint sends the commercial scope and will fail auth.
	TokenURL string `json:"token_url" yaml:"token_url"`

	// AlertsURL overrides the MS Graph security alerts endpoint derived from
	// Endpoint (e.g. https://graph.microsoft.com/v1.0/security/alerts_v2 for
	// the enterprise endpoint). It only overrides the alerts URL; the OAuth2
	// scope still follows Endpoint, so an alerts_url pointed at a gov host
	// without also setting endpoint sends the commercial scope and will 401.
	AlertsURL string `json:"alerts_url" yaml:"alerts_url"`

	// PollInterval overrides how long the adapter waits between polls of the
	// alerts endpoint (default: 30s). Not exposed in json/yaml configs
	// (time.Duration does not deserialize meaningfully); it is a seam for
	// tests, like in the other polling adapters.
	PollInterval time.Duration `json:"-" yaml:"-"`

	// CheckpointStore is supplied by the caller; nil retains first-start behavior.
	CheckpointStore utils.CheckpointStore `json:"-" yaml:"-"`
	// CheckpointIssue receives fixed operational codes only. It may be called
	// concurrently and must return promptly.
	CheckpointIssue func(string) `json:"-" yaml:"-"`
}

// endpoint returns the configured Microsoft national cloud deployment name,
// defaulting to "enterprise" when unset.
func (c *DefenderConfig) endpoint() string {
	if c.Endpoint == "" {
		return defaultEndpoint
	}
	return c.Endpoint
}

// tokenURL returns the token endpoint to use: the configured override, or the
// Azure AD token endpoint for the configured environment and tenant.
func (c *DefenderConfig) tokenURL() string {
	if c.TokenURL != "" {
		return c.TokenURL
	}
	return fmt.Sprintf(defaultTokenURLTemplate, environments[c.endpoint()].tokenHost, c.TenantID)
}

// alertsURL returns the alerts endpoint to use: the configured override, or
// the MS Graph security alerts endpoint for the configured environment.
func (c *DefenderConfig) alertsURL() string {
	if c.AlertsURL != "" {
		return c.AlertsURL
	}
	return environments[c.endpoint()].alertsURL
}

// scope returns the OAuth2 scope for the configured environment, which must
// match that environment's MS Graph service root.
func (c *DefenderConfig) scope() string {
	return environments[c.endpoint()].scope
}

func (c *DefenderConfig) Validate() error {
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
	if c.Endpoint != "" {
		if _, ok := environments[c.Endpoint]; !ok {
			return fmt.Errorf("invalid endpoint %q, must be one of: enterprise, gcc-gov, gcc-high-gov, dod-gov", c.Endpoint)
		}
	}
	return nil
}

func NewDefenderAdapter(ctx context.Context, conf DefenderConfig) (*DefenderAdapter, chan struct{}, error) {
	return newDefenderAdapter(ctx, conf, nil)
}

func newDefenderAdapter(ctx context.Context, conf DefenderConfig, sink uspSink) (*DefenderAdapter, chan struct{}, error) {
	return newDefenderAdapterWithRuntime(ctx, conf, sink, defaultRuntime())
}

// Private runtime injection keeps clock/rate tests independent of wall-clock waits.
func newDefenderAdapterWithRuntime(ctx context.Context, conf DefenderConfig, sink uspSink, runtime defenderRuntime) (*DefenderAdapter, chan struct{}, error) {
	if _, ok := environments[conf.endpoint()]; !ok {
		return nil, nil, errors.New("invalid Defender environment")
	}
	endpoint, err := collectionEndpoint(conf.alertsURL())
	if err != nil {
		return nil, nil, err
	}
	if _, err := collectionEndpoint(conf.tokenURL()); err != nil {
		return nil, nil, errors.New("invalid Defender token endpoint")
	}
	if sink == nil {
		client, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
		sink = client
	}
	workerCtx, cancel := context.WithCancel(ctx)
	tracker, err := utils.NewACKTracker(sink, 2, checkpointPositionLimit+16)
	if err != nil {
		cancel()
		_, _ = sink.Close()
		return nil, nil, err
	}
	a := &DefenderAdapter{conf: conf, ctx: workerCtx, cancel: cancel, uspClient: sink, tracker: tracker,
		endpoint: conf.endpoint(), collectionURL: endpoint, runtime: runtime,
		pollInterval: conf.PollInterval, startedAt: runtime.now().UTC().UnixNano(), chStopped: make(chan struct{})}
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}
	a.httpClient = &http.Client{Timeout: 10 * time.Second,
		Transport:     &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	a.wgSenders.Add(1)
	go func() { defer a.wgSenders.Done(); defer close(a.chStopped); a.run() }()
	return a, a.chStopped, nil
}

func (a *DefenderAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.cancel()
		// Seal admission before joining a producer blocked on SDK backpressure.
		if client, ok := a.uspClient.(interface{ BeginDrain() }); ok {
			client.BeginDrain()
		}
		a.wgSenders.Wait()
		err1 := a.uspClient.Drain(time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if a.generation != nil {
			a.finalSave(ctx, a.generation)
		}
		cancel()
		_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		a.httpClient.CloseIdleConnections()
		a.closeErr = errors.Join(err1, err2)
	})
	return a.closeErr
}

func (a *DefenderAdapter) issue(code string) {
	if a.conf.CheckpointIssue != nil {
		a.conf.CheckpointIssue(code)
	}
	if code != "allocation_refused" && a.conf.ClientOptions.OnWarning != nil {
		a.conf.ClientOptions.OnWarning("defender checkpoint: " + code)
	}
}
