package usp_entraid

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

var URL = map[string]string{
	"get_alerts": "https://graph.microsoft.com/v1.0/identityProtection/riskDetections",
}

// environment holds the hosts of one Microsoft national cloud deployment.
type environment struct {
	// loginHost is the Microsoft identity platform base URL the OAuth2
	// client_credentials token is requested from.
	loginHost string
	// graphHost is the Microsoft Graph service root the collections are
	// fetched from.
	graphHost string
	// scope is the OAuth2 scope, which must match graphHost -- tokens are not
	// interchangeable between deployments.
	scope string
}

// environments maps an endpoint name to its Microsoft national cloud
// deployment. gcc-gov (US Government GCC / moderate) uses the worldwide
// endpoints -- identical to enterprise -- and is kept as a named option for
// parity with the defender and o365 adapters, which expose the same four names.
// Reference: https://learn.microsoft.com/en-us/graph/deployments
var environments = map[string]environment{
	"enterprise": {
		loginHost: "https://login.microsoftonline.com",
		graphHost: "https://graph.microsoft.com",
		scope:     "https://graph.microsoft.com/.default",
	},
	"gcc-gov": {
		loginHost: "https://login.microsoftonline.com",
		graphHost: "https://graph.microsoft.com",
		scope:     "https://graph.microsoft.com/.default",
	},
	"gcc-high-gov": {
		loginHost: "https://login.microsoftonline.us",
		graphHost: "https://graph.microsoft.us",
		scope:     "https://graph.microsoft.us/.default",
	},
	"dod-gov": {
		loginHost: "https://login.microsoftonline.us",
		graphHost: "https://dod-graph.microsoft.us",
		scope:     "https://dod-graph.microsoft.us/.default",
	},
}

const (
	// defaultEndpoint is the environment used when Endpoint is left empty,
	// preserving the commercial-cloud behavior of existing deployments.
	defaultEndpoint     = "enterprise"
	defaultPollInterval = 30 * time.Second

	// defaultStreams preserves the historical behavior of the adapter, which
	// only polled Identity Protection risk detections.
	defaultStreams = "risk_detections"
)

// entraStream describes one Microsoft Graph collection the adapter can poll.
// Each collection filters on its existing timestamp field. Preserve $orderby
// where configured; complete window traversal does not rely on response order.
type entraStream struct {
	name    string
	path    string
	tsField string
	orderBy string
}

// Streams supported through EntraIDConfig.Streams. sign_ins requires the
// tenant to hold an Entra ID P1/P2 license (a Microsoft Graph requirement);
// sign_ins and audit_logs both require the AuditLog.Read.All application
// permission, risk_detections requires IdentityRiskEvent.Read.All.
// risk_detections deliberately sends no $orderby, preserving the historical
// request shape for existing deployments.
var entraStreamsByName = map[string]entraStream{
	"risk_detections": {name: "risk_detections", path: "/v1.0/identityProtection/riskDetections", tsField: "activityDateTime"},
	"sign_ins":        {name: "sign_ins", path: "/v1.0/auditLogs/signIns", tsField: "createdDateTime", orderBy: "createdDateTime asc"},
	"audit_logs":      {name: "audit_logs", path: "/v1.0/auditLogs/directoryAudits", tsField: "activityDateTime", orderBy: "activityDateTime asc"},
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	utils.CheckpointClient
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type EntraIDAdapter struct {
	conf       EntraIDConfig
	uspClient  uspSink
	httpClient *http.Client

	pollInterval time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx        context.Context
	cancel     context.CancelFunc
	tracker    *utils.ACKTracker
	generation *entraGeneration
	streams    []entraStream
	runtime    entraRuntime
	startedAt  int64
	closeOnce  sync.Once
	closeErr   error
}

type EntraIDConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	TenantID      string                  `json:"tenant_id" yaml:"tenant_id"`
	ClientID      string                  `json:"client_id" yaml:"client_id"`
	ClientSecret  string                  `json:"client_secret" yaml:"client_secret"`

	// Streams selects which Entra ID collections to poll, as comma separated
	// values. Supported values: "risk_detections" (Identity Protection risk
	// detections), "sign_ins" (auditLogs/signIns sign-in logs) and
	// "audit_logs" (auditLogs/directoryAudits directory audit logs). Empty
	// selects "risk_detections" only, preserving the historical behavior of
	// existing deployments.
	Streams string `json:"streams,omitempty" yaml:"streams,omitempty"`

	// Endpoint selects the Microsoft national cloud deployment. Valid values:
	// "enterprise" (default, global/commercial), "gcc-gov" (US Government GCC /
	// moderate, which runs on the worldwide endpoints), "gcc-high-gov" (US
	// Government GCC High / L4) and "dod-gov" (US Government DoD / L5). An
	// empty value defaults to "enterprise", so existing configs keep talking to
	// the commercial cloud unchanged.
	// Reference: https://learn.microsoft.com/en-us/graph/deployments
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`

	// LoginEndpoint overrides the base URL of the Microsoft identity platform
	// used for the OAuth2 client_credentials token exchange, derived from
	// Endpoint otherwise (https://login.microsoftonline.com for "enterprise").
	// It only overrides the host; the OAuth2 scope still follows Endpoint, so a
	// login_endpoint pointed at a gov host without also setting endpoint sends
	// the commercial scope and will fail auth.
	LoginEndpoint string `json:"login_endpoint,omitempty" yaml:"login_endpoint,omitempty"`

	// GraphEndpoint overrides the base URL of the Microsoft Graph API the
	// collections are fetched from, derived from Endpoint otherwise
	// (https://graph.microsoft.com for "enterprise"). It only overrides the
	// host; the OAuth2 scope still follows Endpoint, so a graph_endpoint
	// pointed at a gov host without also setting endpoint sends the commercial
	// scope and will 401.
	GraphEndpoint string `json:"graph_endpoint,omitempty" yaml:"graph_endpoint,omitempty"`

	// PollInterval overrides the wait between polls of each stream (default
	// 30s). It is not settable through a config file; it exists as a seam for
	// tests.
	PollInterval time.Duration `json:"-" yaml:"-"`

	// CheckpointStore is injected by the caller. Nil preserves first-start behavior.
	CheckpointStore utils.CheckpointStore `json:"-" yaml:"-"`
	// CheckpointIssue receives fixed operational codes, never source data or errors.
	// It may be called concurrently and must return promptly.
	CheckpointIssue func(string) `json:"-" yaml:"-"`
	runtime         *entraRuntime
}

// endpoint returns the configured Microsoft national cloud deployment name,
// defaulting to "enterprise" when unset.
func (c EntraIDConfig) endpoint() string {
	if c.Endpoint == "" {
		return defaultEndpoint
	}
	return c.Endpoint
}

// loginEndpoint returns the Microsoft identity platform base URL to use: the
// configured override, or the identity host of the configured environment.
func (c EntraIDConfig) loginEndpoint() string {
	if c.LoginEndpoint != "" {
		return strings.TrimRight(c.LoginEndpoint, "/")
	}
	return environments[c.endpoint()].loginHost
}

// graphEndpoint returns the Microsoft Graph base URL to use: the configured
// override, or the Graph service root of the configured environment.
func (c EntraIDConfig) graphEndpoint() string {
	if c.GraphEndpoint != "" {
		return strings.TrimRight(c.GraphEndpoint, "/")
	}
	return environments[c.endpoint()].graphHost
}

// scope returns the OAuth2 scope for the configured environment, which must
// match that environment's Graph service root -- access tokens are not
// interchangeable between national cloud deployments.
func (c EntraIDConfig) scope() string {
	return environments[c.endpoint()].scope
}

// tokenURL is the OAuth2 client_credentials token endpoint for the tenant.
func (c EntraIDConfig) tokenURL() string {
	return fmt.Sprintf("%s/%s/oauth2/v2.0/token", c.loginEndpoint(), c.TenantID)
}

// riskDetectionsURL is the Identity Protection risk detections endpoint.
func (c EntraIDConfig) riskDetectionsURL() string {
	return c.graphEndpoint() + entraStreamsByName["risk_detections"].path
}

// streams resolves the configured comma separated stream names into their
// definitions, defaulting to risk detections when unset. Order is preserved
// and duplicates are collapsed.
func (c EntraIDConfig) streams() ([]entraStream, error) {
	raw := c.Streams
	if strings.TrimSpace(raw) == "" {
		raw = defaultStreams
	}
	streams := []entraStream{}
	seen := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		stream, ok := entraStreamsByName[name]
		if !ok {
			return nil, fmt.Errorf("unknown stream %q, supported streams: risk_detections, sign_ins, audit_logs", name)
		}
		seen[name] = struct{}{}
		streams = append(streams, stream)
	}
	if len(streams) == 0 {
		return nil, errors.New("no streams specified")
	}
	return streams, nil
}

func (c *EntraIDConfig) Validate() error {
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
	if _, ok := environments[c.endpoint()]; !ok {
		return fmt.Errorf("invalid endpoint %q, supported endpoints: enterprise, gcc-gov, gcc-high-gov, dod-gov", c.Endpoint)
	}
	if _, err := c.streams(); err != nil {
		return fmt.Errorf("streams: %v", err)
	}
	return nil
}

func NewEntraIDAdapter(ctx context.Context, conf EntraIDConfig) (*EntraIDAdapter, chan struct{}, error) {
	return newEntraIDAdapter(ctx, conf, nil)
}

func newEntraIDAdapter(ctx context.Context, conf EntraIDConfig, sink uspSink) (*EntraIDAdapter, chan struct{}, error) {
	streams, err := conf.streams()
	if err != nil {
		return nil, nil, err
	}
	if _, ok := environments[conf.endpoint()]; !ok {
		return nil, nil, errors.New("invalid Entra endpoint")
	}
	if _, err := collectionEndpoint(conf.graphEndpoint()); err != nil {
		return nil, nil, err
	}
	if _, err := collectionEndpoint(conf.loginEndpoint()); err != nil {
		return nil, nil, err
	}
	rt := defaultRuntime()
	if conf.runtime != nil {
		rt = *conf.runtime
	}
	workerCtx, cancel := context.WithCancel(ctx)
	a := &EntraIDAdapter{conf: conf, ctx: workerCtx, cancel: cancel, streams: streams, runtime: rt, pollInterval: conf.PollInterval, chStopped: make(chan struct{})}
	a.startedAt = rt.now().UTC().UnixNano()
	if rt.start != 0 {
		a.startedAt = rt.start
	}
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}
	if sink == nil {
		client, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		sink = client
	}
	a.uspClient = sink
	a.tracker, err = utils.NewACKTracker(sink, 1, checkpointPositionLimit+16)
	if err != nil {
		cancel()
		_, _ = sink.Close()
		return nil, nil, err
	}
	a.httpClient = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext}}
	a.wgSenders.Add(1)
	go func() { defer a.wgSenders.Done(); defer close(a.chStopped); a.run() }()
	return a, a.chStopped, nil
}

func (a *EntraIDAdapter) issue(code string) {
	if a.conf.CheckpointIssue != nil {
		a.conf.CheckpointIssue(code)
	}
}

func (a *EntraIDAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.cancel()
		if client, ok := a.uspClient.(interface{ BeginDrain() }); ok {
			client.BeginDrain()
		}
		a.wgSenders.Wait()
		drainErr := a.uspClient.Drain(5 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if a.generation != nil {
			a.finalSave(ctx, a.generation)
		}
		cancel()
		_, closeErr := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		a.httpClient.CloseIdleConnections()
		a.closeErr = errors.Join(drainErr, closeErr)
	})
	return a.closeErr
}
