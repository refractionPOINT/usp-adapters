package usp_okta

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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

const (
	logsURL             = "/api/v1/logs"
	overlapPeriod       = 30 * time.Minute
	defaultPollInterval = 30 * time.Second
)

type uspSink interface {
	utils.CheckpointClient
	Drain(time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type OktaConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiKey          string                  `json:"apikey" yaml:"apikey"`
	URL             string                  `json:"url" yaml:"url"`
	PollInterval    time.Duration           `json:"-" yaml:"-"`
	CheckpointStore utils.CheckpointStore   `json:"-" yaml:"-"`
	// CheckpointIssue receives fixed operational codes, never store errors,
	// cursors or source identifiers. The hook must return promptly.
	CheckpointIssue func(string) `json:"-" yaml:"-"`
}

func (c *OktaConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if _, err := oktaEndpoint(c.URL); err != nil {
		return err
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}
	return nil
}

func oktaEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid okta endpoint")
	}
	u.Path = logsURL
	return u, nil
}

type OktaAdapter struct {
	conf         OktaConfig
	uspClient    uspSink
	httpClient   *http.Client
	pollInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	chStopped    chan struct{}
	wgSenders    sync.WaitGroup
	tracker      *utils.ACKTracker
	endpoint     *url.URL
	// Only the worker writes generation. Close reads it after joining worker.
	generation *oktaGeneration
	closeOnce  sync.Once
	closeErr   error
}

func NewOktaAdapter(ctx context.Context, conf OktaConfig) (*OktaAdapter, chan struct{}, error) {
	return newOktaAdapter(ctx, conf, nil)
}

func newOktaAdapter(ctx context.Context, conf OktaConfig, sink uspSink) (*OktaAdapter, chan struct{}, error) {
	endpoint, err := oktaEndpoint(conf.URL)
	if err != nil {
		return nil, nil, err
	}
	if sink == nil {
		client, e := uspclient.NewClient(ctx, conf.ClientOptions)
		if e != nil {
			return nil, nil, e
		}
		sink = client
	}
	workerCtx, cancel := context.WithCancel(ctx)
	tracker, err := utils.NewACKTracker(sink, 1, checkpointPositionLimit+16)
	if err != nil {
		cancel()
		_, _ = sink.Close()
		return nil, nil, err
	}
	a := &OktaAdapter{conf: conf, uspClient: sink, endpoint: endpoint, tracker: tracker, ctx: workerCtx, cancel: cancel, pollInterval: conf.PollInterval, chStopped: make(chan struct{})}
	if a.pollInterval <= 0 {
		a.pollInterval = defaultPollInterval
	}
	a.httpClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext},
		// Reject redirects; even a same-origin hop could conceal another origin.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	a.wgSenders.Add(1)
	go func() { defer a.wgSenders.Done(); defer close(a.chStopped); a.run() }()
	return a, a.chStopped, nil
}

func (a *OktaAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.cancel()
		// Seal admission and wake blocked Ship calls before joining producers.
		// Accepted messages stay buffered for the subsequent ACK observation.
		if client, ok := a.uspClient.(interface{ BeginDrain() }); ok {
			client.BeginDrain()
		}
		a.wgSenders.Wait()
		err1 := a.uspClient.Drain(time.Minute)
		// Drain alone is not an ACK proof. All HTTP/Ship producers have joined.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if a.generation != nil {
			_ = a.saveConfirmed(ctx, a.generation)
		}
		cancel()
		_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		a.httpClient.CloseIdleConnections()
		a.closeErr = errors.Join(err1, err2)
	})
	return a.closeErr
}

func (a *OktaAdapter) issue(code string) {
	if a.conf.CheckpointIssue != nil {
		a.conf.CheckpointIssue(code)
	}
	// Refused optional adoption preserves first-start collection and is an
	// internal capacity signal only, without a vendor or warning callback.
	if code == "allocation_refused" {
		return
	}
	if a.conf.ClientOptions.OnWarning != nil {
		a.conf.ClientOptions.OnWarning("okta checkpoint: " + code)
	}
}

func (a *OktaAdapter) initialPosition() ([]byte, error) {
	u := *a.endpoint
	q := u.Query()
	// Release one retains today's first-start floor, fixed once after absence.
	q.Set("since", time.Now().UTC().Format(time.RFC3339))
	q.Set("sortOrder", "ASCENDING")
	u.RawQuery = q.Encode()
	return encodeOktaPosition(oktaPosition{Boundary: u.String(), Next: u.String()})
}

func (a *OktaAdapter) run() {
	source := sha256.Sum256([]byte(strings.ToLower(a.endpoint.Scheme+"://"+a.endpoint.Host) + logsURL))
	var generation uint64
	for a.ctx.Err() == nil {
		generation++
		var session *utils.CheckpointSession
		var raw []byte
		var err error
		if a.conf.CheckpointStore != nil {
			session, raw, err = utils.OpenCheckpoint(a.ctx, a.conf.CheckpointStore, 1, source, a.initialPosition, func(raw []byte) error { _, err := a.decodePosition(raw); return err }, a.issue)
		} else {
			raw, err = a.initialPosition()
		}
		if err != nil {
			return
		}
		position, err := a.decodePosition(raw)
		if err != nil {
			a.issue("invalid_position")
			return
		}
		g := newOktaGeneration(generation, session, position)
		a.generation = g
		ctx, cancel := context.WithCancel(a.ctx)
		done := make(chan struct{})
		go func() { defer close(done); a.collect(ctx, g) }()
		timer := time.NewTimer(saveInterval())
		conflict := false
	worker:
		for {
			select {
			case <-a.ctx.Done():
				break worker
			case <-done:
				break worker
			case <-timer.C:
				saveCtx, stop := context.WithTimeout(ctx, 2*time.Second)
				err := a.saveConfirmed(saveCtx, g)
				stop()
				if errors.Is(err, utils.ErrCheckpointConflict) {
					conflict = true
					break worker
				}
				timer.Reset(saveInterval())
			}
		}
		timer.Stop()
		cancel()
		<-done // Join the older traversal before reclaiming any generation.
		if !conflict {
			return
		}
		a.issue("ownership_changed")
		// Keep the SDK/tracker's sequence history; ignore old candidate IDs.
		a.generation = nil
	}
}

func (a *OktaAdapter) collect(ctx context.Context, g *oktaGeneration) {
	if !waitExact(ctx, a.pollInterval) {
		return
	}
	next := g.position.Next
	backoff := time.Second
	for ctx.Err() == nil {
		if !g.makeRoom(time.Now()) {
			a.issue("live_history_pressure")
			if !utils.CheckpointWait(ctx, a.pollInterval) {
				return
			}
			continue
		}
		page, err := a.requestPage(ctx, next)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errInvalidContinuation) {
				// The original scan can precede restored ID coverage by hours.
				// Without a proven replay anchor, retain and retry this cursor:
				// rewinding could duplicate uncovered history, and now could skip it.
				a.issue("continuation_stalled")
			} else if errors.Is(err, errVendorCredential) {
				if a.conf.ClientOptions.OnError != nil {
					a.conf.ClientOptions.OnError(errors.New("okta API credentials or permission rejected"))
				}
			} else {
				a.issue("request_retry")
			}
			delay := backoff
			if page.delay > delay {
				delay = page.delay
			}
			if errors.Is(err, errVendorCredential) {
				delay = a.pollInterval
			}
			if !utils.CheckpointWait(ctx, delay) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			continue
		}
		backoff = time.Second
		g.page++
		for _, item := range page.items {
			if ctx.Err() != nil {
				return
			}
			id, _ := item["uuid"].(string)
			if id == "" {
				a.issue("invalid_event")
				return
			}
			digest := eventDigest(id)
			entry, exists := g.live[digest]
			if !exists {
				msg := &protocol.DataMessage{JsonPayload: item, TimestampMs: uint64(time.Now().UnixMilli())}
				admission, err := a.tracker.Ship(msg, 10*time.Second)
				if errors.Is(err, uspclient.ErrorBufferFull) {
					if a.conf.ClientOptions.OnWarning != nil {
						a.conf.ClientOptions.OnWarning("stream falling behind")
					}
					admission, err = a.tracker.Ship(msg, time.Hour)
				}
				if err != nil {
					if !errors.Is(err, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
						a.conf.ClientOptions.OnError(errors.New("okta event admission failed"))
					}
					return
				}
				entry = liveID{dependency: admission, page: g.page, expiry: time.Now().Add(overlapPeriod).Unix()}
				g.live[digest] = entry // A failed/in-flight Ship is never suppression.
				g.remember(savedID{digest: digest, expiry: entry.expiry})
			}
			if entry.page > g.confirmed.Load() {
				var err error
				g.dependency, err = g.dependency.Include(entry.dependency)
				if err != nil {
					a.issue("invalid_dependency")
					return
				}
				g.hasDependency = true
			}
		}
		g.position.Next = page.next
		g.position.IDs = append([]savedID(nil), g.recent...)
		raw, err := encodeOktaPosition(g.position)
		if err != nil {
			a.issue("position_oversize")
		} else {
			candidate := candidateBytes(g.id, g.page, raw)
			if g.hasDependency {
				err = a.tracker.Candidate("logs", candidate, g.dependency)
			} else {
				err = a.tracker.Candidate("logs", candidate)
			}
			if err != nil {
				a.issue("candidate_rejected")
			}
		}
		next = page.next
		delay := page.delay
		if len(page.items) == 0 && delay < a.pollInterval {
			delay = a.pollInterval
		}
		if delay > 0 && !utils.CheckpointWait(ctx, delay) {
			return
		}
	}
}

func waitExact(ctx context.Context, duration time.Duration) bool {
	t := time.NewTimer(duration)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
