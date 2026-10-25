package usp_okta

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
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	logsURL             = "/api/v1/logs"
	overlapPeriod       = 30 * time.Minute
	defaultPollInterval = 30 * time.Second
	// Bound one uncancellable SDK admission while preserving the original
	// ten-second warning threshold and one-hour retry budget.
	admissionSlice = 250 * time.Millisecond
)

type uspSink interface {
	acktrack.Client
	Drain(time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

type OktaConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ApiKey        string                  `json:"apikey" yaml:"apikey"`
	URL           string                  `json:"url" yaml:"url"`
	PollInterval  time.Duration           `json:"-" yaml:"-"`
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
	tracker      *acktrack.ACKTracker
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
		client, e := acktrack.NewClient(ctx, conf.ClientOptions)
		if e != nil {
			return nil, nil, e
		}
		sink = client
	}
	workerCtx, cancel := context.WithCancel(ctx)
	tracker, err := acktrack.NewACKTracker(sink, 1, 16)
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
		_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		a.httpClient.CloseIdleConnections()
		a.closeErr = errors.Join(err1, err2)
	})
	return a.closeErr
}

func (a *OktaAdapter) issue(code string) {
	if a.conf.ClientOptions.DebugLog != nil {
		a.conf.ClientOptions.DebugLog("okta collection: " + code)
	}
}

func (a *OktaAdapter) initialPosition() oktaPosition {
	u := *a.endpoint
	q := u.Query()
	// Fix the ordinary first-worker baseline once; never rewind an expired cursor.
	q.Set("since", time.Now().UTC().Format(time.RFC3339))
	q.Set("sortOrder", "ASCENDING")
	u.RawQuery = q.Encode()
	return oktaPosition{Next: u.String()}
}

func (a *OktaAdapter) run() {
	g := newOktaGeneration(1, a.initialPosition())
	a.generation = g
	done := make(chan struct{})
	go func() { defer close(done); a.collect(a.ctx, g) }()
	timer := time.NewTicker(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-a.ctx.Done():
			<-done
			return
		case <-done:
			return
		case <-timer.C:
			_ = a.confirm(g)
		}
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
			if !acktrack.Wait(ctx, a.pollInterval) {
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
				// Keep retrying the exact opaque cursor. Rewinding could duplicate
				// earlier pages; replacing it with now could skip source records.
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
			if !acktrack.Wait(ctx, delay) {
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
				// A page may straddle the live bound. Retain its exact offset
				// while completed ACKed pages yield; never drop a current/pending ID.
				for len(g.live) >= liveIDLimit && !g.makeRoom(time.Now()) {
					a.issue("live_history_pressure")
					if !acktrack.Wait(ctx, a.pollInterval) {
						return
					}
				}
				if ctx.Err() != nil {
					return
				}
				msg := &protocol.DataMessage{JsonPayload: item, TimestampMs: uint64(time.Now().UnixMilli())}
				admission, err := a.admit(ctx, msg)
				if err != nil {
					if errors.Is(err, acktrack.ErrSequence) || errors.Is(err, acktrack.ErrDependency) || errors.Is(err, acktrack.ErrLimit) {
						a.issue("admission_tracking_failed")
						return
					}
					if ctx.Err() == nil && !errors.Is(err, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
						a.conf.ClientOptions.OnError(errors.New("okta event admission failed"))
					}
					return
				}
				entry = liveID{dependency: admission, page: g.page, expiry: time.Now().Add(overlapPeriod).Unix()}
				g.live[digest] = entry // A failed/in-flight Ship is never suppression.
			}
			// A referenced prior-page ID also protects duplicates later in this
			// current page. Pressure must not retire it before page completion.
			if exists && entry.page != g.page {
				entry.page = g.page
				g.live[digest] = entry
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
		var candidateErr error
		if g.hasDependency {
			candidateErr = a.tracker.Candidate("logs", candidateBytes(g.id, g.page), g.dependency)
		} else {
			candidateErr = a.tracker.Candidate("logs", candidateBytes(g.id, g.page))
		}
		if candidateErr != nil {
			a.issue("candidate_rejected")
			return
		}

		next = page.next
		delay := page.delay
		if len(page.items) == 0 && delay < a.pollInterval {
			delay = a.pollInterval
		}
		if delay > 0 && !acktrack.Wait(ctx, delay) {
			return
		}
	}
}

// admit retries only an unaccepted message. Successful admissions keep their
// dependency even if cancellation races their return; the SDK is never sealed
// here; accepted records retain their SDK buffer and sequence history.
func (a *OktaAdapter) admit(ctx context.Context, msg *protocol.DataMessage) (acktrack.ACKDependency, error) {
	started := time.Now()
	deadline := started.Add(10*time.Second + time.Hour)
	warned := false
	for {
		if err := ctx.Err(); err != nil {
			return acktrack.ACKDependency{}, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return acktrack.ACKDependency{}, uspclient.ErrorBufferFull
		}
		slice := min(admissionSlice, remaining)
		attempted := time.Now()
		dep, err := a.tracker.Ship(msg, slice)
		if !errors.Is(err, uspclient.ErrorBufferFull) {
			return dep, err
		}
		if !warned && time.Since(started) >= 10*time.Second {
			warned = true
			if a.conf.ClientOptions.OnWarning != nil {
				a.conf.ClientOptions.OnWarning("stream falling behind")
			}
		}
		// The real SDK normally consumes the slice. Avoid a hot loop if a
		// client instead reports a full buffer before its timeout expires.
		if delay := time.Until(attempted.Add(slice)); delay > 0 && !waitExact(ctx, delay) {
			return acktrack.ACKDependency{}, ctx.Err()
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
