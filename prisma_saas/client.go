// Package usp_prisma_saas implements a USP adapter for the Palo Alto Networks
// SaaS Security (formerly Prisma SaaS) Log Events API.
//
// Events are read from a per-tenant queue. Reading an event removes it from the
// queue and there is no cursor or replay, so the adapter reads only what it can
// ship: each batch is shipped in full (retrying on backpressure, never
// dropping) before the next one is requested. Events are forwarded to
// LimaCharlie in their original JSON form.
package usp_prisma_saas

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultPlatform  = "prisma_saas"
	defaultRegion    = "us"
	defaultMaxPolls  = 4
	maxAllowedPolls  = 30 // upper bound also used by the reference XSOAR collector
	unknownEventType = "unknown"
	timestampField   = "timestamp"
	eventTypeField   = "log_type"
	shipTimeout      = 10 * time.Second
	defaultEmptyWait = 3 * time.Second
	defaultRetryBase = 1 * time.Second
	defaultRetryMax  = 60 * time.Second
	maxRetryAfter    = 5 * time.Minute
	defaultShutGrace = 25 * time.Second // > the 20s server-side long-poll timeout
	// defaultProducerGrace bounds how long Close waits for readers blocked in
	// Ship once no read is in flight, before the USP client is sealed.
	defaultProducerGrace = 10 * time.Second
	minLongPollLength    = 1 * time.Second // a faster "no data" is not a real long-poll timeout
)

// regionHosts maps a SaaS Security region to its API host.
var regionHosts = map[string]string{
	"us":   "https://api.aperture.paloaltonetworks.com",
	"apac": "https://api.aperture-apac.paloaltonetworks.com",
	"eu":   "https://api.aperture-eu.paloaltonetworks.com",
	"uk":   "https://api.aperture-uk.paloaltonetworks.com",
	"in":   "https://api.in1.prisma-saas.paloaltonetworks.com",
	"au":   "https://api.au1.prisma-saas.paloaltonetworks.com",
	"uk2":  "https://api.uk2.prisma-saas.paloaltonetworks.com",
	"jp":   "https://api.jp1.prisma-saas.paloaltonetworks.com",
}

// timestampLayouts are the accepted formats of an event's timestamp. The API
// documents ISO8601 with an optional millisecond part and a Z suffix.
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
}

// PrismaSaaSConfig is the adapter configuration.
type PrismaSaaSConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// ClientID and ClientSecret are the credentials of an API client created in
	// Strata Cloud Manager with the "Log File Access" scope.
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"client_secret" yaml:"client_secret"`

	// Region selects the API host: us (default), eu, apac, uk, uk2, in, au, jp.
	Region string `json:"region" yaml:"region"`

	// BaseURL overrides the region's host. It must be an https URL.
	BaseURL string `json:"base_url" yaml:"base_url"`

	// MaxConcurrentPolls is the number of concurrent readers. The single-event
	// endpoint returns one event per call, so several long-polls run in
	// parallel for throughput. Default 4, at most 30.
	MaxConcurrentPolls int `json:"max_concurrent_polls" yaml:"max_concurrent_polls"`

	// Test seams (not settable from a config file).
	allowInsecureHTTP bool
	emptyWait         time.Duration
	retryBaseDelay    time.Duration
	retryMaxDelay     time.Duration
	shutdownGrace     time.Duration
	producerStopGrace time.Duration
}

// Validate checks the configuration and fills in defaults.
func (c *PrismaSaaSConfig) Validate() error {
	if strings.TrimSpace(c.ClientOptions.Platform) == "" {
		c.ClientOptions.Platform = defaultPlatform
	}
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	c.ClientID = strings.TrimSpace(c.ClientID)
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}

	c.Region = strings.ToLower(strings.TrimSpace(c.Region))
	if c.Region == "" {
		c.Region = defaultRegion
	}
	if _, ok := regionHosts[c.Region]; !ok {
		return fmt.Errorf("unknown region %q (valid: us, eu, apac, uk, uk2, in, au, jp)", c.Region)
	}

	c.BaseURL = strings.TrimSpace(c.BaseURL)
	if c.BaseURL != "" {
		u, err := url.Parse(c.BaseURL)
		if err != nil || u.Host == "" {
			return errors.New("base_url is not a valid URL")
		}
		if u.Scheme != "https" && !(c.allowInsecureHTTP && u.Scheme == "http") {
			return errors.New("base_url must use https")
		}
		if u.User != nil {
			return errors.New("base_url must not contain credentials")
		}
	}

	if c.MaxConcurrentPolls < 0 || c.MaxConcurrentPolls > maxAllowedPolls {
		return fmt.Errorf("max_concurrent_polls must be between 1 and %d", maxAllowedPolls)
	}
	if c.MaxConcurrentPolls == 0 {
		c.MaxConcurrentPolls = defaultMaxPolls
	}

	if c.emptyWait <= 0 {
		c.emptyWait = defaultEmptyWait
	}
	if c.retryBaseDelay <= 0 {
		c.retryBaseDelay = defaultRetryBase
	}
	if c.retryMaxDelay <= 0 {
		c.retryMaxDelay = defaultRetryMax
	}
	if c.shutdownGrace <= 0 {
		c.shutdownGrace = defaultShutGrace
	}
	if c.producerStopGrace <= 0 {
		c.producerStopGrace = defaultProducerGrace
	}
	return nil
}

// resolveBaseURL returns the API root: base_url when set, else the region host.
func (c *PrismaSaaSConfig) resolveBaseURL() string {
	if c.BaseURL != "" {
		return strings.TrimRight(c.BaseURL, "/")
	}
	return regionHosts[c.Region]
}

// uspSink is the subset of *uspclient.Client the adapter depends on, so tests
// can substitute an in-memory sink.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// PrismaSaaSAdapter reads SaaS Security log events and ships them to
// LimaCharlie.
type PrismaSaaSAdapter struct {
	conf      PrismaSaaSConfig
	uspClient uspSink
	api       *apiClient

	// useBulk is true until log_events_bulk answers 404, after which the
	// documented single-event endpoint is used for good.
	useBulk atomic.Bool

	// Reads in flight. Close must not let the USP client stop accepting
	// messages while a read that has already popped events is still pending.
	readsMu    sync.Mutex
	reads      int
	readsDone  chan struct{} // closed once stopping and no read is in flight
	readsClose sync.Once

	chStopped chan struct{}
	wgReaders sync.WaitGroup
	doStop    *utils.Event
	cancel    context.CancelFunc

	closeOnce sync.Once
	closeErr  error

	ctx context.Context
}

// NewPrismaSaaSAdapter creates an adapter wired to LimaCharlie.
func NewPrismaSaaSAdapter(ctx context.Context, conf PrismaSaaSConfig) (*PrismaSaaSAdapter, chan struct{}, error) {
	return newPrismaSaaSAdapter(ctx, conf, nil)
}

// newPrismaSaaSAdapter is the implementation behind NewPrismaSaaSAdapter. A
// non-nil sink replaces the real LimaCharlie client (test seam).
func newPrismaSaaSAdapter(ctx context.Context, conf PrismaSaaSConfig, sink uspSink) (*PrismaSaaSAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &PrismaSaaSAdapter{
		conf:   conf,
		doStop: utils.NewEvent(),
	}
	a.useBulk.Store(true)

	if sink != nil {
		a.uspClient = sink
	} else {
		c, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
		a.uspClient = c
	}

	// Reads are not cancelled the moment Close is called: a response already
	// in flight carries events that have left PAN's queue, so it is given
	// time to arrive and be shipped. The context is cancelled after the grace
	// period, or when the parent context ends.
	a.ctx, a.cancel = context.WithCancel(ctx)
	a.api = newAPIClient(conf.resolveBaseURL(), conf.ClientID, conf.ClientSecret, conf.MaxConcurrentPolls)
	a.chStopped = make(chan struct{})
	a.readsDone = make(chan struct{})

	for i := 0; i < conf.MaxConcurrentPolls; i++ {
		a.wgReaders.Add(1)
		go a.runReader()
	}

	go func() {
		a.doStop.Wait()
		select {
		case <-time.After(a.conf.shutdownGrace):
			a.cancel()
		case <-a.chStopped:
		}
	}()
	go func() {
		a.wgReaders.Wait()
		a.cancel()
		close(a.chStopped)
	}()

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("prisma_saas: started %d readers against %s", conf.MaxConcurrentPolls, conf.resolveBaseURL()))
	return a, a.chStopped, nil
}

// Close stops the adapter. Events already read are shipped first. It is
// idempotent: repeated calls return the result of the first.
//
// Ordering matters: a read that returns after the USP client stopped accepting
// messages would pop events from PAN's queue and then have nowhere to put them.
// So Close first stops new reads and waits for the in-flight ones (cancelled
// after shutdownGrace), and only then arms the timer that seals the USP client
// against readers stuck in Ship.
func (a *PrismaSaaSAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.conf.ClientOptions.DebugLog("prisma_saas: closing")
		a.doStop.Set()
		a.markStopping()
		<-a.readsDone
		stopProducers := utils.StopBlockedProducersAfter(a.uspClient, a.conf.producerStopGrace)
		defer stopProducers()
		a.wgReaders.Wait()
		err1 := a.uspClient.Drain(1 * time.Minute)
		_, err2 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		a.api.Close()
		if err1 != nil {
			a.closeErr = err1
		} else {
			a.closeErr = err2
		}
	})
	return a.closeErr
}

// beginRead registers an in-flight read. It returns false once the adapter is
// stopping, in which case no read may start.
func (a *PrismaSaaSAdapter) beginRead() bool {
	a.readsMu.Lock()
	defer a.readsMu.Unlock()
	if a.doStop.IsSet() {
		return false
	}
	a.reads++
	return true
}

// endRead unregisters a read, after its events (if any) are in hand.
func (a *PrismaSaaSAdapter) endRead() {
	a.readsMu.Lock()
	defer a.readsMu.Unlock()
	a.reads--
	a.signalReadsDoneLocked()
}

// markStopping records that no new read may start; readsDone closes once the
// last in-flight read has returned.
func (a *PrismaSaaSAdapter) markStopping() {
	a.readsMu.Lock()
	defer a.readsMu.Unlock()
	a.signalReadsDoneLocked()
}

func (a *PrismaSaaSAdapter) signalReadsDoneLocked() {
	if a.doStop.IsSet() && a.reads == 0 {
		a.readsClose.Do(func() { close(a.readsDone) })
	}
}

// runReader is one reader loop. Each reader reads a batch, ships all of it,
// and only then reads again, so at most MaxConcurrentPolls batches are ever
// off PAN's queue but not yet handed to the USP client.
func (a *PrismaSaaSAdapter) runReader() {
	defer a.wgReaders.Done()

	failures := 0
	for !a.doStop.IsSet() {
		bulk := a.useBulk.Load()
		path := singleEventPath
		if bulk {
			path = bulkEventsPath
		}

		if !a.beginRead() {
			return
		}
		started := time.Now()
		batch, err := a.api.fetchEvents(a.ctx, path, bulk)
		a.endRead()
		if err != nil {
			if a.doStop.IsSet() || a.ctx.Err() != nil {
				return
			}
			if bulk && isNotFound(err) {
				if a.useBulk.CompareAndSwap(true, false) {
					a.conf.ClientOptions.OnWarning("prisma_saas: log_events_bulk not available (404), falling back to the single-event log_events endpoint")
				}
				continue
			}
			if !isTransientError(err) {
				a.conf.ClientOptions.OnError(fmt.Errorf("prisma_saas: stopping: %v", err))
				a.doStop.Set()
				return
			}
			delay := a.backoff(failures, retryAfterOf(err))
			failures++
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("prisma_saas: transient error, retrying in %v: %v", delay.Round(time.Millisecond), err))
			if a.doStop.WaitFor(delay) {
				return
			}
			continue
		}
		failures = 0

		if batch.skipped > 0 {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("prisma_saas: skipped %d non-object element(s) in a response", batch.skipped))
		}
		if len(batch.events) == 0 {
			// Nothing was queued. The bulk endpoint answers at once, so back
			// off; the single-event endpoint holds the request for ~20s, so
			// poll again immediately unless it came back suspiciously fast.
			if bulk || time.Since(started) < minLongPollLength {
				if a.doStop.WaitFor(a.conf.emptyWait) {
					return
				}
			}
			continue
		}

		for _, ev := range batch.events {
			if !a.ship(ev) {
				return
			}
		}
	}
}

// backoff returns the delay before retry number n (0-based): exponential from
// retryBaseDelay up to retryMaxDelay with jitter, never shorter than a
// server-requested Retry-After (itself capped at maxRetryAfter).
func (a *PrismaSaaSAdapter) backoff(n int, retryAfter time.Duration) time.Duration {
	if n > 20 {
		n = 20
	}
	d := a.conf.retryBaseDelay << uint(n)
	if d <= 0 || d > a.conf.retryMaxDelay {
		d = a.conf.retryMaxDelay
	}
	d += time.Duration(rand.Int63n(int64(d)/10 + 1))
	if retryAfter > maxRetryAfter {
		retryAfter = maxRetryAfter
	}
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

// ship forwards one event to LimaCharlie. A full buffer is waited out rather
// than dropping the event, since it has already left PAN's queue. It returns
// false if the adapter should stop.
func (a *PrismaSaaSAdapter) ship(ev utils.Dict) bool {
	msg := &protocol.DataMessage{
		JsonPayload: ev,
		EventType:   eventType(ev),
		TimestampMs: eventTime(ev),
	}
	warned := false
	for {
		err := a.uspClient.Ship(msg, shipTimeout)
		if err == nil {
			return true
		}
		if errors.Is(err, uspclient.ErrorBufferFull) {
			if !warned {
				a.conf.ClientOptions.OnWarning("prisma_saas: stream falling behind, holding events until there is room")
				warned = true
			}
			if a.ctx.Err() != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("prisma_saas: shutting down with a %q event unshipped", msg.EventType))
				a.doStop.Set()
				return false
			}
			continue
		}
		if errors.Is(err, uspclient.ErrorClosed) {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("prisma_saas: client closed with a %q event unshipped", msg.EventType))
		} else {
			a.conf.ClientOptions.OnError(fmt.Errorf("prisma_saas: Ship(): %v", err))
		}
		a.doStop.Set()
		return false
	}
}

// eventType returns the event's log_type, which names its kind
// (activity_monitoring, incident, remediation, policy_violation, admin_audit).
func eventType(ev utils.Dict) string {
	if s, ok := ev[eventTypeField].(string); ok && s != "" {
		return s
	}
	return unknownEventType
}

// eventTime parses the event's ISO8601 timestamp, falling back to now.
func eventTime(ev utils.Dict) uint64 {
	if s, ok := ev[timestampField].(string); ok {
		s = strings.TrimSpace(s)
		for _, layout := range timestampLayouts {
			if t, err := time.Parse(layout, s); err == nil {
				return uint64(t.UnixMilli())
			}
		}
	}
	return uint64(time.Now().UnixMilli())
}
