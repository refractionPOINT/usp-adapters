package usp_demo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/demo/scenarios"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultPollingInterval = 1 * time.Minute
	defaultBackfillPeriod  = 1 * time.Hour
)

type DemoAdapter struct {
	conf      DemoConfig
	uspClient *uspclient.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context

	eventCount int64
	scenario   *scenarios.Scenario
}

type DemoConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	PollingInterval int                     `json:"polling_interval" yaml:"polling_interval"`
	Scenario        string                  `json:"scenario" yaml:"scenario"`
}

func (c *DemoConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Scenario != "" {
		if scenarios.Get(c.Scenario) == nil {
			available := strings.Join(scenarios.List(), ", ")
			return fmt.Errorf("unknown scenario '%s', available scenarios: %s", c.Scenario, available)
		}
	}
	return nil
}

func NewDemoAdapter(ctx context.Context, conf DemoConfig) (*DemoAdapter, chan struct{}, error) {
	var err error
	a := &DemoAdapter{
		conf:       conf,
		ctx:        ctx,
		doStop:     utils.NewEvent(),
		eventCount: 0,
	}

	// Load scenario if specified
	if conf.Scenario != "" {
		a.scenario = scenarios.Get(conf.Scenario)
		if a.scenario == nil {
			return nil, nil, fmt.Errorf("scenario '%s' not found", conf.Scenario)
		}
		conf.ClientOptions.DebugLog(fmt.Sprintf("using scenario: %s - %s", a.scenario.Name, a.scenario.Description))
	}

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.chStopped = make(chan struct{})

	if a.scenario != nil {
		// Start a goroutine for each event generator in the scenario
		for _, gen := range a.scenario.Generators {
			a.wgSenders.Add(1)
			go a.runGenerator(gen)
		}
	} else {
		// Default simple demo mode
		a.wgSenders.Add(1)
		go a.generateSimpleEvents()
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *DemoAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

// runGenerator runs a single event generator with its own interval
func (a *DemoAdapter) runGenerator(gen scenarios.EventGenerator) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("generator %s exiting", gen.Name))

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting generator %s with %v interval", gen.Name, gen.Interval))

	// Backfill events for this generator
	if !a.backfillGenerator(gen) {
		return
	}

	// Continue generating events at the specified interval
	sequence := int(defaultBackfillPeriod / gen.Interval)
	for !a.doStop.WaitFor(gen.Interval) {
		sequence++
		event := gen.Generate(time.Now(), sequence)
		if err := a.shipScenarioEvent(event, gen.Name); err != nil {
			return
		}
	}
}

func (a *DemoAdapter) backfillGenerator(gen scenarios.EventGenerator) bool {
	now := time.Now()
	backfillStart := now.Add(-defaultBackfillPeriod)

	// Calculate how many events to backfill for this generator
	numEvents := int(defaultBackfillPeriod / gen.Interval)
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("backfilling %d events for generator %s", numEvents, gen.Name))

	for i := 0; i < numEvents; i++ {
		if a.doStop.IsSet() {
			return false
		}

		eventTime := backfillStart.Add(time.Duration(i) * gen.Interval)
		event := gen.Generate(eventTime, i)
		if err := a.shipScenarioEvent(event, gen.Name); err != nil {
			return false
		}
	}

	return true
}

func (a *DemoAdapter) shipScenarioEvent(event scenarios.Event, genName string) error {
	count := atomic.AddInt64(&a.eventCount, 1)

	msg := &protocol.DataMessage{
		TimestampMs: uint64(event.Timestamp.UnixNano() / int64(time.Millisecond)),
		EventType:   event.EventType,
	}

	// Use text or JSON payload based on what's set
	if event.TextPayload != "" {
		msg.TextPayload = event.TextPayload
	} else if event.JsonPayload != nil {
		msg.JsonPayload = event.JsonPayload
	} else {
		return errors.New("event has no payload")
	}

	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			a.doStop.Set()
			return err
		}
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("sent event #%d from %s", count, genName))
	return nil
}

// generateSimpleEvents is the default mode when no scenario is specified
func (a *DemoAdapter) generateSimpleEvents() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("simple event generation exiting")

	pollingInterval := defaultPollingInterval
	if a.conf.PollingInterval > 0 {
		pollingInterval = time.Duration(a.conf.PollingInterval) * time.Second
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting simple demo event generation with %v interval", pollingInterval))

	// Backfill events for the last hour on startup
	if !a.backfillSimpleEvents(pollingInterval) {
		return
	}

	for !a.doStop.WaitFor(pollingInterval) {
		if err := a.shipSimpleEvent(time.Now()); err != nil {
			return
		}
	}
}

func (a *DemoAdapter) backfillSimpleEvents(pollingInterval time.Duration) bool {
	now := time.Now()
	backfillStart := now.Add(-defaultBackfillPeriod)

	// Calculate how many events to backfill
	numEvents := int(defaultBackfillPeriod / pollingInterval)
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("backfilling %d simple events from the last hour", numEvents))

	for i := 0; i < numEvents; i++ {
		if a.doStop.IsSet() {
			return false
		}

		eventTime := backfillStart.Add(time.Duration(i) * pollingInterval)
		if err := a.shipSimpleEvent(eventTime); err != nil {
			return false
		}
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("backfill complete, sent %d historical events", numEvents))
	return true
}

func (a *DemoAdapter) shipSimpleEvent(eventTime time.Time) error {
	count := atomic.AddInt64(&a.eventCount, 1)
	event := a.createSimpleEvent(eventTime, int(count))

	msg := &protocol.DataMessage{
		JsonPayload: event,
		TimestampMs: uint64(eventTime.UnixNano() / int64(time.Millisecond)),
	}

	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			a.doStop.Set()
			return err
		}
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("sent simple event #%d", count))
	return nil
}

func (a *DemoAdapter) createSimpleEvent(eventTime time.Time, sequence int) utils.Dict {
	return utils.Dict{
		"event_type": "demo_event",
		"timestamp":  eventTime.UTC().Format(time.RFC3339),
		"event_id":   fmt.Sprintf("demo-%d-%d", eventTime.UnixNano(), sequence),
		"message":    fmt.Sprintf("Demo event #%d generated at %s", sequence, eventTime.Format(time.RFC3339)),
		"source":     "demo_adapter",
	}
}
