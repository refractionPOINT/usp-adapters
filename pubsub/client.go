package usp_pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/credguard"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// receiveRestartDelay is how long to wait before restarting Receive after it
// returned while the adapter is still running. The wait doubles per
// consecutive failure up to receiveRestartMaxDelay; a Receive that ran for
// receiveHealthyAfter resets it. Variables only so tests can shorten them.
var (
	receiveRestartDelay    = 2 * time.Second
	receiveRestartMaxDelay = 5 * time.Minute
	receiveHealthyAfter    = 1 * time.Minute
)

const (
	defaultWriteTimeout = 60 * 10

	// shipRetrySlice is how long one retry of a blocked Ship may wait. The
	// retries are sliced so a message handler notices that the adapter is
	// closing without relying on the client to wake it.
	shipRetrySlice = 2 * time.Second
	// fallingBehindLogAfter is how long a handler waits for buffer space
	// before it logs that the stream is falling behind.
	fallingBehindLogAfter = 10 * time.Second
	// maxShipBlock is how long a message handler keeps retrying a Ship that
	// reports a full buffer before giving the message back to Pub/Sub.
	maxShipBlock = 1 * time.Hour
)

type PubSubAdapter struct {
	conf      PubSubConfig
	uspClient *uspclient.Client

	psClient *pubsub.Client

	ctx                 context.Context
	buildSub            *pubsub.Subscription
	stopSub             context.CancelFunc
	receiveStopped      <-chan struct{}
	receiveDrainTimeout time.Duration
}

type PubSubConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	SubscriptionName    string                  `json:"sub_name" yaml:"sub_name"`
	ProjectName         string                  `json:"project_name" yaml:"project_name"`
	ServiceAccountCreds string                  `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
	MaxPSBuffer         int                     `json:"max_ps_buffer,omitempty" yaml:"max_ps_buffer,omitempty"`
}

func (c *PubSubConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.SubscriptionName == "" {
		return errors.New("missing sub_name")
	}
	if c.ProjectName == "" {
		return errors.New("missing project_name")
	}
	c.ServiceAccountCreds = strings.TrimSpace(c.ServiceAccountCreds)
	if c.ServiceAccountCreds == "" {
		return errors.New("missing service_account_creds")
	}
	return nil
}

func NewPubSubAdapter(ctx context.Context, conf PubSubConfig) (*PubSubAdapter, chan struct{}, error) {
	a := &PubSubAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error
	if a.conf.ServiceAccountCreds == "" {
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName); err != nil {
			return nil, nil, err
		}
	} else if a.conf.ServiceAccountCreds == "-" {
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName, option.WithoutAuthentication()); err != nil {
			return nil, nil, err
		}
	} else {
		credOpt, cerr := credguard.CredentialOption(a.conf.ServiceAccountCreds)
		if cerr != nil {
			return nil, nil, cerr
		}
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName, credOpt); err != nil {
			return nil, nil, err
		}
	}

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		a.psClient.Close()
		return nil, nil, err
	}

	a.buildSub = a.psClient.Subscription(a.conf.SubscriptionName)
	if conf.MaxPSBuffer != 0 {
		a.buildSub.ReceiveSettings.MaxOutstandingBytes = conf.MaxPSBuffer
	}
	a.buildSub.ReceiveSettings.MaxExtension = 61 * time.Minute
	pubsubCtx, pubsubCancel := context.WithCancel(a.ctx)
	a.stopSub = pubsubCancel
	chStopped := make(chan struct{})
	a.receiveStopped = chStopped
	a.receiveDrainTimeout = 10 * time.Second
	var (
		subErrMu sync.Mutex
		subErr   error
	)
	go func() {
		defer close(chStopped)
		receiveWithRestarts(pubsubCtx, func(ctx context.Context) error {
			return a.buildSub.Receive(ctx, a.processEvent)
		}, func(err error, retryIn time.Duration) {
			if a.conf.ClientOptions.OnWarning != nil {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("buildSub.Receive: %v, restarting in %v", err, retryIn))
			}
			subErrMu.Lock()
			subErr = err
			subErrMu.Unlock()
		})
	}()
	// Give it a second to start the subscriber to check it's
	// working without any errors.
	time.Sleep(2 * time.Second)
	subErrMu.Lock()
	startErr := subErr
	subErrMu.Unlock()
	if startErr != nil {
		pubsubCancel()
		a.psClient.Close()
		a.uspClient.Close()
		return nil, nil, startErr
	}

	return a, chStopped, nil
}

// receiveWithRestarts runs receive until ctx is cancelled, restarting it each
// time it returns. Close() (or a failed start) cancels the context: that is
// the only way out, transient errors restart receive after a wait that grows
// while it keeps failing, so a persistent failure (e.g. TLS) is not retried
// and reported every couple of seconds.
func receiveWithRestarts(ctx context.Context, receive func(context.Context) error, onErr func(err error, retryIn time.Duration)) {
	failures := 0
	for {
		started := time.Now()
		err := receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) >= receiveHealthyAfter {
			failures = 0
		}
		failures++
		delay := receiveRestartDelay
		for i := 1; i < failures && delay < receiveRestartMaxDelay; i++ {
			delay *= 2
		}
		if delay > receiveRestartMaxDelay {
			delay = receiveRestartMaxDelay
		}
		if err != nil {
			onErr(err, delay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (a *PubSubAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.stopSub()
	var flushErr error
	if a.receiveStopped != nil {
		timer := time.NewTimer(a.receiveDrainTimeout)
		select {
		case <-a.receiveStopped:
			timer.Stop()
		case <-timer.C:
			flushErr = errors.New("timed out flushing Pub/Sub acknowledgements; source delivery may repeat")
			if a.conf.ClientOptions.OnWarning != nil {
				a.conf.ClientOptions.OnWarning(flushErr.Error())
			}
		}
	}
	// Receive flushes source ACK/NACKs before returning. Keep its RPC transport
	// alive until then; on the bounded fallback, Close aborts stuck source RPCs.
	err1 := a.psClient.Close()
	err2 := a.uspClient.Drain(1 * time.Minute)
	_, err3 := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)

	return errors.Join(flushErr, err1, err2, err3)
}

func (a *PubSubAdapter) processEvent(ctx context.Context, message *pubsub.Message) {
	msg := &protocol.DataMessage{
		TextPayload: string(message.Data),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.shipWhileRunning(ctx, msg); err != nil {
		// A closed client or a stopped subscription is the adapter shutting
		// down, not a failure: hand the message back quietly.
		if ctx.Err() == nil && !errors.Is(err, uspclient.ErrorClosed) {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
		}
		message.Nack()
		return
	}
	message.Ack()
}

// shipWhileRunning ships msg, waiting for room in the client's buffer for up
// to maxShipBlock, but gives up as soon as ctx is done or the client is closed
// (the adapter is closing). Pub/Sub's Receive waits for every message handler
// to return before it returns, so a handler stuck here would keep the adapter
// from stopping.
func (a *PubSubAdapter) shipWhileRunning(ctx context.Context, msg *protocol.DataMessage) error {
	blockedSince := time.Now()
	loggedBehind := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := a.uspClient.Ship(msg, shipRetrySlice)
		if err != uspclient.ErrorBufferFull {
			return err
		}
		if !loggedBehind && time.Since(blockedSince) >= fallingBehindLogAfter {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			loggedBehind = true
		}
		if time.Since(blockedSince) >= maxShipBlock {
			return err
		}
	}
}
