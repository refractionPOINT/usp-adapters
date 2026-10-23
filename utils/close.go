package utils

import (
	"errors"
	"fmt"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

type clientCloser interface {
	Close() ([]*protocol.DataMessage, error)
}

// UnackedMessagesError reports a shutdown which could not confirm all messages.
// Payloads are excluded because error callbacks may send these to logs. An
// adapter cannot generically replay data its source already acknowledged or
// checkpointed, so it reports uncertain delivery rather than hiding it.
type UnackedMessagesError struct {
	Count             int
	FirstSeq, LastSeq uint64
}

func (e *UnackedMessagesError) Error() string {
	return fmt.Sprintf("adapter shutdown: %d unacknowledged messages (sequences %d..%d); delivery uncertain", e.Count, e.FirstSeq, e.LastSeq)
}

// CloseUSP closes a previously drained client and reports unconfirmed delivery
// through its return value and OnWarning, even when a source cleanup error takes
// precedence in the adapter's Close result.
func CloseUSP(c clientCloser, opts uspclient.ClientOptions) ([]*protocol.DataMessage, error) {
	messages, err := c.Close()
	if len(messages) != 0 {
		pending := &UnackedMessagesError{Count: len(messages), FirstSeq: messages[0].SeqNum, LastSeq: messages[len(messages)-1].SeqNum}
		if opts.OnWarning != nil {
			opts.OnWarning(pending.Error())
		}
		err = errors.Join(err, pending)
	}
	return messages, err
}

// DrainCloseUSP is for adapters which previously closed without a drain.
func DrainCloseUSP(c interface {
	clientCloser
	Drain(timeout time.Duration) error
}, opts uspclient.ClientOptions) ([]*protocol.DataMessage, error) {
	drainErr := c.Drain(time.Minute)
	messages, closeErr := CloseUSP(c, opts)
	return messages, errors.Join(drainErr, closeErr)
}

// StopBlockedProducers lets healthy in-flight producers finish, while ensuring
// a producer stuck in Ship cannot hold shutdown forever. Close cancels the
// returned timer once producers have joined and the client has been drained.
// The adapter must signal its own producer stop immediately after calling this.
func StopBlockedProducers(c interface{}) func() {
	return stopBlockedProducersAfter(c, 10*time.Second)
}

func stopBlockedProducersAfter(c interface{}, grace time.Duration) func() {
	client, ok := c.(interface{ BeginDrain() })
	if !ok {
		return func() {}
	}
	timer := time.AfterFunc(grace, client.BeginDrain)
	return func() { timer.Stop() }
}
