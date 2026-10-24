package acktrack

import (
	"context"
	"sync"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// SDKClient keeps the original SDK lifecycle, including BeginDrain before joins.
// This interface and the mode adapter are internal construction details.
type SDKClient interface {
	Client
	BeginDrain()
	Drain(time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// NewClient preserves the SDK's explicit testing sink. That mode discards data,
// returns success without a SeqNum, and has no pending messages. Only there do
// we assign local admission numbers; these are not production delivery ACKs.
// Ordinary clients are returned unchanged and use the strict SDK/ACK contract.
func NewClient(ctx context.Context, options uspclient.ClientOptions) (SDKClient, error) {
	client, err := uspclient.NewClient(ctx, options)
	if err != nil {
		return nil, err
	}
	if !options.TestSinkMode {
		return client, nil
	}
	return &testSinkClient{Client: client}, nil
}

type testSinkClient struct {
	*uspclient.Client
	mu   sync.Mutex
	next uint64
}

func (c *testSinkClient) Ship(message *protocol.DataMessage, timeout time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.Client.Ship(message, timeout); err != nil {
		return err
	}
	c.next++
	message.SeqNum = c.next
	return nil
}
