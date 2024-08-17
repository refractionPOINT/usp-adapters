package usp_pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultWriteTimeout = 60 * 10
)

var (
	ErrTimeout = errors.New("timeout")
)

type IMAPAdapter struct {
	conf      PubSubConfig
	uspClient *uspclient.Client

	imapClient *client.Client
	chStop     chan struct{}
	chStopped  chan struct{}

	ctx context.Context
}

type PubSubConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Server        string                  `json:"server" yaml:"server"`
	UserName      string                  `json:"username" yaml:"username"`
	Password      string                  `json:"password" yaml:"password"`
	InboxName     string                  `json:"inbox_name" yaml:"inbox_name"`
}

func (c *PubSubConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Server == "" {
		return errors.New("missing server")
	}
	if c.UserName == "" {
		return errors.New("missing username")
	}
	if c.Password == "" {
		return errors.New("missing password")
	}
	// Inbox name defaults to "INBOX".
	if c.InboxName == "" {
		c.InboxName = "INBOX"
	}
	return nil
}

func NewIMAPAdapter(conf PubSubConfig) (*IMAPAdapter, chan struct{}, error) {
	a := &IMAPAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	// Connect to the server.
	var err error
	a.imapClient, err = client.DialTLS(a.conf.Server, nil)
	if err != nil {
		return nil, nil, err
	}

	// Login
	if err := a.imapClient.Login(a.conf.UserName, a.conf.Password); err != nil {
		a.imapClient.Logout()
		a.imapClient.Close()
		return nil, nil, err
	}

	// Open a mailbox
	_, err = a.imapClient.Select(a.conf.InboxName, false)
	if err != nil {
		a.imapClient.Logout()
		a.imapClient.Close()
		return nil, nil, err
	}

	// Create the USP client to ship to LC
	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		a.imapClient.Logout()
		a.imapClient.Close()
		return nil, nil, err
	}

	// Create channels for the life cycle
	stop := make(chan struct{})
	chStopped := make(chan struct{})
	a.chStop = stop
	a.chStopped = chStopped

	// Start the connection handler
	go a.handleConnection()

	return a, chStopped, nil
}

func (a *IMAPAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	close(a.chStop)
	err1 := a.imapClient.Logout()
	err2 := a.imapClient.Close()
	err3 := a.uspClient.Drain(1 * time.Minute)
	_, err4 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	if err3 != nil {
		return err3
	}

	return err4
}

func (a *IMAPAdapter) handleConnection() {
	defer close(a.chStopped)

	// Start by FETCHing the last UID
	lastUID := uint32(0)
	seqSet := &imap.SeqSet{}
	seqSet.Add("$")
	messages := make(chan *imap.Message, 1)

	done := make(chan error, 1)
	go func() {
		done <- a.imapClient.Fetch(seqSet, []imap.FetchItem{imap.FetchUid}, messages)
	}()
	if err := waitForErrorFromChanForDuration(done, 10*time.Second); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("initial.Fetch(): %v", err))
		return
	}

	// Try to consume the last UID
	for msg := range messages {
		lastUID = msg.Uid
		break
	}

	// Then start polling every X seconds for new messages by searching for messages with UID > last UID
	// Bail if the chStop channel is closed
	for {
		select {
		case <-a.chStop:
			return
		default:
		}

		seqSet, err := imap.ParseSeqSet(fmt.Sprintf("%d:*", lastUID+1))
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("ParseSeqSet(): %v", err))
			return
		}
		messages := make(chan *imap.Message, 10)
		done := make(chan error, 1)
		go func() {
			done <- a.imapClient.Fetch(seqSet, []imap.FetchItem{imap.FetchAll}, messages)
		}()
		if err := waitForErrorFromChanForDuration(done, 10*time.Second); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("next.Fetch(): %v", err))
			return
		}

		for {
			isDone := false
			select {
			case <-a.chStop:
				return
			case msg := <-messages:
				if err := a.processEvent(a.ctx, msg); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("processEvent(): %v", err))
					return
				}
				// Update the last UID
				lastUID = msg.Uid
			default:
				isDone = true
			}
			if isDone {
				time.Sleep(10 * time.Second)
				break
			}
		}
	}
}

func waitForErrorFromChanForDuration(ch chan error, duration time.Duration) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(duration):
		return ErrTimeout
	}
}

func (a *IMAPAdapter) processEvent(ctx context.Context, message *imap.Message) error {
	j, err := a.messageToJSON(message)
	if err != nil {
		return fmt.Errorf("messageToJSON(): %v", err)
	}
	msg := &protocol.DataMessage{
		TextPayload: j,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			return fmt.Errorf("Ship(): %v", err)
		}
	}
	return nil
}

func (a *IMAPAdapter) messageToJSON(message *imap.Message) (string, error) {
	return "", nil
}
