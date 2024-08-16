package usp_pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-imap/idle"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultWriteTimeout = 60 * 10
)

type IMAPAdapter struct {
	conf      PubSubConfig
	uspClient *uspclient.Client

	imapClient *client.Client
	chUpdates  chan client.Update
	chStop chan struct{}

	ctx      context.Context
}

type PubSubConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Server string `json:"server" yaml:"server"`
	UserName string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	InboxName string `json:"inbox_name" yaml:"inbox_name"`
	UseStartTLS bool `json:"use_starttls" yaml:"use_starttls"`
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
	if a.conf.UseStartTLS{
		a.imapClient, err = client.DialStartTLS(a.conf.Server, nil)
	} else {
		a.imapClient, err = client.DialTLS(a.conf.Server, nil)
	}
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

	// Create an IDLE client
	idleClient := idle.NewClient(a.imapClient)

	// Create a channel to receive mailbox updates
	updates := make(chan client.Update)
	a.imapClient.Updates = updates
	a.chUpdates = updates

	// Create channels for the life cycle of the IDLE client
	stop := make(chan struct{})
	chStopped := make(chan struct{})
	go func() {
		chStopped <- idleClient.IdleWithFallback(stop, 0)
	}()
	a.chStop = stop

	go func() {
		for {
			select {
			case update := <-updates:
				switch update.(type) {
				case *client.MailboxUpdate:
					log.Println("New message received")
					// Here you can fetch the new email or perform other actions
				}
			case <-time.After(29 * time.Minute):
				// Stop IDLE after 29 minutes as many servers close the connection after 30 minutes
				close(stop)
				if err := <-done; err != nil {
					log.Fatal(err)
				}
				return
			}
		}
	}()

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

func (a *IMAPAdapter) processEvent(ctx context.Context, message *client.MailboxUpdate) {
	msg := &protocol.DataMessage{
		TextPayload: string(message.Data),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			message.Nack()
			return
		}
	}
	message.Ack()
}
