package usp_imap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

var (
	ErrTimeout = errors.New("timeout")
)

type IMAPAdapter struct {
	conf      ImapConfig
	uspClient *uspclient.Client

	imapClient *client.Client
	chStop     chan struct{}
	chStopped  chan struct{}

	ctx context.Context
}

type ImapConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Server        string                  `json:"server" yaml:"server"`
	UserName      string                  `json:"username" yaml:"username"`
	Password      string                  `json:"password" yaml:"password"`
	InboxName     string                  `json:"inbox_name" yaml:"inbox_name"`
	FromZero	  bool                    `json:"from_zero" yaml:"from_zero"`
}

func (c *ImapConfig) Validate() error {
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
	return nil
}

func NewImapAdapter(conf ImapConfig) (*IMAPAdapter, chan struct{}, error) {
	a := &IMAPAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	a.conf.ClientOptions.DebugLog("connecting")

	// Connect to the server.
	var err error
	// a.imapClient, err = client.DialTLS(a.conf.Server, nil)
	a.imapClient, err = client.Dial(a.conf.Server)
	if err != nil {
		a.conf.ClientOptions.DebugLog("failed to connect")
		return nil, nil, err
	}

	a.conf.ClientOptions.DebugLog("connected")

	// Login
	if err := a.imapClient.Login(a.conf.UserName, a.conf.Password); err != nil {
		a.imapClient.Logout()
		a.imapClient.Close()
		return nil, nil, err
	}

	a.conf.ClientOptions.DebugLog("logged in")

	// Open a mailbox
	// Inbox name defaults to "INBOX".
	if a.conf.InboxName == "" {
		a.conf.InboxName = "INBOX"
	}
	_, err = a.imapClient.Select(a.conf.InboxName, false)
	if err != nil {
		a.imapClient.Logout()
		a.imapClient.Close()
		return nil, nil, err
	}

	a.conf.ClientOptions.DebugLog("selected inbox")

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
	a.conf.ClientOptions.DebugLog("waiting for stop")
	err1 := a.imapClient.Logout()
	a.conf.ClientOptions.DebugLog("logged out")
	err2 := a.imapClient.Close()
	a.conf.ClientOptions.DebugLog("closed")
	err3 := a.uspClient.Drain(1 * time.Minute)
	a.conf.ClientOptions.DebugLog("drained")
	_, err4 := a.uspClient.Close()
	a.conf.ClientOptions.DebugLog("closed USP client")

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
	defer func() {
		close(a.chStopped)
		a.conf.ClientOptions.DebugLog("stopped")
	}()

	a.conf.ClientOptions.DebugLog("starting")

	// Start by FETCHing the last UID
	// Do this by fetching the UIDs of messages in the mailbox
	// for the last day (to limit the data we get)
	lastUID, err := a.getLastUID()
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("getLastUID(): %v", err))
		return
	}
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching last UID: %d", lastUID))

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
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching messages: %s", seqSet.String()))
		messages := make(chan *imap.Message, 10)
		done := make(chan error, 1)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			done <- a.imapClient.Fetch(seqSet, append([]imap.FetchItem{imap.FetchBodyStructure, imap.FetchUid}, imap.FetchFull.Expand()...), messages)
		}()
		go func() {
			defer wg.Done()
			hadError := false
			for msg := range messages {
				if hadError {
					// If we had an error, we just want to drain the channel
					continue
				}
				select {
				case <- a.chStop:
					continue
				default:
				}
				if err := a.processEvent(a.ctx, msg); err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("processEvent(): %v", err))
					hadError = true
					continue
				}
				// Update the last UID
				lastUID = msg.Uid
			}
		}()
		if err := a.waitForErrorFromChanForDuration(a.chStopped, done, 10*time.Minute); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("next.Fetch(): %v", err))
			return
		}
		wg.Wait()
	}
}

func (a *IMAPAdapter) getLastUID() (uint32, error) {
	// If we're starting from zero, return 0
	if a.conf.FromZero {
		return 0, nil
	}
	sc := imap.NewSearchCriteria()
	sc.Since = time.Now().Add(-24 * time.Hour)
	seqs, err := a.imapClient.Search(sc)
	if err != nil {
		return 0, fmt.Errorf("initial.Search(): %v", err)
	}
	seqSet := &imap.SeqSet{}
	seqSet.AddNum(seqs...)
	messages := make(chan *imap.Message, 1)
	done := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		done <- a.imapClient.Fetch(seqSet, []imap.FetchItem{imap.FetchUid}, messages)
	}()
	lastUID := uint32(0)
	go func() {
		defer wg.Done()
		for msg := range messages {
			lastUID = msg.Uid
		}
	}()
	if err := a.waitForErrorFromChanForDuration(a.chStopped, done, 10*time.Second); err != nil {
		return 0, fmt.Errorf("initial.Fetch(): %v", err)
	}
	wg.Wait()
	return lastUID, nil
}

func (a *IMAPAdapter)waitForErrorFromChanForDuration(chStopped chan struct{}, ch chan error, duration time.Duration) error {
	select {
	case <-chStopped:
		return nil
	case err := <-ch:
		return err
	case <-time.After(duration):
		return ErrTimeout
	}
}

func (a *IMAPAdapter) processEvent(ctx context.Context, message *imap.Message) error {
	d, err := a.messageToJSON(message)
	if err != nil {
		return fmt.Errorf("messageToJSON(): %v", err)
	}
	
	if err := a.uspClient.Ship(d, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(d, 1*time.Hour)
		}
		if err != nil {
			return fmt.Errorf("Ship(): %v", err)
		}
	}
	return nil
}

func (a *IMAPAdapter) messageToJSON(message *imap.Message) (*protocol.DataMessage, error) {
	j := utils.Dict{
		"imap": utils.Dict{
			"uid": message.Uid,
			"internal_date": message.InternalDate.UnixMilli(),
			"flags": message.Flags,
			"size": message.Size,
		},
	}
	if message.Envelope != nil {
		j["envelope"] = convertEnvelope(message.Envelope)
	}
	if message.BodyStructure != nil {
		j["body_structure"] = convertBodyStructure(message.BodyStructure)
	}
	d, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	msg := &protocol.DataMessage{
		TextPayload: string(d),
		TimestampMs: uint64(time.Now().UnixMilli()),
		EventType: "email",
	}
	return msg, nil
}

func imapAddressesToJSON(addrs []*imap.Address) []utils.Dict {
	to := make([]utils.Dict, len(addrs))
	for i, f := range addrs {
		to[i] = utils.Dict{
			"personal_name": f.PersonalName,
			"mailbox_name": f.MailboxName,
			"host_name": f.HostName,
			"at_domain_list": f.AtDomainList,
		}
	}
	return to
}

func convertEnvelope(e *imap.Envelope) utils.Dict {
	return utils.Dict{
		"date": e.Date.UnixMilli(),
		"subject": e.Subject,
		"from": imapAddressesToJSON(e.From),
		"reply_to": imapAddressesToJSON(e.ReplyTo),
		"sender": imapAddressesToJSON(e.Sender),
		"to": imapAddressesToJSON(e.To),
		"cc": imapAddressesToJSON(e.Cc),
		"bcc": imapAddressesToJSON(e.Bcc),
		"in_reply_to": e.InReplyTo,
		"message_id": e.MessageId,
	}
}

func convertBodyStructure(bs *imap.BodyStructure) utils.Dict {
	d := utils.Dict{
		"mime_type": bs.MIMEType,
		"mime_subtype": bs.MIMESubType,
		"params": bs.Params,
		"id": bs.Id,
		"description": bs.Description,
		"encoding": bs.Encoding,
		"size": bs.Size,
		"extended": bs.Extended,
		"disposition": bs.Disposition,
		"disposition_params": bs.DispositionParams,
		"language": bs.Language,
		"location": bs.Location,
		"md5": bs.MD5,
	}
	parts := make([]utils.Dict, len(bs.Parts))
	for i, p := range bs.Parts {
		parts[i] = convertBodyStructure(p)
	}
	d["parts"] = parts
	if bs.BodyStructure != nil {
		d["body_structure"] = convertBodyStructure(bs.BodyStructure)
	}
	if bs.Envelope != nil {
		d["envelope"] = convertEnvelope(bs.Envelope)
	}
	return d
}