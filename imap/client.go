package usp_imap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-message/mail"

	"github.com/refractionPOINT/go-limacharlie/limacharlie"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
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

	artifactUploader *limacharlie.Organization
	wgArtifacts      sync.WaitGroup

	ctx context.Context
}

type ImapConfig struct {
	ClientOptions           uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Server                  string                  `json:"server" yaml:"server"`
	UserName                string                  `json:"username" yaml:"username"`
	Password                string                  `json:"password" yaml:"password"`
	InboxName               string                  `json:"inbox_name" yaml:"inbox_name"`
	IsInsecure              bool                    `json:"is_insecure" yaml:"is_insecure"`
	FromZero                bool                    `json:"from_zero" yaml:"from_zero"`
	IncludeAttachments      bool                    `json:"include_attachments" yaml:"include_attachments"`
	MaxBodySize             int                     `json:"max_body_size" yaml:"max_body_size"`
	AttachmentIngestKey     string                  `json:"attachment_ingest_key" yaml:"attachment_ingest_key"`
	AttachmentRetentionDays int                     `json:"attachment_retention_days" yaml:"attachment_retention_days"`
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
	if a.conf.IsInsecure {
		a.imapClient, err = client.Dial(a.conf.Server)
	} else {
		a.imapClient, err = client.DialTLS(a.conf.Server, nil)
	}
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

	if a.conf.IncludeAttachments && a.conf.AttachmentIngestKey != "" {
		a.artifactUploader, err = limacharlie.NewOrganizationFromClientOptions(limacharlie.ClientOptions{
			OID: a.conf.ClientOptions.Identity.Oid,
		}, &limacharlie.LCLoggerGCP{})
		if err != nil {
			a.imapClient.Logout()
			a.imapClient.Close()
			return nil, nil, fmt.Errorf("artifactUploader: %v", err)
		}
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
	a.conf.ClientOptions.DebugLog("waiting for stop")
	err1 := a.imapClient.Logout()
	a.conf.ClientOptions.DebugLog("logged out")
	err2 := a.imapClient.Close()
	a.conf.ClientOptions.DebugLog("closed")
	err3 := a.uspClient.Drain(1 * time.Minute)
	a.conf.ClientOptions.DebugLog("drained")
	_, err4 := a.uspClient.Close()
	a.conf.ClientOptions.DebugLog("closed USP client")
	a.wgArtifacts.Wait()

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
	isFirstRun := true
	for {
		select {
		case <-a.chStop:
			return
		default:
			if isFirstRun {
				isFirstRun = false
			} else {
				time.Sleep(10 * time.Second)
			}
		}

		if _, err = a.imapClient.Select(a.conf.InboxName, false); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Select(): %v", err))
			return
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
			toFetch := append([]imap.FetchItem{
				imap.FetchBodyStructure,
				imap.FetchUid,
				imap.FetchFlags,
				imap.FetchRFC822Size,
				imap.FetchRFC822Text,
			})
			if a.conf.IncludeAttachments {
				bsn := &imap.BodySectionName{}
				toFetch = append(toFetch, imap.FetchFull.Expand()...)
				toFetch = append(toFetch, bsn.FetchItem())
			}
			done <- a.imapClient.Fetch(seqSet, toFetch, messages)
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
				case <-a.chStop:
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

func (a *IMAPAdapter) waitForErrorFromChanForDuration(chStopped chan struct{}, ch chan error, duration time.Duration) error {
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
	// For INCLUDE ATTACHMENT, use the "" body part.
	// For WITHOUT, use the "TEXT" body part.
	body := ""
	if a.conf.IncludeAttachments {
		if a.conf.MaxBodySize == 0 || len(message.Body) <= a.conf.MaxBodySize {
			for bpn, bp := range message.Body {
				// We only want the "" body part.
				if bpn.Specifier != "" {
					continue
				}
				fullBody, err := io.ReadAll(bp)
				if err != nil {
					return nil, fmt.Errorf("ReadAll(): %v", err)
				}
				if a.conf.AttachmentIngestKey != "" {
					mr, err := mail.CreateReader(bytes.NewReader(fullBody))
					if err != nil {
						return nil, fmt.Errorf("CreateReader(): %v", err)
					}
					for {
						part, err := mr.NextPart()
						if err == io.EOF {
							break
						} else if err != nil {
							mr.Close()
							return nil, fmt.Errorf("NextPart(): %v", err)
						}
						isAttachment := false
						attachmentName := ""
						if ah, ok := part.Header.(*mail.InlineHeader); ok {
							for k, v := range ah.Map() {
								if strings.ToLower(k) == "content-type" {
									for _, vv := range v {
										if !strings.Contains(strings.ToLower(vv), "text/html") && !strings.Contains(strings.ToLower(vv), "text/plain") {
											isAttachment = true
											break
										}
									}
								} else if strings.ToLower(k) == "content-disposition" {
									// Try to get the attachment name from the content-disposition header.
									for _, vv := range v {
										vv = strings.ToLower(vv)
										// Get the index of filename= to extract the attachment name.
										if strings.Contains(vv, "filename=") {
											attachmentName = strings.Trim(vv[strings.Index(vv, "filename=")+9:], "\"")
											break
										}
									}
								}
							}
						} else if ah, ok := part.Header.(*mail.AttachmentHeader); ok {
							for k, v := range ah.Map() {
								if strings.ToLower(k) == "content-type" {
									for _, vv := range v {
										if !strings.Contains(strings.ToLower(vv), "text/html") && !strings.Contains(strings.ToLower(vv), "text/plain") {
											isAttachment = true
											break
										}
									}
								} else if strings.ToLower(k) == "content-disposition" {
									// Try to get the attachment name from the content-disposition header.
									for _, vv := range v {
										vv = strings.ToLower(vv)
										// Get the index of filename= to extract the attachment name.
										if strings.Contains(vv, "filename=") {
											attachmentName = strings.Trim(vv[strings.Index(vv, "filename=")+9:], "\"")
											break
										}
									}
								}
							}
						}
						if !isAttachment {
							continue
						}
						body, err := io.ReadAll(part.Body)
						if err != nil {
							mr.Close()
							return nil, fmt.Errorf("ReadAll(): %v", err)
						}
						// Upload the attachment as an artifact to LC.
						a.wgArtifacts.Add(1)
						go func() {
							defer a.wgArtifacts.Done()

							a.conf.ClientOptions.DebugLog(fmt.Sprintf("Uploading attachment: %s", attachmentName))
							defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("Uploaded attachment: %s", attachmentName))

							// Hash the attachment with sha256 and use it as the artifact id.
							hash := sha256.Sum256(body)
							artifactID := hex.EncodeToString(hash[:])
							// Upload the attachment.
							if err := a.artifactUploader.UploadArtifact(bytes.NewBuffer(body), int64(len(body)), "attachment", a.conf.ClientOptions.Hostname, artifactID, attachmentName, a.conf.AttachmentRetentionDays, a.conf.AttachmentIngestKey); err != nil {
								mr.Close()
								a.conf.ClientOptions.OnError(fmt.Errorf("CreateArtifactFromBytes(): %v", err))
							}
						}()
					}
					mr.Close()
				}

				body = string(fullBody)
			}
		}
	} else {
		if a.conf.MaxBodySize == 0 || len(message.Body) <= a.conf.MaxBodySize {
			for bpn, bp := range message.Body {
				// We only want the "TEXT" body part.
				if bpn.Specifier != "TEXT" {
					continue
				}
				data, err := io.ReadAll(bp)
				if err != nil {
					return nil, err
				}
				body = string(data)
				break
			}
		}
	}
	msg := &protocol.DataMessage{
		TextPayload: body,
		TimestampMs: uint64(time.Now().UnixMilli()),
	}
	return msg, nil
}
