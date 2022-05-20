package usp_pubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultWriteTimeout = 60 * 10
)

type PubSubAdapter struct {
	conf      PubSubConfig
	uspClient *uspclient.Client

	psClient *pubsub.Client

	ctx        context.Context
	buildTopic *pubsub.Topic
	buildSub   *pubsub.Subscription
	stopSub    context.CancelFunc
}

type PubSubConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	SubscriptionName    string                  `json:"sub_name" yaml:"sub_name"`
	ProjectName         string                  `json:"project_name" yaml:"project_name"`
	ServiceAccountCreds string                  `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
}

func NewPubSubAdapter(conf PubSubConfig) (*PubSubAdapter, chan struct{}, error) {
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
	} else if !strings.HasPrefix(a.conf.ServiceAccountCreds, "{") {
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName, option.WithCredentialsFile(a.conf.ServiceAccountCreds)); err != nil {
			return nil, nil, err
		}
	} else {
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName, option.WithCredentialsJSON([]byte(a.conf.ServiceAccountCreds))); err != nil {
			return nil, nil, err
		}
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		a.psClient.Close()
		return nil, nil, err
	}

	a.buildSub = a.psClient.Subscription(a.conf.SubscriptionName)
	pubsubCtx, pubsubCancel := context.WithCancel(a.ctx)
	a.stopSub = pubsubCancel
	chStopped := make(chan struct{})
	var subErr error
	go func() {
		defer close(chStopped)
		for {
			if err := a.buildSub.Receive(pubsubCtx, a.processEvent); err != nil {
				a.conf.ClientOptions.DebugLog(fmt.Sprintf("buildSub.Receive: %v", err))
				subErr = err
			}
			time.Sleep(2 * time.Second)
			if a.buildSub == nil {
				break
			}
		}
	}()
	// Give it a second to start the subscriber to check it's
	// working without any errors.
	time.Sleep(2 * time.Second)
	if subErr != nil {
		a.psClient.Close()
		a.uspClient.Close()
		return nil, nil, subErr
	}

	return a, chStopped, nil
}

func (a *PubSubAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.stopSub()
	err1 := a.psClient.Close()
	_, err2 := a.uspClient.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (a *PubSubAdapter) processEvent(ctx context.Context, message *pubsub.Message) {
	msg := &protocol.DataMessage{
		TextPayload: string(message.Data),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		message.Nack()
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(msg, 0)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			return
		}
	}
	message.Ack()
}
