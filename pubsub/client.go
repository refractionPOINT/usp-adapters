package usp_pubsub

import (
	"context"
	"fmt"
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
	dbgLog    func(string)
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

func NewPubSubAdapter(conf PubSubConfig) (*PubSubAdapter, error) {
	a := &PubSubAdapter{
		conf: conf,
		dbgLog: func(s string) {
			if conf.ClientOptions.DebugLog == nil {
				return
			}
			conf.ClientOptions.DebugLog(s)
		},
		ctx: context.Background(),
	}

	var err error
	if a.conf.ServiceAccountCreds == "" {
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName); err != nil {
			return nil, err
		}
	} else {
		if a.psClient, err = pubsub.NewClient(a.ctx, a.conf.ProjectName, option.WithCredentialsJSON([]byte(a.conf.ServiceAccountCreds))); err != nil {
			return nil, err
		}
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		a.psClient.Close()
		return nil, err
	}

	a.buildSub = a.psClient.Subscription(a.conf.SubscriptionName)
	pubsubCtx, pubsubCancel := context.WithCancel(a.ctx)
	a.stopSub = pubsubCancel
	var subErr error
	go func() {
		for {
			if err := a.buildSub.Receive(pubsubCtx, a.processEvent); err != nil {
				a.dbgLog(fmt.Sprintf("buildSub.Receive: %v", err))
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
		return nil, subErr
	}

	return a, nil
}

func (a *PubSubAdapter) Close() error {
	a.dbgLog("closing")
	a.stopSub()
	err1 := a.psClient.Close()
	_, err2 := a.uspClient.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (a *PubSubAdapter) processEvent(ctx context.Context, message *pubsub.Message) {
	msg := &protocol.DataMessage{}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		message.Nack()
		if err == uspclient.ErrorBufferFull {
			a.dbgLog("stream falling behind")
			err = a.uspClient.Ship(msg, 0)
		}
		if err != nil {
			a.dbgLog(fmt.Sprintf("Ship(): %v", err))
		}
		return
	}
	message.Ack()
}
