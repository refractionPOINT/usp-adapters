package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/utils"
)

type USPClient interface {
	Close() error
}

type GeneralConfigs struct {
	IsDebug string                  `json:"debug" yaml:"debug"`
	Syslog  usp_syslog.SyslogConfig `json:"syslog" yaml:"syslog"`
	PubSub  usp_pubsub.PubSubConfig `json:"pubsub" yaml:"pubsub"`
}

func logError(format string, elems ...interface{}) {
	os.Stderr.Write([]byte(fmt.Sprintf(format+"\n", elems...)))
}

func log(format string, elems ...interface{}) {
	fmt.Printf(format+"\n", elems...)
}

func main() {
	log("starting")
	configs := GeneralConfigs{}
	if len(os.Args) <= 3 {
		logError("Usage: ./adapter adapter_type")
		os.Exit(1)
	}
	adapterType := os.Args[1]
	if err := utils.ParseCLI(os.Args[2:], &configs); err != nil {
		logError("ParseCLI(): %v", err)
		os.Exit(1)
	}

	if configs.IsDebug != "" {
		configs.Syslog.ClientOptions.DebugLog = func(msg string) {
			log(msg)
		}
	}

	var client USPClient
	var err error

	if adapterType == "syslog" {
		client, err = usp_syslog.NewSyslogAdapter(configs.Syslog)
	} else if adapterType == "pubsub" {
		client, err = usp_pubsub.NewPubSubAdapter(configs.PubSub)
	} else {
		logError("unknown adapter_type: %s", adapterType)
		os.Exit(1)
	}

	if err != nil {
		logError("error instantiating client: %v", err)
		os.Exit(1)
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, os.Kill, syscall.SIGTERM)
	_ = <-osSignals
	log("received signal to exit")

	if err := client.Close(); err != nil {
		logError("error closing client: %v", err)
		os.Exit(1)
	}
	log("exited")
}
