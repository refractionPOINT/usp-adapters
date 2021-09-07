package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/utils"
)

type USPClient interface {
	Close() error
}

type GeneralConfigs struct {
	Syslog usp_syslog.SyslogConfig
}

func logError(format string, elems ...interface{}) {
	os.Stderr.Write([]byte(fmt.Sprintf(format+"\n", elems...)))
}

func main() {
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

	var client USPClient
	var err error

	if adapterType == "syslog" {
		client, err = usp_syslog.NewSyslogAdapter(configs.Syslog)
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

	if err := client.Close(); err != nil {
		logError("error closing client: %v", err)
		os.Exit(1)
	}
}
