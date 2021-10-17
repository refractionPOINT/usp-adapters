package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/s3"
	"github.com/refractionPOINT/usp-adapters/stdin"
	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/utils"

	"gopkg.in/yaml.v2"
)

type USPClient interface {
	Close() error
}

type GeneralConfigs struct {
	Syslog usp_syslog.SyslogConfig `json:"syslog" yaml:"syslog"`
	PubSub usp_pubsub.PubSubConfig `json:"pubsub" yaml:"pubsub"`
	S3     usp_s3.S3Config         `json:"s3" yaml:"s3"`
	Stdin  usp_stdin.StdinConfig   `json:"stdin" yaml:"stdin"`
}

func logError(format string, elems ...interface{}) {
	os.Stderr.Write([]byte(fmt.Sprintf(format+"\n", elems...)))
}

func log(format string, elems ...interface{}) {
	fmt.Printf(format+"\n", elems...)
}

func printUsage() {
	logError("Usage: ./adapter adapter_type [config_file.yaml | <param>...]")
}

func printConfig(adapterType string, c interface{}) {
	b, _ := yaml.Marshal(c)
	log("Configs in use (%s):\n----------------------------------\n%s\n----------------------------------\n", adapterType, string(b))
}

func main() {
	log("starting")
	configs := GeneralConfigs{}
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	adapterType := os.Args[1]
	if len(os.Args) == 3 {
		// Read the config from disk.
		f, err := os.Open(os.Args[2])
		if err != nil {
			logError("os.Open(): %v", err)
			printUsage()
			os.Exit(1)
		}
		b, err := io.ReadAll(f)
		if err != nil {
			logError("io.ReadAll(): %v", err)
			printUsage()
			os.Exit(1)
		}
		if err := json.Unmarshal(b, &configs); err != nil {
			err2 := yaml.Unmarshal(b, &configs)
			if err2 != nil {
				logError("json.Unmarshal(): %v", err)
				logError("yaml.Unmarshal(): %v", err2)
				printUsage()
				os.Exit(1)
			}
		}
	} else {
		// Read the config from the CLI.
		if err := utils.ParseCLI(os.Args[1], os.Args[2:], &configs); err != nil {
			logError("ParseCLI(): %v", err)
			printUsage()
			os.Exit(1)
		}
		// Read the config from the Env.
		if err := utils.ParseCLI(os.Args[1], os.Environ(), &configs); err != nil {
			logError("ParseEnv(): %v", err)
			printUsage()
			os.Exit(1)
		}
	}

	// Syslog
	configs.Syslog.ClientOptions.DebugLog = func(msg string) {
		log(msg)
	}
	configs.Syslog.ClientOptions.BufferOptions.BufferCapacity = 4096
	configs.Syslog.ClientOptions.BufferOptions.OnBackPressure = func() {
		log("experiencing back pressure")
	}

	// Pubsub
	configs.PubSub.ClientOptions.DebugLog = func(msg string) {
		log(msg)
	}
	configs.PubSub.ClientOptions.BufferOptions.BufferCapacity = 4096
	configs.PubSub.ClientOptions.BufferOptions.OnBackPressure = func() {
		log("experiencing back pressure")
	}

	// S3
	configs.S3.ClientOptions.DebugLog = func(msg string) {
		log(msg)
	}
	configs.S3.ClientOptions.BufferOptions.BufferCapacity = 4096
	configs.S3.ClientOptions.BufferOptions.OnBackPressure = func() {
		log("experiencing back pressure")
	}

	// Stdin
	configs.Stdin.ClientOptions.DebugLog = func(msg string) {
		log(msg)
	}
	configs.Stdin.ClientOptions.BufferOptions.BufferCapacity = 4096
	configs.Stdin.ClientOptions.BufferOptions.OnBackPressure = func() {
		log("experiencing back pressure")
	}

	// Enforce the usp_adapter Architecture on all configs.
	configs.Syslog.ClientOptions.Architecture = "usp_adapter"
	configs.PubSub.ClientOptions.Architecture = "usp_adapter"
	configs.S3.ClientOptions.Architecture = "usp_adapter"
	configs.Stdin.ClientOptions.Architecture = "usp_adapter"

	var client USPClient
	var chRunning chan struct{}
	var err error

	if adapterType == "syslog" {
		printConfig(adapterType, configs.Syslog)
		client, chRunning, err = usp_syslog.NewSyslogAdapter(configs.Syslog)
	} else if adapterType == "pubsub" {
		printConfig(adapterType, configs.PubSub)
		client, chRunning, err = usp_pubsub.NewPubSubAdapter(configs.PubSub)
	} else if adapterType == "s3" {
		printConfig(adapterType, configs.S3)
		client, chRunning, err = usp_s3.NewS3Adapter(configs.S3)
	} else if adapterType == "stdin" {
		printConfig(adapterType, configs.Stdin)
		client, chRunning, err = usp_stdin.NewStdinAdapter(configs.Stdin)
	} else {
		logError("unknown adapter_type: %s", adapterType)
		os.Exit(1)
	}

	if err != nil {
		logError("error instantiating client: %v", err)
		os.Exit(1)
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case <-osSignals:
		log("received signal to exit")
		break
	case <-chRunning:
		log("client stopped")
		break
	}

	if err := client.Close(); err != nil {
		logError("error closing client: %v", err)
		os.Exit(1)
	}
	log("exited")
}
