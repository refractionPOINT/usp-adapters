package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/1password"
	"github.com/refractionPOINT/usp-adapters/azure_event_hub"
	"github.com/refractionPOINT/usp-adapters/o365"
	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/s3"
	"github.com/refractionPOINT/usp-adapters/stdin"
	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/refractionPOINT/usp-adapters/wel"

	"gopkg.in/yaml.v2"
)

type USPClient interface {
	Close() error
}

// These configs are top level and cannot
// be used by underlying adapters.
type RuntimeConfig struct {
	Healthcheck int `json:"healthcheck" yaml:"healthcheck"`
}

type GeneralConfigs struct {
	Syslog        usp_syslog.SyslogConfig            `json:"syslog" yaml:"syslog"`
	PubSub        usp_pubsub.PubSubConfig            `json:"pubsub" yaml:"pubsub"`
	S3            usp_s3.S3Config                    `json:"s3" yaml:"s3"`
	Stdin         usp_stdin.StdinConfig              `json:"stdin" yaml:"stdin"`
	OnePassword   usp_1password.OnePasswordConfig    `json:"1password" yaml:"1password"`
	Office365     usp_o365.Office365Config           `json:"office365" yaml:"office365"`
	Wel           usp_wel.WELConfig                  `json:"wel" yaml:"wel"`
	AzureEventHub usp_azure_event_hub.EventHubConfig `json:"azure_event_hub" yaml:"azure_event_hub"`
}

func logError(format string, elems ...interface{}) {
	os.Stderr.Write([]byte(fmt.Sprintf(format+"\n", elems...)))
}

func log(format string, elems ...interface{}) {
	fmt.Printf(format+"\n", elems...)
}

func printStruct(prefix string, s interface{}, isTop bool) {
	val := reflect.ValueOf(s)
	for i := 0; i < val.Type().NumField(); i++ {
		// logError("%#v", val.Type().Field(i))
		tag := val.Type().Field(i).Tag.Get("json")
		if tag == "-" {
			continue
		}
		components := strings.Split(tag, ",")
		p := components[0]
		if prefix != "" {
			p = fmt.Sprintf("%s.%s", prefix, p)
		}
		if isTop {
			logError("\nFor %s\n----------------------------------", p)
			p = ""
		}
		e := val.Field(i)
		if e.Kind() == reflect.Struct {
			printStruct(p, e.Interface(), false)
		} else {
			logError(p)
		}
	}
}

func printUsage() {
	logError("Usage: ./adapter adapter_type [config_file.yaml | <param>...]")
	logError("Available configs:\n")
	printStruct("", GeneralConfigs{}, true)
	logError("\nGlobal Runtime Options\n----------------------------------")
	printStruct("", RuntimeConfig{}, false)
}

func printConfig(adapterType string, c interface{}) {
	b, _ := yaml.Marshal(c)
	log("Configs in use (%s):\n----------------------------------\n%s----------------------------------\n", adapterType, string(b))
}

func main() {
	log("starting")
	runtimeConfigs := RuntimeConfig{}
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
		if err := json.Unmarshal(b, &runtimeConfigs); err != nil {
			err2 := yaml.Unmarshal(b, &runtimeConfigs)
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
		// Get the runtime configs.
		if err := utils.ParseCLI("", os.Args[2:], &runtimeConfigs); err != nil {
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
		// Get the runtime configs.
		if err := utils.ParseCLI("", os.Environ(), &runtimeConfigs); err != nil {
			logError("ParseEnv(): %v", err)
			printUsage()
			os.Exit(1)
		}
	}

	// Syslog
	configs.Syslog.ClientOptions = applyLogging(configs.Syslog.ClientOptions)

	// Pubsub
	configs.PubSub.ClientOptions = applyLogging(configs.PubSub.ClientOptions)

	// S3
	configs.S3.ClientOptions = applyLogging(configs.S3.ClientOptions)

	// Stdin
	configs.Stdin.ClientOptions = applyLogging(configs.Stdin.ClientOptions)

	// 1Password
	configs.OnePassword.ClientOptions = applyLogging(configs.OnePassword.ClientOptions)

	// Office365
	configs.Office365.ClientOptions = applyLogging(configs.Office365.ClientOptions)

	// Windows Event Logs
	configs.Wel.ClientOptions = applyLogging(configs.Wel.ClientOptions)

	// Azure Event Hub
	configs.AzureEventHub.ClientOptions = applyLogging(configs.AzureEventHub.ClientOptions)

	// Enforce the usp_adapter Architecture on all configs.
	configs.Syslog.ClientOptions.Architecture = "usp_adapter"
	configs.PubSub.ClientOptions.Architecture = "usp_adapter"
	configs.S3.ClientOptions.Architecture = "usp_adapter"
	configs.Stdin.ClientOptions.Architecture = "usp_adapter"
	configs.OnePassword.ClientOptions.Architecture = "usp_adapter"
	configs.Office365.ClientOptions.Architecture = "usp_adapter"
	configs.Wel.ClientOptions.Architecture = "usp_adapter"
	configs.AzureEventHub.ClientOptions.Architecture = "usp_adapter"

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
	} else if adapterType == "1password" {
		printConfig(adapterType, configs.OnePassword)
		client, chRunning, err = usp_1password.NewOnePasswordpAdapter(configs.OnePassword)
	} else if adapterType == "office365" {
		printConfig(adapterType, configs.Office365)
		client, chRunning, err = usp_o365.NewOffice365Adapter(configs.Office365)
	} else if adapterType == "wel" {
		printConfig(adapterType, configs.Wel)
		client, chRunning, err = usp_wel.NewWELAdapter(configs.Wel)
	} else if adapterType == "azure_event_hub" {
		printConfig(adapterType, configs.AzureEventHub)
		client, chRunning, err = usp_azure_event_hub.NewEventHubAdapter(configs.AzureEventHub)
	} else {
		logError("unknown adapter_type: %s", adapterType)
		os.Exit(1)
	}

	if err != nil {
		logError("error instantiating client: %v", err)
		os.Exit(1)
	}

	// If healthchecks were requested, start it.
	if runtimeConfigs.Healthcheck != 0 {
		if err := startHealthChecks(runtimeConfigs.Healthcheck); err != nil {
			logError("error starting healthchecks: %v", err)
			os.Exit(1)
		}
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

func applyLogging(o uspclient.ClientOptions) uspclient.ClientOptions {
	o.DebugLog = func(msg string) {
		log("DBG %s: %s", time.Now().Format(time.Stamp), msg)
	}
	o.OnWarning = func(msg string) {
		log("WRN %s: %s", time.Now().Format(time.Stamp), msg)
	}
	o.OnError = func(err error) {
		logError("ERR %s: %s", time.Now().Format(time.Stamp), err.Error())
	}
	o.BufferOptions.OnBackPressure = func() {
		log("FLO %s: experiencing back pressure", time.Now().Format(time.Stamp))
	}
	o.BufferOptions.OnAck = func() {
		log("FLO %s: received data ack from limacharlie", time.Now().Format(time.Stamp))
	}
	return o
}
