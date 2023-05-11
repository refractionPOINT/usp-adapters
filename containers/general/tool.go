package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/1password"
	"github.com/refractionPOINT/usp-adapters/azure_event_hub"
	"github.com/refractionPOINT/usp-adapters/duo"
	"github.com/refractionPOINT/usp-adapters/evtx"
	"github.com/refractionPOINT/usp-adapters/file"
	"github.com/refractionPOINT/usp-adapters/gcs"
	"github.com/refractionPOINT/usp-adapters/o365"
	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/s3"
	"github.com/refractionPOINT/usp-adapters/simulator"
	"github.com/refractionPOINT/usp-adapters/slack"
	"github.com/refractionPOINT/usp-adapters/sqs"
	"github.com/refractionPOINT/usp-adapters/sqs-files"
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
	Duo           usp_duo.DuoConfig                  `json:"duo" yaml:"duo"`
	Gcs           usp_gcs.GCSConfig                  `json:"gcs" yaml:"gcs"`
	Slack         usp_slack.SlackConfig              `json:"slack" yaml:"slack"`
	Sqs           usp_sqs.SQSConfig                  `json:"sqs" yaml:"sqs"`
	SqsFiles      usp_sqs_files.SQSFilesConfig       `json:"sqs-files" yaml:"sqs-files"`
	Simulator     usp_simulator.SimulatorConfig      `json:"simulator" yaml:"simulator"`
	File          usp_file.FileConfig                `json:"file" yaml:"file"`
	Evtx          usp_evtx.EVTXConfig                `json:"evtx" yaml:"evtx"`
}

type AdapterStats struct {
	m                sync.Mutex
	lastAck          time.Time
	lastBackPressure time.Time
}

func logError(format string, elems ...interface{}) string {
	s := fmt.Sprintf(format+"\n", elems...)
	os.Stderr.Write([]byte(s))
	return s
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

func printConfig(method string, c interface{}) {
	b, _ := yaml.Marshal(c)
	log("Configs in use (%s):\n----------------------------------\n%s----------------------------------\n", method, string(b))
}

func main() {
	log("starting")

	if len(os.Args) > 1 && strings.HasPrefix(os.Args[1], "-") {
		if err := serviceMode(os.Args[0], os.Args[1], os.Args[2:]); err != nil {
			logError("service: %v", err)
			os.Exit(1)
		}
		return
	}

	method, runtimeConfigs, configs, err := parseConfigs(os.Args[1:])
	if err != nil {
		logError("error: %s", err)
		printUsage()
		os.Exit(1)
	}

	client, chRunning, err := runAdapter(method, *runtimeConfigs, *configs)
	if err != nil {
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

func runAdapter(method string, runtimeConfigs RuntimeConfig, configs GeneralConfigs) (USPClient, chan struct{}, error) {
	var client USPClient
	var chRunning chan struct{}
	var err error

	if method == "syslog" {
		configs.Syslog.ClientOptions = applyLogging(configs.Syslog.ClientOptions)
		configs.Syslog.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Syslog)
		client, chRunning, err = usp_syslog.NewSyslogAdapter(configs.Syslog)
	} else if method == "pubsub" {
		configs.PubSub.ClientOptions = applyLogging(configs.PubSub.ClientOptions)
		configs.PubSub.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.PubSub)
		client, chRunning, err = usp_pubsub.NewPubSubAdapter(configs.PubSub)
	} else if method == "gcs" {
		configs.Gcs.ClientOptions = applyLogging(configs.Gcs.ClientOptions)
		configs.Gcs.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Gcs)
		client, chRunning, err = usp_gcs.NewGCSAdapter(configs.Gcs)
	} else if method == "s3" {
		configs.S3.ClientOptions = applyLogging(configs.S3.ClientOptions)
		configs.S3.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.S3)
		client, chRunning, err = usp_s3.NewS3Adapter(configs.S3)
	} else if method == "stdin" {
		configs.Stdin.ClientOptions = applyLogging(configs.Stdin.ClientOptions)
		configs.Stdin.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Stdin)
		client, chRunning, err = usp_stdin.NewStdinAdapter(configs.Stdin)
	} else if method == "1password" {
		configs.OnePassword.ClientOptions = applyLogging(configs.OnePassword.ClientOptions)
		configs.OnePassword.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.OnePassword)
		client, chRunning, err = usp_1password.NewOnePasswordpAdapter(configs.OnePassword)
	} else if method == "office365" {
		configs.Office365.ClientOptions = applyLogging(configs.Office365.ClientOptions)
		configs.Office365.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Office365)
		client, chRunning, err = usp_o365.NewOffice365Adapter(configs.Office365)
	} else if method == "wel" {
		configs.Wel.ClientOptions = applyLogging(configs.Wel.ClientOptions)
		configs.Wel.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Wel)
		client, chRunning, err = usp_wel.NewWELAdapter(configs.Wel)
	} else if method == "azure_event_hub" {
		configs.AzureEventHub.ClientOptions = applyLogging(configs.AzureEventHub.ClientOptions)
		configs.AzureEventHub.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.AzureEventHub)
		client, chRunning, err = usp_azure_event_hub.NewEventHubAdapter(configs.AzureEventHub)
	} else if method == "duo" {
		configs.Duo.ClientOptions = applyLogging(configs.Duo.ClientOptions)
		configs.Duo.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Duo)
		client, chRunning, err = usp_duo.NewDuoAdapter(configs.Duo)
	} else if method == "slack" {
		configs.Slack.ClientOptions = applyLogging(configs.Slack.ClientOptions)
		configs.Slack.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Slack)
		client, chRunning, err = usp_slack.NewSlackAdapter(configs.Slack)
	} else if method == "sqs" {
		configs.Sqs.ClientOptions = applyLogging(configs.Sqs.ClientOptions)
		configs.Sqs.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Sqs)
		client, chRunning, err = usp_sqs.NewSQSAdapter(configs.Sqs)
	} else if method == "sqs-files" {
		configs.SqsFiles.ClientOptions = applyLogging(configs.SqsFiles.ClientOptions)
		configs.SqsFiles.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.SqsFiles)
		client, chRunning, err = usp_sqs_files.NewSQSFilesAdapter(configs.SqsFiles)
	} else if method == "simulator" {
		configs.Simulator.ClientOptions = applyLogging(configs.Simulator.ClientOptions)
		configs.Simulator.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Simulator)
		client, chRunning, err = usp_simulator.NewSimulatorAdapter(configs.Simulator)
	} else if method == "file" {
		configs.File.ClientOptions = applyLogging(configs.File.ClientOptions)
		configs.File.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.File)
		client, chRunning, err = usp_file.NewFileAdapter(configs.File)
	} else if method == "evtx" {
		configs.Evtx.ClientOptions = applyLogging(configs.Evtx.ClientOptions)
		configs.Evtx.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Evtx)
		client, chRunning, err = usp_evtx.NewEVTXAdapter(configs.Evtx)
	} else {
		return nil, nil, errors.New(logError("unknown adapter_type: %s", method))
	}

	if err != nil {
		return nil, nil, errors.New(logError("error instantiating client: %v", err))
	}

	// If healthchecks were requested, start it.
	if runtimeConfigs.Healthcheck != 0 {
		if err := startHealthChecks(runtimeConfigs.Healthcheck); err != nil {
			client.Close()
			return nil, nil, errors.New(logError("error starting healthchecks: %v", err))
		}
	}

	return client, chRunning, nil
}

func parseConfigs(args []string) (string, *RuntimeConfig, *GeneralConfigs, error) {
	runtimeConfigs := &RuntimeConfig{}
	configs := &GeneralConfigs{}
	var err error
	if len(args) < 2 {
		return "", nil, nil, errors.New("not enough arguments")
	}

	method := args[0]
	args = args[1:]
	if len(args) == 1 {
		log("loading config from file: %s", args[0])
		if runtimeConfigs, configs, err = parseConfigsFromFile(args[0]); err != nil {
			return "", nil, nil, err
		}
	} else {
		// Read the config from the CLI.
		if err = parseConfigsFromParams(method, args, runtimeConfigs, configs); err != nil {
			return "", nil, nil, err
		}
		// Read the config from the Env.
		if err = parseConfigsFromParams(method, os.Environ(), runtimeConfigs, configs); err != nil {
			return "", nil, nil, err
		}
	}
	return method, runtimeConfigs, configs, nil
}

func parseConfigsFromFile(filePath string) (*RuntimeConfig, *GeneralConfigs, error) {
	runtimeConfigs := &RuntimeConfig{}
	configs := &GeneralConfigs{}

	f, err := os.Open(filePath)
	if err != nil {
		printUsage()
		return nil, nil, errors.New(logError("os.Open(): %v", err))
	}
	b, err := io.ReadAll(f)
	if err != nil {
		printUsage()
		return nil, nil, errors.New(logError("io.ReadAll(): %v", err))
	}
	if err := json.Unmarshal(b, &configs); err != nil {
		err2 := yaml.Unmarshal(b, &configs)
		if err2 != nil {
			printUsage()
			err = errors.New(logError("json.Unmarshal(): %v", err))
			err2 = errors.New(logError("yaml.Unmarshal(): %v", err2))
			return nil, nil, fmt.Errorf("%v\n%v", err, err2)
		}
	}
	if err := json.Unmarshal(b, &runtimeConfigs); err != nil {
		err2 := yaml.Unmarshal(b, &runtimeConfigs)
		if err2 != nil {
			printUsage()
			err = errors.New(logError("json.Unmarshal(): %v", err))
			err2 = errors.New(logError("yaml.Unmarshal(): %v", err2))
			return nil, nil, fmt.Errorf("%v\n%v", err, err2)
		}
	}
	return runtimeConfigs, configs, nil
}

func parseConfigsFromParams(prefix string, params []string, runtimeConfigs *RuntimeConfig, configs *GeneralConfigs) error {
	// Read the config from the CLI.
	if err := utils.ParseCLI(prefix, params, configs); err != nil {
		printUsage()
		return errors.New(logError("ParseCLI(): %v", err))
	}
	// Get the runtime configs.
	if err := utils.ParseCLI("", params, runtimeConfigs); err != nil {
		printUsage()
		return errors.New(logError("ParseCLI(): %v", err))
	}

	return nil
}

func applyLogging(o uspclient.ClientOptions) uspclient.ClientOptions {
	stats := &AdapterStats{
		lastAck:          time.Now(),
		lastBackPressure: time.Now(),
	}

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
		stats.m.Lock()
		defer stats.m.Unlock()
		stats.lastBackPressure = time.Now()
	}
	o.BufferOptions.OnAck = func() {
		stats.m.Lock()
		defer stats.m.Unlock()
		stats.lastAck = time.Now()
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			stats.m.Lock()
			log("FLO %s: last_ack=%s last_pressure=%s", time.Now().Format(time.Stamp), stats.lastAck.Format(time.Stamp), stats.lastBackPressure.Format(time.Stamp))
			stats.m.Unlock()
		}
	}()

	return o
}
