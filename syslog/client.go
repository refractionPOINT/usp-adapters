package usp_syslog

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
)

const defaultWriteTimeout = 60 * 10

type SyslogAdapter struct {
	conf         SyslogConfig
	listener     net.Listener
	connMutex    sync.Mutex
	wg           sync.WaitGroup
	isRunning    uint32
	dbgLog       func(string)
	uspClient    *uspclient.Client
	writeTimeout time.Duration
}

type SyslogConfig struct {
	ClientOptons    uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Port            uint16                  `json:"port" yaml:"port"`
	Interface       string                  `json:"iface" yaml:"iface"`
	SslCertPath     string                  `json:"ssl_cert" yaml:"ssl_cert"`
	SslKeyPath      string                  `json:"ssl_key" yaml:"ssl_key"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
}

func NewSyslogAdapter(conf SyslogConfig) (*SyslogAdapter, error) {
	a := &SyslogAdapter{
		conf: conf,
		dbgLog: func(s string) {
			if conf.ClientOptons.DebugLog == nil {
				return
			}
			conf.ClientOptons.DebugLog(s)
		},
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	addr := fmt.Sprintf("%s:%d", conf.Interface, conf.Port)
	var l net.Listener
	var err error
	if conf.SslCertPath != "" && conf.SslKeyPath != "" {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(conf.SslCertPath, conf.SslKeyPath)
		if err != nil {
			return nil, fmt.Errorf("error loading certificate with cert path '%s' and key path '%s': %s", conf.SslCertPath, conf.SslKeyPath, err)
		}
		tlsConfig := tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		l, err = tls.Listen("tcp", addr, &tlsConfig)
	} else {
		l, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, err
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptons)
	if err != nil {
		l.Close()
		return nil, err
	}

	a.listener = l

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.handleConnections()
	}()

	return a, nil
}

func (a *SyslogAdapter) Close() error {
	atomic.StoreUint32(&a.isRunning, 0)
	err1 := a.listener.Close()
	_, err2 := a.uspClient.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (a *SyslogAdapter) handleConnections() {
	a.dbgLog(fmt.Sprintf("listening for connections on %s:%d", a.conf.Interface, a.conf.Port))

	var err error

	defer a.dbgLog(fmt.Sprintf("stopped listening for connections on %s:%d (%v)", a.conf.Interface, a.conf.Port, err))

	for atomic.LoadUint32(&a.isRunning) == 1 {
		var conn net.Conn
		conn, err = a.listener.Accept()
		if err != nil {
			break
		}
		a.connMutex.Lock()
		if atomic.LoadUint32(&a.isRunning) == 0 {
			a.connMutex.Unlock()
			conn.Close()
			break
		}
		a.wg.Add(1)
		a.connMutex.Unlock()
		go func() {
			defer a.wg.Done()
			a.handleConnection(conn)
		}()
	}
}

func (a *SyslogAdapter) handleConnection(conn net.Conn) {
	defer conn.Close()

	readBufferSize := 1024 * 16

	readBuffer := make([]byte, readBufferSize)
	currentData := make([]byte, 0, readBufferSize*2)
	for atomic.LoadUint32(&a.isRunning) == 1 {
		sizeRead, err := conn.Read(readBuffer[:])
		if err != nil {
			if err != io.EOF {
				a.dbgLog(fmt.Sprintf("conn.Read(): %v", err))
			}
			return
		}

		data := readBuffer[:sizeRead]
		dataStart := 0

		for i, b := range data {
			if b == 0x0a {
				// Found a newline, so we can use what we
				// have accumulated before plus this as
				// a message.
				if i-1 > dataStart {
					currentData = append(currentData, data[dataStart:i]...)
				}
				dataStart = i + 1
				a.handleLine(currentData)
				currentData = make([]byte, 0, readBufferSize*2)
				continue
			}
			if len(data)-1 == i {
				// This is the end of the buffer and
				// we got no newline, keep it for later.
				currentData = append(currentData, data[dataStart:i+1]...)
			}
		}
	}
}

func (a *SyslogAdapter) handleLine(line []byte) {
	if err := a.uspClient.Ship(&uspclient.UspDataMessage{}, a.writeTimeout); err != nil {

	}

}
