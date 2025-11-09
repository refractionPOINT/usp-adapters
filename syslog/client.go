package usp_syslog

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/adaptertypes"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
	udpBufferSize       = 64 * 1024
)

type SyslogAdapter struct {
	conf         adaptertypes.SyslogConfig
	listener     net.Listener
	udpListener  *net.UDPConn
	connMutex    sync.Mutex
	wg           sync.WaitGroup
	isRunning    uint32
	uspClient    *uspclient.Client
	writeTimeout time.Duration
}

func NewSyslogAdapter(conf adaptertypes.SyslogConfig) (*SyslogAdapter, chan struct{}, error) {
	a := &SyslogAdapter{
		conf:      conf,
		isRunning: 1,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	if conf.IsUDP && (conf.SslCertPath != "" || conf.SslKeyPath != "") {
		return nil, nil, errors.New("ssl cannot be enabled for udp")
	}

	addr := fmt.Sprintf("%s:%d", conf.Interface, conf.Port)
	var l net.Listener
	var ul *net.UDPConn
	var err error
	if conf.SslCertPath != "" && conf.SslKeyPath != "" {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(conf.SslCertPath, conf.SslKeyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("error loading certificate with cert path '%s' and key path '%s': %s", conf.SslCertPath, conf.SslKeyPath, err)
		}
		tlsConfig := tls.Config{
			Certificates: []tls.Certificate{cert},
		}

		// If mutual TLS is enabled, load the client certificate.
		if conf.MutualTlsCertPath != "" {
			caCert, err := os.ReadFile(conf.MutualTlsCertPath)
			if err != nil {
				return nil, nil, fmt.Errorf("error loading mutual TLS certificate with path '%s': %s", conf.MutualTlsCertPath, err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.ClientCAs = caCertPool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}

		l, err = tls.Listen("tcp", addr, &tlsConfig)
	} else if conf.IsUDP {
		var udpAddr *net.UDPAddr
		if udpAddr, err = net.ResolveUDPAddr("udp", addr); err != nil {
			return nil, nil, err
		}
		ul, err = net.ListenUDP("udp", udpAddr)
		if err == nil {
			ul.SetReadBuffer(udpBufferSize)
		}
	} else {
		l, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, nil, err
	}

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		if l != nil {
			l.Close()
		} else {
			ul.Close()
		}
		return nil, nil, err
	}

	a.listener = l
	a.udpListener = ul

	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)
		if conf.IsUDP {
			a.handleConnection(a.udpListener, true)
		} else {
			a.handleTCPConnections()
		}
	}()

	return a, chStopped, nil
}

func (a *SyslogAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isRunning, 0)
	var err1 error
	if a.listener != nil {
		err1 = a.listener.Close()
	} else {
		err1 = a.udpListener.Close()
	}
	err2 := a.uspClient.Drain(1 * time.Minute)
	_, err3 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	return err3
}

func (a *SyslogAdapter) handleTCPConnections() {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("listening for connections on %s:%d", a.conf.Interface, a.conf.Port))

	var err error

	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("stopped listening for connections on %s:%d (%v)", a.conf.Interface, a.conf.Port, err))

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
			a.handleConnection(conn, false)
		}()
	}
}

func (a *SyslogAdapter) handleConnection(conn net.Conn, isDatagram bool) {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("handling new connection from %+v", conn.RemoteAddr()))
	defer func() {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("connection from %+v leaving", conn.RemoteAddr()))
		conn.Close()
	}()

	readBufferSize := 1024 * 16
	st := utils.StreamTokenizer{
		ExpectedSize: readBufferSize * 2,
		Token:        0x0a,
	}

	readBuffer := make([]byte, readBufferSize)
	for atomic.LoadUint32(&a.isRunning) == 1 {
		sizeRead, err := conn.Read(readBuffer)
		if err != nil {
			if err != io.EOF {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("conn.Read(): %v", err))
			}
			return
		}

		data := readBuffer[:sizeRead]

		if isDatagram {
			// Datagram syslog contains one record per datagram.
			a.handleLine(data)
			continue
		}

		chunks, err := st.Add(data)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("tokenizer: %v", err))
		}
		for _, chunk := range chunks {
			a.handleLine(chunk)
		}
	}
}

func (a *SyslogAdapter) handleLine(line []byte) {
	if len(line) == 0 {
		return
	}
	msg := &protocol.DataMessage{
		TextPayload: string(line),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	err := a.uspClient.Ship(msg, a.writeTimeout)
	if err == uspclient.ErrorBufferFull {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		err = a.uspClient.Ship(msg, 1*time.Hour)
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
	}
}
