package usp_unifi_syslog

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
	udpBufferSize       = 64 * 1024
)

type UnifiSyslogAdapter struct {
	conf         UnifiSyslogConfig
	listener     net.Listener
	udpListener  *net.UDPConn
	connMutex    sync.Mutex
	wg           sync.WaitGroup
	isRunning    uint32
	uspClient    *uspclient.Client
	writeTimeout time.Duration
}

type UnifiSyslogConfig struct {
	ClientOptions     uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	Port              uint16                  `json:"port" yaml:"port"`
	Interface         string                  `json:"iface" yaml:"iface"`
	IsUDP             bool                    `json:"is_udp,omitempty" yaml:"is_udp,omitempty"`
	SslCertPath       string                  `json:"ssl_cert" yaml:"ssl_cert"`
	SslKeyPath        string                  `json:"ssl_key" yaml:"ssl_key"`
	MutualTlsCertPath string                  `json:"mutual_tls_cert,omitempty" yaml:"mutual_tls_cert,omitempty"`
	WriteTimeoutSec   uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
}

func (c *UnifiSyslogConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Port == 0 {
		return errors.New("missing port")
	}
	return nil
}

func NewUnifiSyslogAdapter(ctx context.Context, conf UnifiSyslogConfig) (*UnifiSyslogAdapter, chan struct{}, error) {
	a := &UnifiSyslogAdapter{
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

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
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

func (a *UnifiSyslogAdapter) Close() error {
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

func (a *UnifiSyslogAdapter) handleTCPConnections() {
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

func (a *UnifiSyslogAdapter) handleConnection(conn net.Conn, isDatagram bool) {
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

func (a *UnifiSyslogAdapter) handleLine(line []byte) {
	if len(line) == 0 {
		return
	}

	s := string(line)

	// Strip syslog priority header if present (e.g., "<134>").
	if len(s) > 0 && s[0] == '<' {
		if idx := strings.Index(s, ">"); idx != -1 && idx < 10 {
			s = s[idx+1:]
		}
	}

	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return
	}

	var msg *protocol.DataMessage

	// Find CEF payload within the syslog line. Real syslog often prepends
	// a header like "Feb  5 22:19:15 hostname CEF:0|..."
	cefLine := s
	if idx := strings.Index(s, "CEF:"); idx > 0 {
		cefLine = s[idx:]
	}

	parsed, ok := parseCEF(cefLine)
	if ok {
		eventType := "json"
		if name, _ := parsed["name"].(string); name != "" {
			eventType = name
		}
		msg = &protocol.DataMessage{
			JsonPayload: parsed,
			EventType:   eventType,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
	} else {
		// Non-CEF line: parse BSD syslog fields into structured JSON.
		msg = &protocol.DataMessage{
			JsonPayload: parseSyslog(s),
			EventType:   "syslog",
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
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

// parseCEF parses a CEF-formatted string into a structured map.
// CEF format: CEF:Version|Vendor|Product|Version|EventClassID|Name|Severity|Extension
// Returns the parsed map and true if the line is valid CEF, or nil and false otherwise.
func parseCEF(line string) (utils.Dict, bool) {
	// CEF lines must start with "CEF:" prefix.
	if !strings.HasPrefix(line, "CEF:") {
		return nil, false
	}

	// Split into at most 8 parts on pipe delimiter.
	// The 8th part is the extension string (which may contain pipes in values).
	parts := strings.SplitN(line, "|", 8)
	if len(parts) < 8 {
		return nil, false
	}

	result := utils.Dict{
		"cef_version":           strings.TrimPrefix(parts[0], "CEF:"),
		"device_vendor":         parts[1],
		"device_product":        parts[2],
		"device_version":        parts[3],
		"device_event_class_id": parts[4],
		"name":                  parts[5],
		"severity":              parts[6],
	}

	ext := parseCEFExtension(strings.TrimSpace(parts[7]))
	if len(ext) > 0 {
		result["extension"] = ext
	}

	return result, true
}

// cefStandardKeys contains standard CEF extension keys recognized as key=value boundaries.
var cefStandardKeys = map[string]bool{
	"act": true, "app": true, "c6a1": true, "c6a1Label": true,
	"c6a2": true, "c6a2Label": true, "c6a3": true, "c6a3Label": true,
	"c6a4": true, "c6a4Label": true, "cat": true, "cfp1": true,
	"cfp1Label": true, "cfp2": true, "cfp2Label": true, "cfp3": true,
	"cfp3Label": true, "cfp4": true, "cfp4Label": true, "cn1": true,
	"cn1Label": true, "cn2": true, "cn2Label": true, "cn3": true,
	"cn3Label": true, "cnt": true, "cs1": true, "cs1Label": true,
	"cs2": true, "cs2Label": true, "cs3": true, "cs3Label": true,
	"cs4": true, "cs4Label": true, "cs5": true, "cs5Label": true,
	"cs6": true, "cs6Label": true, "deviceAction": true,
	"deviceCustomDate1": true, "deviceCustomDate1Label": true,
	"deviceCustomDate2": true, "deviceCustomDate2Label": true,
	"deviceDirection": true, "deviceDnsDomain": true,
	"deviceExternalId": true, "deviceFacility": true,
	"deviceInboundInterface": true, "deviceOutboundInterface": true,
	"deviceMacAddress": true, "deviceNtDomain": true,
	"devicePayloadId": true, "deviceProcessName": true,
	"deviceTranslatedAddress": true, "deviceTranslatedZoneExternalID": true,
	"deviceTranslatedZoneURI": true, "deviceZoneExternalID": true,
	"deviceZoneURI": true, "dhost": true, "dmac": true, "dntdom": true,
	"dpid": true, "dpriv": true, "dproc": true, "dpt": true, "dst": true,
	"dtz": true, "duid": true, "duser": true, "dvc": true, "dvchost": true,
	"dvcmac": true, "dvcpid": true, "end": true, "externalId": true,
	"fileCreateTime": true, "fileHash": true, "fileId": true,
	"fileModificationTime": true, "fileName": true, "filePath": true,
	"filePermission": true, "fileSize": true, "fileType": true,
	"flexDate1": true, "flexDate1Label": true, "flexNumber1": true,
	"flexNumber1Label": true, "flexNumber2": true, "flexNumber2Label": true,
	"flexString1": true, "flexString1Label": true, "flexString2": true,
	"flexString2Label": true, "fname": true, "fsize": true, "in": true,
	"msg": true, "oldFileCreateTime": true, "oldFileHash": true,
	"oldFileId": true, "oldFileModificationTime": true, "oldFileName": true,
	"oldFilePath": true, "oldFilePermission": true, "oldFileSize": true,
	"oldFileType": true, "out": true, "outcome": true, "proto": true,
	"reason": true, "request": true, "requestClientApplication": true,
	"requestContext": true, "requestCookies": true, "requestMethod": true,
	"rt": true, "shost": true, "smac": true, "sntdom": true, "sourceDnsDomain": true,
	"sourceServiceName": true, "sourceTranslatedAddress": true,
	"sourceTranslatedPort": true, "sourceTranslatedZoneExternalID": true,
	"sourceTranslatedZoneURI": true, "sourceZoneExternalID": true,
	"sourceZoneURI": true, "spid": true, "spriv": true, "sproc": true,
	"spt": true, "src": true, "start": true, "suid": true, "suser": true,
	"type": true,
}

// isCEFKey returns true if the candidate is a recognized CEF key.
// Recognizes standard CEF keys and UniFi-prefixed keys (UNIFI*).
func isCEFKey(candidate string) bool {
	if len(candidate) == 0 {
		return false
	}
	if cefStandardKeys[candidate] {
		return true
	}
	if strings.HasPrefix(candidate, "UNIFI") {
		return true
	}
	return false
}

// parseCEFExtension parses CEF extension key=value pairs.
// Only recognized CEF keys (standard + UNIFI-prefixed) are treated as boundaries.
// Values run from after '=' until the space before the next recognized key.
func parseCEFExtension(ext string) utils.Dict {
	if ext == "" {
		return nil
	}

	result := utils.Dict{}

	type kvPos struct {
		keyStart int
		eqPos    int
	}

	var positions []kvPos
	i := 0
	for i < len(ext) {
		eqIdx := strings.Index(ext[i:], "=")
		if eqIdx == -1 {
			break
		}
		eqIdx += i

		keyStart := eqIdx
		for keyStart > 0 && ext[keyStart-1] != ' ' {
			keyStart--
		}

		key := ext[keyStart:eqIdx]
		if isCEFKey(key) {
			positions = append(positions, kvPos{keyStart: keyStart, eqPos: eqIdx})
		}

		i = eqIdx + 1
	}

	for idx, pos := range positions {
		key := ext[pos.keyStart:pos.eqPos]
		var value string
		if idx+1 < len(positions) {
			valueEnd := positions[idx+1].keyStart
			if valueEnd > 0 && ext[valueEnd-1] == ' ' {
				valueEnd--
			}
			value = ext[pos.eqPos+1 : valueEnd]
		} else {
			value = ext[pos.eqPos+1:]
		}
		result[key] = value
	}

	return result
}

// BSD syslog: "MMM DD HH:MM:SS hostname tag[pid]: message"
// UniFi variant: "MMM DD HH:MM:SS hostname hostname tag[pid]: message"
var syslogRe = regexp.MustCompile(
	`^([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+(.+)$`)

// Process tag with optional PID: "mcad[4857]: message" or "kernel: message"
var tagRe = regexp.MustCompile(`^([^\[:\s]+)(?:\[(\d+)\])?:\s*(.*)$`)

// parseSyslog parses a BSD syslog line into structured JSON.
// Falls back to {"raw": line} if the format doesn't match.
func parseSyslog(line string) utils.Dict {
	m := syslogRe.FindStringSubmatch(line)
	if m == nil {
		return utils.Dict{"raw": line}
	}

	result := utils.Dict{
		"timestamp": m[1],
		"hostname":  m[2],
	}

	remainder := m[3]

	// Strip doubled hostname (UniFi sends "host host process[pid]: msg").
	if strings.HasPrefix(remainder, m[2]+" ") {
		remainder = remainder[len(m[2])+1:]
	}

	// Parse "process[pid]: message".
	tm := tagRe.FindStringSubmatch(remainder)
	if tm != nil {
		result["process"] = tm[1]
		if tm[2] != "" {
			result["pid"] = tm[2]
		}
		result["message"] = tryParseJSON(tm[3])
	} else {
		result["message"] = tryParseJSON(remainder)
	}

	return result
}

// tryParseJSON attempts to parse a string as JSON. If successful, returns the
// parsed object (map or slice). Otherwise returns the original string.
func tryParseJSON(s string) interface{} {
	s = strings.TrimSpace(s)
	if len(s) == 0 || (s[0] != '{' && s[0] != '[') {
		return s
	}
	var parsed interface{}
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return s
	}
	return parsed
}
