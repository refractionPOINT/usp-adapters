package usp_syslog

// End-to-end tests for the syslog adapter. The adapter is a listener, so the
// tests play the role of the syslog sender: they start the adapter on an
// ephemeral loopback port, connect to it over TCP/UDP/TLS, send realistic
// RFC3164/RFC5424 lines and assert the exact messages the adapter ships,
// using an in-memory uspSink in place of a real LimaCharlie client.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/require"
)

const (
	eventuallyTimeout = 5 * time.Second
	eventuallyTick    = 10 * time.Millisecond
)

// Realistic (but clearly fake) syslog samples.
const (
	sampleRFC3164 = "<34>Oct 11 22:14:15 host1.example.com su[1024]: 'su root' failed for alice on /dev/pts/8"
	sampleRFC5424 = `<165>1 2026-06-11T22:14:15.003Z host2.example.com evntslog 1234 ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry`
)

// --- test logging -------------------------------------------------------------

// testLogger funnels adapter callbacks to t.Logf, but goes quiet once the test
// is over: the adapter's Close does not wait for per-connection goroutines, so
// a handler can emit a DebugLog/OnWarning after the test body returns, and
// calling t.Logf then would panic the test binary.
type testLogger struct {
	mu   sync.Mutex
	t    *testing.T
	done bool
}

func (l *testLogger) logf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return
	}
	l.t.Logf(format, args...)
}

// testClientOptions returns ClientOptions wired for a sink (no real LimaCharlie
// connection) with the logging callbacks pointed at the test log.
func testClientOptions(t *testing.T) uspclient.ClientOptions {
	t.Helper()
	l := &testLogger{t: t}
	t.Cleanup(func() {
		l.mu.Lock()
		l.done = true
		l.mu.Unlock()
	})
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "test-installation-key",
		},
		Platform:     "text",
		TestSinkMode: true,
		DebugLog:     func(msg string) { l.logf("DBG: %s", msg) },
		OnWarning:    func(msg string) { l.logf("WRN: %s", msg) },
		OnError:      func(err error) { l.logf("ERR: %v", err) },
	}
}

// --- in-memory USP sink -------------------------------------------------------

// captureSink is an in-memory uspSink that records every shipped message.
type captureSink struct {
	mu       sync.Mutex
	messages []*protocol.DataMessage
}

func (s *captureSink) Ship(m *protocol.DataMessage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, m)
	return nil
}

func (s *captureSink) Drain(time.Duration) error               { return nil }
func (s *captureSink) Close() ([]*protocol.DataMessage, error) { return nil, nil }

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

func (s *captureSink) snapshot() []*protocol.DataMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*protocol.DataMessage, len(s.messages))
	copy(out, s.messages)
	return out
}

func (s *captureSink) textPayloads() []string {
	msgs := s.snapshot()
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.TextPayload)
	}
	return out
}

// --- adapter bring-up ----------------------------------------------------------

// startSyslogAdapter starts the adapter on an ephemeral loopback port and
// returns it with its stop channel and the address clients should dial. The
// real bound port is read off the adapter's listener (the package-internal
// view), so tests never race over a probed free port.
func startSyslogAdapter(t *testing.T, sink uspSink, mutate func(*SyslogConfig)) (*SyslogAdapter, chan struct{}, string) {
	t.Helper()
	conf := SyslogConfig{
		ClientOptions: testClientOptions(t),
		Port:          0, // ephemeral; the OS picks a free port
		Interface:     "127.0.0.1",
	}
	if mutate != nil {
		mutate(&conf)
	}
	a, chStopped, err := newSyslogAdapter(context.Background(), conf, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	var addr string
	if conf.IsUDP {
		addr = a.udpListener.LocalAddr().String()
	} else {
		addr = a.listener.Addr().String()
	}
	return a, chStopped, addr
}

// --- test PKI ------------------------------------------------------------------

// testPKI is a certificate + key generated for a test, with their PEM forms.
type testPKI struct {
	certPEM []byte
	keyPEM  []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

// newTestCert generates a certificate. With parent == nil it is self-signed
// (and may be a CA); otherwise it is signed by the parent.
func newTestCert(t *testing.T, cn string, isCA bool, eku []x509.ExtKeyUsage, parent *testPKI) testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"Example Test Org"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           eku,
		BasicConstraintsValid: true,
		DNSNames:              []string{cn, "localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	if isCA {
		tmpl.IsCA = true
		tmpl.KeyUsage |= x509.KeyUsageCertSign
	}

	parentCert, parentKey := tmpl, key
	if parent != nil {
		parentCert, parentKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parentCert, &key.PublicKey, parentKey)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return testPKI{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		cert:    parsed,
		key:     key,
	}
}

func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, data, 0o600))
	return p
}

// --- tests ----------------------------------------------------------------------

// TestTCPShipsSyslogLinesVerbatim sends an RFC3164 and an RFC5424 line (plus a
// blank line) over TCP and asserts each non-empty line ships exactly once, as
// a verbatim TextPayload, with a "now" timestamp and no JSON payload.
func TestTCPShipsSyslogLinesVerbatim(t *testing.T) {
	t.Parallel()
	sink := &captureSink{}
	_, _, addr := startSyslogAdapter(t, sink, nil)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	before := uint64(time.Now().UnixMilli())
	_, err = conn.Write([]byte(sampleRFC3164 + "\n" + sampleRFC5424 + "\n\n"))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.count() == 2 }, eventuallyTimeout, eventuallyTick)
	after := uint64(time.Now().UnixMilli())

	msgs := sink.snapshot()
	require.Len(t, msgs, 2)
	require.Equal(t, sampleRFC3164, msgs[0].TextPayload)
	require.Equal(t, sampleRFC5424, msgs[1].TextPayload)
	for _, m := range msgs {
		require.Nil(t, m.JsonPayload, "syslog ships raw text, never JSON")
		require.Empty(t, m.EventType)
		require.GreaterOrEqual(t, m.TimestampMs, before)
		require.LessOrEqual(t, m.TimestampMs, after)
	}
}

// TestTCPFramingPartialAndCoalescedWrites exercises the newline framing: a
// line split across two TCP writes is reassembled, several lines in a single
// segment are split apart, and a line far larger than the 16KiB read buffer
// (spanning several reads) ships intact.
func TestTCPFramingPartialAndCoalescedWrites(t *testing.T) {
	t.Parallel()
	sink := &captureSink{}
	_, _, addr := startSyslogAdapter(t, sink, nil)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	line1 := "<34>Oct 11 22:14:15 host1.example.com sshd[2001]: Accepted publickey for bob from 192.0.2.10 port 51514 ssh2"
	line2 := "<13>Jun 11 09:00:01 host2.example.com cron[7]: (root) CMD (/usr/local/bin/example-job)"
	line3 := "<13>Jun 11 09:00:02 host3.example.com cron[8]: (root) CMD (/usr/local/bin/other-job)"

	// First half of line1, no newline: nothing may ship yet.
	split := len(line1) / 2
	_, err = conn.Write([]byte(line1[:split]))
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond) // let the adapter read the partial segment
	require.Equal(t, 0, sink.count(), "a partial line must not ship")

	// Rest of line1 plus two full lines, all in one segment.
	_, err = conn.Write([]byte(line1[split:] + "\n" + line2 + "\n" + line3 + "\n"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 3 }, eventuallyTimeout, eventuallyTick)
	require.Equal(t, []string{line1, line2, line3}, sink.textPayloads())

	// A line much larger than the adapter's 16KiB read buffer.
	bigLine := "<134>1 2026-06-11T00:00:00Z host1.example.com bigapp 99 - - " + strings.Repeat("A", 48*1024)
	_, err = conn.Write([]byte(bigLine + "\n"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 4 }, eventuallyTimeout, eventuallyTick)
	require.Equal(t, bigLine, sink.snapshot()[3].TextPayload)
}

// TestTCPConcurrentConnections runs several simultaneous TCP senders and
// asserts every line from every connection ships exactly once.
func TestTCPConcurrentConnections(t *testing.T) {
	t.Parallel()
	sink := &captureSink{}
	_, _, addr := startSyslogAdapter(t, sink, nil)

	const nConns = 5
	const nLines = 20

	expected := make([]string, 0, nConns*nLines)
	for c := 0; c < nConns; c++ {
		for i := 0; i < nLines; i++ {
			expected = append(expected, fmt.Sprintf(
				"<13>Jun 11 09:%02d:%02d host%d.example.com app[%d]: concurrent test line %d-%d",
				c, i, c+1, 100+c, c, i))
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, nConns)
	for c := 0; c < nConns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			var b strings.Builder
			for i := 0; i < nLines; i++ {
				b.WriteString(expected[c*nLines+i])
				b.WriteByte('\n')
			}
			if _, err := conn.Write([]byte(b.String())); err != nil {
				errs <- err
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return sink.count() == nConns*nLines }, eventuallyTimeout, eventuallyTick)
	require.ElementsMatch(t, expected, sink.textPayloads())
}

// TestUDPShipsDatagramPerRecord covers UDP mode: each datagram is one record,
// shipped verbatim with no newline tokenization -- a datagram containing an
// embedded newline ships as a single message (pinned production behavior).
func TestUDPShipsDatagramPerRecord(t *testing.T) {
	t.Parallel()
	sink := &captureSink{}
	_, _, addr := startSyslogAdapter(t, sink, func(c *SyslogConfig) { c.IsUDP = true })

	conn, err := net.Dial("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte(sampleRFC3164))
	require.NoError(t, err)
	_, err = conn.Write([]byte(sampleRFC5424))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 2 }, eventuallyTimeout, eventuallyTick)
	require.ElementsMatch(t, []string{sampleRFC3164, sampleRFC5424}, sink.textPayloads())

	// Datagrams are never split on newlines: this ships as ONE message.
	multi := "<13>first line from host1.example.com\n<13>second line from host1.example.com"
	_, err = conn.Write([]byte(multi))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 3 }, eventuallyTimeout, eventuallyTick)
	require.Equal(t, multi, sink.snapshot()[2].TextPayload)
}

// TestTLSShipsLines runs the adapter with a self-signed server certificate and
// asserts a verifying TLS client can deliver lines.
func TestTLSShipsLines(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	server := newTestCert(t, "host1.example.com", false, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, nil)
	certPath := writeTempFile(t, dir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, dir, "server.key", server.keyPEM)

	sink := &captureSink{}
	_, _, addr := startSyslogAdapter(t, sink, func(c *SyslogConfig) {
		c.SslCertPath = certPath
		c.SslKeyPath = keyPath
	})

	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(server.certPEM))
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    roots,
		ServerName: "host1.example.com",
	})
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte(sampleRFC3164 + "\n" + sampleRFC5424 + "\n"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 2 }, eventuallyTimeout, eventuallyTick)
	require.Equal(t, []string{sampleRFC3164, sampleRFC5424}, sink.textPayloads())
}

// TestMutualTLS runs the adapter with client-certificate verification enabled:
// a client presenting a certificate signed by the configured CA delivers its
// line; a client with no certificate is rejected and its line never ships.
func TestMutualTLS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ca := newTestCert(t, "Example Test CA", true, nil, nil)
	server := newTestCert(t, "host1.example.com", false, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, nil)
	clientCert := newTestCert(t, "sender1.example.com", false, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, &ca)

	sink := &captureSink{}
	_, _, addr := startSyslogAdapter(t, sink, func(c *SyslogConfig) {
		c.SslCertPath = writeTempFile(t, dir, "server.crt", server.certPEM)
		c.SslKeyPath = writeTempFile(t, dir, "server.key", server.keyPEM)
		c.MutualTlsCertPath = writeTempFile(t, dir, "ca.crt", ca.certPEM)
	})

	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(server.certPEM))

	// A client with NO certificate must be rejected. Depending on the TLS
	// version the failure surfaces at Dial (TLS 1.2) or on the first Read
	// after the handshake (TLS 1.3) -- either way its line must never ship.
	badLine := "<13>Jun 11 09:00:00 intruder.example.com app[666]: should never ship"
	badConn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    roots,
		ServerName: "host1.example.com",
	})
	if err == nil {
		_, _ = badConn.Write([]byte(badLine + "\n"))
		require.NoError(t, badConn.SetReadDeadline(time.Now().Add(2*time.Second)))
		_, rerr := badConn.Read(make([]byte, 1))
		require.Error(t, rerr, "server must reject a client without a certificate")
		badConn.Close()
	}

	// A client presenting a CA-signed certificate succeeds.
	pair, err := tls.X509KeyPair(clientCert.certPEM, clientCert.keyPEM)
	require.NoError(t, err)
	goodConn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:      roots,
		ServerName:   "host1.example.com",
		Certificates: []tls.Certificate{pair},
	})
	require.NoError(t, err)
	defer goodConn.Close()

	goodLine := "<13>Jun 11 09:00:01 sender1.example.com app[42]: authenticated client line"
	_, err = goodConn.Write([]byte(goodLine + "\n"))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.count() == 1 }, eventuallyTimeout, eventuallyTick)
	require.Equal(t, []string{goodLine}, sink.textPayloads(),
		"only the authenticated client's line may ship")
}

// TestUDPWithTLSConfigRejected pins the config guard: TLS options cannot be
// combined with UDP mode.
func TestUDPWithTLSConfigRejected(t *testing.T) {
	t.Parallel()
	conf := SyslogConfig{
		ClientOptions: testClientOptions(t),
		Port:          0,
		Interface:     "127.0.0.1",
		IsUDP:         true,
		SslCertPath:   "/nonexistent/server.crt",
		SslKeyPath:    "/nonexistent/server.key",
	}
	a, ch, err := newSyslogAdapter(context.Background(), conf, &captureSink{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ssl cannot be enabled for udp")
	require.Nil(t, a)
	require.Nil(t, ch)
}

// TestConfigValidateRequiresPort pins Validate(): a zero port is rejected.
// Note that NewSyslogAdapter does not call Validate itself (nothing in this
// repo does), which is why the other tests can bind Port 0 directly.
func TestConfigValidateRequiresPort(t *testing.T) {
	t.Parallel()
	conf := SyslogConfig{
		ClientOptions: testClientOptions(t),
		Port:          0,
	}
	err := conf.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing port")
}

// TestTCPCloseWhileClientConnected covers shutdown with a live TCP client:
// Close() returns promptly, the stop channel closes, the server tears the
// client connection down, and -- aside from at most one line already in flight
// in the handler's blocking Read (pinned production behavior: Close() does not
// close accepted connections, so a read in progress can still complete and
// ship) -- nothing sent after Close is shipped.
func TestTCPCloseWhileClientConnected(t *testing.T) {
	t.Parallel()
	sink := &captureSink{}
	a, chStopped, addr := startSyslogAdapter(t, sink, nil)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Prove the connection handler is up before closing.
	preLine := "<13>Jun 11 08:59:59 host1.example.com app[1]: before close"
	_, err = conn.Write([]byte(preLine + "\n"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 1 }, eventuallyTimeout, eventuallyTick)

	// Close must return promptly with a client still connected...
	require.NoError(t, a.Close())
	// ...and the adapter's stop channel must close.
	require.Eventually(t, func() bool {
		select {
		case <-chStopped:
			return true
		default:
			return false
		}
	}, eventuallyTimeout, eventuallyTick, "chStopped must close after Close()")

	// One line written after Close may still be consumed by the handler's
	// in-flight Read; it must be the last thing that can ever ship.
	lateLine := "<13>Jun 11 09:00:05 host1.example.com app[1]: after close"
	_, _ = conn.Write([]byte(lateLine + "\n"))

	// The handler must tear the connection down (the client observes EOF/reset,
	// not just read timeouts).
	require.Eventually(t, func() bool {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
		_, rerr := conn.Read(make([]byte, 1))
		if rerr == nil {
			return false
		}
		var ne net.Error
		if errors.As(rerr, &ne) && ne.Timeout() {
			return false
		}
		return true
	}, eventuallyTimeout, eventuallyTick, "server must close the client connection after Close()")

	// The handler has exited (it closed the connection on its way out), so the
	// sink is final: the pre-close line, plus at most the one in-flight line.
	got := sink.textPayloads()
	require.GreaterOrEqual(t, len(got), 1)
	require.LessOrEqual(t, len(got), 2)
	require.Equal(t, preLine, got[0])
	if len(got) == 2 {
		require.Equal(t, lateLine, got[1])
	}
}

// TestStreamTokenizerSingleCharLineQuirk pins a known quirk of the tokenizer
// the TCP path feeds every read into (utils/stream_tokenizer.go): the
// `i-1 > dataStart` guard in Add skips segments of exactly one byte, so a
// one-character line whose byte and newline arrive in the SAME Add call comes
// back as an empty chunk -- handleLine then drops it, so the line never ships.
// The same line split across two Adds (byte in one read, newline in the next)
// survives via the end-of-buffer branch. The tokenizer here is configured
// exactly as handleConnection configures it (Token 0x0a).
func TestStreamTokenizerSingleCharLineQuirk(t *testing.T) {
	t.Parallel()

	// Byte and newline in one Add: the 1-byte line is lost (empty chunk),
	// surrounding lines are unaffected.
	st := utils.StreamTokenizer{ExpectedSize: 1024 * 32, Token: 0x0a}
	chunks, err := st.Add([]byte("<13>first line\nX\n<13>second line\n"))
	require.NoError(t, err)
	got := make([]string, 0, len(chunks))
	for _, c := range chunks {
		got = append(got, string(c))
	}
	require.Equal(t, []string{"<13>first line", "", "<13>second line"}, got,
		"a 1-character line in a single read degrades to an empty chunk (dropped by handleLine)")

	// The same 1-byte line split across two Adds is preserved.
	st = utils.StreamTokenizer{ExpectedSize: 1024 * 32, Token: 0x0a}
	chunks, err = st.Add([]byte("X"))
	require.NoError(t, err)
	require.Empty(t, chunks)
	chunks, err = st.Add([]byte("\n"))
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.Equal(t, "X", string(chunks[0]))
}

// TestUDPCloseStopsShipping covers UDP shutdown: Close() closes the socket, so
// datagrams sent after Close are deterministically never shipped.
func TestUDPCloseStopsShipping(t *testing.T) {
	t.Parallel()
	sink := &captureSink{}
	a, chStopped, addr := startSyslogAdapter(t, sink, func(c *SyslogConfig) { c.IsUDP = true })

	conn, err := net.Dial("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	preLine := "<13>Jun 11 08:59:59 host1.example.com app[1]: udp before close"
	_, err = conn.Write([]byte(preLine))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.count() == 1 }, eventuallyTimeout, eventuallyTick)

	require.NoError(t, a.Close())
	require.Eventually(t, func() bool {
		select {
		case <-chStopped:
			return true
		default:
			return false
		}
	}, eventuallyTimeout, eventuallyTick, "chStopped must close after Close()")

	// The socket is closed: these can never be delivered or shipped. Writes may
	// error (ICMP port unreachable) -- that is fine, only shipping matters.
	for i := 0; i < 3; i++ {
		_, _ = conn.Write([]byte(fmt.Sprintf("<13>Jun 11 09:00:0%d host1.example.com app[1]: udp after close", i)))
	}
	require.Never(t, func() bool { return sink.count() > 1 }, 250*time.Millisecond, 20*time.Millisecond,
		"nothing sent after Close may ship")
	require.Equal(t, []string{preLine}, sink.textPayloads())
}
