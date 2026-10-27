package main

import (
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type loggedLine struct {
	level string
	ts    time.Time
	msg   string
}

// fakeLogOutput captures what the limiter prints, with a controllable clock.
type fakeLogOutput struct {
	mu    sync.Mutex
	now   time.Time
	lines []loggedLine
}

func (f *fakeLogOutput) out(level string, ts time.Time, msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, loggedLine{level, ts, msg})
}

func (f *fakeLogOutput) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeLogOutput) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *fakeLogOutput) msgs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, l := range f.lines {
		out = append(out, l.level+" "+l.msg)
	}
	return out
}

func newTestLimiter() (*logLimiter, *fakeLogOutput) {
	f := &fakeLogOutput{now: time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)}
	l := newLogLimiter(f.out)
	l.now = f.clock
	return l, f
}

var (
	reRepeated = regexp.MustCompile(`^last message repeated (\d+) more time\(s\) between .*? and .*?: (.*)$`)
	reDropped  = regexp.MustCompile(`^log rate limit \(\d+/min\) reached: dropped (\d+) (\w+) line\(s\) since .*, last dropped: (.*)$`)
)

// accountedFor returns how many original log calls the printed output stands
// for: one per verbatim line, plus the counts carried by summary lines.
func accountedFor(t *testing.T, lines []loggedLine) int {
	t.Helper()
	total := 0
	for _, l := range lines {
		if m := reRepeated.FindStringSubmatch(l.msg); m != nil {
			n, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			total += n
			continue
		}
		if m := reDropped.FindStringSubmatch(l.msg); m != nil {
			n, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			total += n
			continue
		}
		total++
	}
	return total
}

func TestLogLimiterFirstOccurrencePrintedVerbatim(t *testing.T) {
	l, f := newTestLimiter()

	l.Log("ERR", "Dial(): tls: failed to verify certificate: x509: certificate signed by unknown authority")

	require.Len(t, f.lines, 1)
	assert.Equal(t, "ERR", f.lines[0].level)
	assert.Equal(t, f.now, f.lines[0].ts, "the line must carry the time it was logged")
	assert.Equal(t, "Dial(): tls: failed to verify certificate: x509: certificate signed by unknown authority", f.lines[0].msg)
}

func TestLogLimiterDistinctMessagesAllPrinted(t *testing.T) {
	l, f := newTestLimiter()

	// Messages that differ in a detail which matters for troubleshooting
	// must never be folded together.
	msgs := []string{
		"got 0 entries",
		"got 5 entries",
		"failed to fetch subscription 11111111-1111-1111-1111-111111111111: 403",
		"failed to fetch subscription 22222222-2222-2222-2222-222222222222: 403",
		"Dial attempt 1 failed: x509: certificate has expired, retrying...",
		"Dial attempt 2 failed: x509: certificate has expired, retrying...",
		"conn.Read(): remote error: tls: bad certificate",
		"conn.Read(): remote error: tls: unknown certificate authority",
		"",
		"multi\nline\nmessage",
	}
	for _, m := range msgs {
		l.Log("DBG", m)
	}

	var got []string
	for _, line := range f.lines {
		got = append(got, line.msg)
	}
	assert.Equal(t, msgs, got)
}

func TestLogLimiterSameTextDifferentLevelsNotFolded(t *testing.T) {
	l, f := newTestLimiter()

	l.Log("DBG", "connection reset")
	l.Log("WRN", "connection reset")
	l.Log("ERR", "connection reset")

	assert.Equal(t, []string{"DBG connection reset", "WRN connection reset", "ERR connection reset"}, f.msgs())
}

func TestLogLimiterRepeatsFoldedIntoSummary(t *testing.T) {
	l, f := newTestLimiter()

	l.Log("ERR", "Dial(): x509: certificate signed by unknown authority")
	start := f.now
	for i := 0; i < 11; i++ {
		f.advance(5 * time.Second)
		l.Log("ERR", "Dial(): x509: certificate signed by unknown authority")
	}
	last := f.now
	require.Len(t, f.lines, 1, "repeats within the window must not be printed")

	f.advance(5 * time.Second) // window over
	l.flush(false)

	require.Len(t, f.lines, 2)
	summary := f.lines[1]
	assert.Equal(t, "ERR", summary.level, "the summary keeps the level of the folded message")
	m := reRepeated.FindStringSubmatch(summary.msg)
	require.NotNil(t, m, "unexpected summary: %q", summary.msg)
	assert.Equal(t, "11", m[1])
	assert.Equal(t, "Dial(): x509: certificate signed by unknown authority", m[2])
	assert.Contains(t, summary.msg, start.Add(5*time.Second).Format(time.Stamp), "first repeat time")
	assert.Contains(t, summary.msg, last.Format(time.Stamp), "last repeat time")
}

func TestLogLimiterSteadyFloodOneSummaryPerWindow(t *testing.T) {
	l, f := newTestLimiter()

	// One failure every second for 10 minutes, with the background flusher
	// running every logFlushInterval.
	const total = 600
	for i := 0; i < total; i++ {
		l.Log("WRN", "error reconnecting: tls: handshake failure")
		f.advance(time.Second)
		if i%int(logFlushInterval/time.Second) == 0 {
			l.flush(false)
		}
	}
	l.flush(true)

	// 1 verbatim line + about one summary per minute, instead of 600 lines.
	assert.LessOrEqual(t, len(f.lines), 1+11)
	assert.Equal(t, total, accountedFor(t, f.lines), "every occurrence must be accounted for")
}

func TestLogLimiterQuietMessagePrintsInFullAgain(t *testing.T) {
	l, f := newTestLimiter()

	l.Log("ERR", "Dial(): i/o timeout")
	f.advance(logDedupWindow + time.Second)
	l.flush(false) // no repeats: nothing to report, the entry is forgotten
	require.Len(t, f.lines, 1)

	f.advance(10 * time.Minute)
	l.Log("ERR", "Dial(): i/o timeout")

	assert.Equal(t, []string{"ERR Dial(): i/o timeout", "ERR Dial(): i/o timeout"}, f.msgs(),
		"a message recurring after a quiet period must be printed again, not hidden")
}

func TestLogLimiterWindowExpiryOnNextOccurrence(t *testing.T) {
	l, f := newTestLimiter()

	// No flush in between: the next occurrence after the window reports
	// the folded repeats and starts the next window with itself.
	l.Log("WRN", "stream falling behind")
	f.advance(time.Second)
	l.Log("WRN", "stream falling behind")
	f.advance(logDedupWindow)
	l.Log("WRN", "stream falling behind")

	require.Len(t, f.lines, 2)
	assert.Regexp(t, reRepeated, f.lines[1].msg)

	l.flush(true)
	require.Len(t, f.lines, 3)
	assert.Regexp(t, reRepeated, f.lines[2].msg)
	assert.Equal(t, 3, accountedFor(t, f.lines))
}

func TestLogLimiterEphemeralPortsFolded(t *testing.T) {
	l, f := newTestLimiter()

	// The syslog adapter's per-connection lines: same sender, new port each
	// time it reconnects.
	for port := 50000; port < 50100; port++ {
		l.Log("DBG", fmt.Sprintf("handling new connection from 10.1.2.3:%d", port))
		l.Log("WRN", fmt.Sprintf("tls handshake with 10.1.2.3:%d failed: remote error: tls: bad certificate", port))
		l.Log("DBG", fmt.Sprintf("connection from 10.1.2.3:%d leaving", port))
		f.advance(100 * time.Millisecond)
	}
	l.flush(true)

	assert.Len(t, f.lines, 6, "3 verbatim lines + 3 summaries")
	assert.Equal(t, 300, accountedFor(t, f.lines))
	var summaries []string
	for _, line := range f.lines {
		if m := reRepeated.FindStringSubmatch(line.msg); m != nil {
			summaries = append(summaries, m[2])
		}
	}
	assert.Contains(t, summaries, "tls handshake with 10.1.2.3:50099 failed: remote error: tls: bad certificate",
		"the summary carries the most recent text, real port included")
}

func TestLogLimiterDifferentHostsNotFolded(t *testing.T) {
	l, f := newTestLimiter()

	l.Log("WRN", "tls handshake with 10.1.2.3:50000 failed: remote error: tls: bad certificate")
	l.Log("WRN", "tls handshake with 10.1.2.4:50001 failed: remote error: tls: bad certificate")
	l.Log("WRN", "tls handshake with [2001:db8::1]:50002 failed: remote error: tls: bad certificate")
	l.Log("WRN", "tls handshake with [2001:db8::2]:50003 failed: remote error: tls: bad certificate")

	assert.Len(t, f.lines, 4, "each failing host must be visible")
}

func TestNormalizeLogKey(t *testing.T) {
	for in, want := range map[string]string{
		"dial tcp 10.0.0.1:443: connect: connection refused":  "dial tcp 10.0.0.1:<port>: connect: connection refused",
		"read tcp 127.0.0.1:51234->10.0.0.1:443: i/o timeout": "read tcp 127.0.0.1:<port>->10.0.0.1:<port>: i/o timeout",
		"from [::1]:51234":                                       "from [::1]:<port>",
		"from [fe80::1%eth0]:51234":                              "from [fe80::1%eth0]:<port>",
		"got 42 entries":                                         "got 42 entries",
		"api.example.com:443 unreachable":                        "api.example.com:443 unreachable",
		"Oct 27 10:00:01":                                        "Oct 27 10:00:01",
		"id 11111111-1111-1111-1111-111111111111":                "id 11111111-1111-1111-1111-111111111111",
		"version 1.2.3.4:5 is not an address but looks like one": "version 1.2.3.4:<port> is not an address but looks like one",
	} {
		assert.Equal(t, want, normalizeLogKey(in), in)
	}
}

func TestLogLimiterRateLimitAccountsForDroppedLines(t *testing.T) {
	l, f := newTestLimiter()

	// 1000 distinct debug lines at once: the burst is printed, the rest is
	// counted and reported.
	for i := 0; i < 1000; i++ {
		l.Log("DBG", fmt.Sprintf("fetched page %d", i))
	}
	require.Len(t, f.lines, logRateBurst)

	f.advance(logDedupWindow)
	l.flush(false)

	last := f.lines[len(f.lines)-1]
	m := reDropped.FindStringSubmatch(last.msg)
	require.NotNil(t, m, "unexpected drop summary: %q", last.msg)
	assert.Equal(t, "400", m[1])
	assert.Equal(t, "DBG", m[2])
	assert.Equal(t, "fetched page 999", m[3], "the last dropped line is shown")
	assert.Equal(t, 1000, accountedFor(t, f.lines))
}

func TestLogLimiterRateLimitPerLevel(t *testing.T) {
	l, f := newTestLimiter()

	// A debug flood must not starve warnings and errors.
	for i := 0; i < 5000; i++ {
		l.Log("DBG", fmt.Sprintf("noise %d", i))
	}
	l.Log("WRN", "stream falling behind")
	l.Log("ERR", "Ship(): buffer full")

	msgs := f.msgs()
	assert.Contains(t, msgs, "WRN stream falling behind")
	assert.Contains(t, msgs, "ERR Ship(): buffer full")
}

func TestLogLimiterRateLimitRefills(t *testing.T) {
	l, f := newTestLimiter()

	for i := 0; i < logRateBurst; i++ {
		l.Log("DBG", fmt.Sprintf("line %d", i))
	}
	l.Log("DBG", "dropped")
	require.Len(t, f.lines, logRateBurst)

	f.advance(time.Second) // refills logRatePerMinute/60 tokens
	l.Log("DBG", "after refill")
	assert.Equal(t, "DBG after refill", f.msgs()[len(f.lines)-1])
}

func TestLogLimiterFinalFlushReportsEverything(t *testing.T) {
	l, f := newTestLimiter()

	l.Log("ERR", "Dial(): x509: certificate has expired")
	l.Log("ERR", "Dial(): x509: certificate has expired")
	for i := 0; i < logRateBurst+3; i++ {
		l.Log("WRN", fmt.Sprintf("distinct %d", i))
	}

	// On exit nothing pending may be lost, even mid-window.
	l.flush(true)

	assert.Equal(t, 2+logRateBurst+3, accountedFor(t, f.lines))
	assert.Empty(t, l.entries)
}

func TestLogLimiterTrackedKeysBounded(t *testing.T) {
	l, f := newTestLimiter()
	l.maxKeys = 10
	l.burst = 1e9

	for i := 0; i < 50; i++ {
		l.Log("DBG", fmt.Sprintf("unique %d", i))
	}
	assert.Len(t, l.entries, 10)
	assert.Len(t, f.lines, 50, "messages past the table size are still printed")
}

func TestLogLimiterDisabledPassesEverything(t *testing.T) {
	l, f := newTestLimiter()
	l.disabled = true

	for i := 0; i < 2000; i++ {
		l.Log("ERR", "same error")
	}
	assert.Len(t, f.lines, 2000)
}

func TestLogLimiterConcurrentAccounting(t *testing.T) {
	l, f := newTestLimiter()

	const goroutines, perG = 16, 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < perG; i++ {
				level := []string{"DBG", "WRN", "ERR"}[r.Intn(3)]
				l.Log(level, fmt.Sprintf("error %d from 10.0.0.%d:%d", r.Intn(20), r.Intn(3), 40000+r.Intn(1000)))
				if i%50 == 0 {
					f.advance(time.Second)
					l.flush(false)
				}
			}
		}(g)
	}
	wg.Wait()
	l.flush(true)

	assert.Equal(t, goroutines*perG, accountedFor(t, f.lines))
}

func TestApplyLoggingRoutesThroughLimiter(t *testing.T) {
	l, f := newTestLimiter()
	orig := processLogLimiter
	processLogLimiter = l
	t.Cleanup(func() { processLogLimiter = orig })
	startLogFlusherOnce.Do(func() {}) // no background flusher in the test

	o := applyLogging(uspclient.ClientOptions{})
	o.DebugLog("debug message")
	o.OnWarning("warning message")
	o.OnError(errors.New("error message"))
	o.OnError(errors.New("error message"))
	l.flush(true)

	msgs := f.msgs()
	require.Len(t, msgs, 4)
	assert.Equal(t, "DBG debug message", msgs[0])
	assert.Equal(t, "WRN warning message", msgs[1])
	assert.Equal(t, "ERR error message", msgs[2])
	assert.True(t, strings.HasPrefix(msgs[3], "ERR last message repeated 1 more time(s)"), msgs[3])
}
