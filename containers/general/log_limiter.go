package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// logDedupWindow is how long repeats of a message are folded into one
	// summary line after the message is first printed.
	logDedupWindow = 1 * time.Minute
	// logRateBurst lines of one level can be printed at once, refilled at
	// logRatePerMinute. They only matter when many *different* messages flood
	// in (repeats of the same message are already folded by dedup), and are
	// sized so that the routine per-object debug lines of a busy adapter fit.
	logRatePerMinute = 3000
	logRateBurst     = 3000
	// logMaxTrackedKeys bounds the dedup table. Messages past it are still
	// printed (subject to the rate limit), just not deduplicated.
	logMaxTrackedKeys = 1000
	// logFlushInterval is how often pending summaries are checked.
	logFlushInterval = 5 * time.Second
	// logEphemeralPortMin is the start of the ephemeral port range (Linux
	// uses 32768+, Windows and macOS 49152+). Only such ports are masked:
	// a service port like 443 or 8443 tells messages apart.
	logEphemeralPortMin = 32768

	// logDedupDisableEnv turns the limiter off: every line is printed as is.
	logDedupDisableEnv = "USP_LOG_NO_DEDUP"
)

// reIPPort matches an IPv4 or bracketed IPv6 address followed by a port.
// Ephemeral ports are masked in dedup keys so that the same error from the
// same host does not look unique only because each connection uses a new
// port. The address itself is kept: a failure from another host is another
// message.
var reIPPort = regexp.MustCompile(`(\b\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-fA-F.]*:[0-9a-fA-F:.]*(?:%[0-9A-Za-z_.-]+)?\]):(\d{1,5})\b`)

func normalizeLogKey(msg string) string {
	return reIPPort.ReplaceAllStringFunc(msg, func(m string) string {
		sub := reIPPort.FindStringSubmatch(m)
		if port, err := strconv.Atoi(sub[2]); err != nil || port < logEphemeralPortMin {
			return m
		}
		return sub[1] + ":<port>"
	})
}

type logEntry struct {
	level       string
	windowStart time.Time
	firstRepeat time.Time
	lastRepeat  time.Time
	repeats     int
	lastMsg     string
}

type logBucket struct {
	tokens       float64
	lastRefill   time.Time
	dropped      int
	firstDropped time.Time
	lastDropped  string
}

type logLine struct {
	level string
	ts    time.Time
	msg   string
}

// logLimiter sits between one adapter's DebugLog/OnWarning/OnError callbacks
// and the process output. It never discards a message without accounting for
// it: the first occurrence of a message is printed as is, repeats within
// logDedupWindow are counted and reported in a summary line carrying the most
// recent text, and lines dropped by the rate limit are reported the same way.
type logLimiter struct {
	mu       sync.Mutex
	disabled bool
	closed   bool // after the final flush: print everything directly
	// tracked limiters are listed in logLimiters while they hold anything
	// to report (registered), so the flusher and the exit flush reach them,
	// and drop out of it once idle, so limiters of adapters replaced by a
	// config update do not accumulate.
	tracked    bool
	registered bool
	window     time.Duration
	rate       float64 // lines per second, per level
	burst      float64
	maxKeys    int
	now        func() time.Time
	out        func(level string, ts time.Time, msg string)
	entries    map[string]*logEntry
	buckets    map[string]*logBucket
}

func newLogLimiter(out func(level string, ts time.Time, msg string)) *logLimiter {
	return &logLimiter{
		window:  logDedupWindow,
		rate:    float64(logRatePerMinute) / 60,
		burst:   logRateBurst,
		maxKeys: logMaxTrackedKeys,
		now:     time.Now,
		out:     out,
		entries: map[string]*logEntry{},
		buckets: map[string]*logBucket{},
	}
}

// printLogLine is the production output: errors to stderr, the rest to stdout,
// in the same "LVL <timestamp>: message" format the adapters always used.
func printLogLine(level string, ts time.Time, msg string) {
	if level == "ERR" {
		logError("%s %s: %s", level, ts.Format(time.Stamp), msg)
		return
	}
	log("%s %s: %s", level, ts.Format(time.Stamp), msg)
}

// logOutput is where limiters created from now on print. A variable so tests
// can capture it; each limiter keeps the value it was created with.
var logOutput = printLogLine

// logDedupDisabled reports whether logDedupDisableEnv asks for raw logs.
func logDedupDisabled() bool {
	v := os.Getenv(logDedupDisableEnv)
	if v == "" {
		return false
	}
	disabled, err := strconv.ParseBool(v)
	if err != nil {
		logError("ignoring %s=%q: not a boolean", logDedupDisableEnv, v)
		return false
	}
	return disabled
}

var (
	logLimitersMu sync.Mutex
	logLimiters   []*logLimiter
	// logFinalFlushed is set by the final flush. From then on tracked
	// limiters print every line directly, including those not listed at
	// that moment because they were idle.
	logFinalFlushed atomic.Bool
	logFlusherOne   sync.Once
)

// newAdapterLogLimiter returns the limiter for one adapter. Each adapter gets
// its own, so a chatty adapter cannot use up the budget of another and the
// same message from two adapters is not merged into one.
func newAdapterLogLimiter() *logLimiter {
	l := newLogLimiter(logOutput)
	l.disabled = logDedupDisabled()
	l.tracked = true

	logFlusherOne.Do(func() {
		if l.disabled {
			log("log deduplication disabled by %s", logDedupDisableEnv)
			return
		}
		log("log deduplication enabled: repeats within %v are summarized, set %s=1 to disable", l.window, logDedupDisableEnv)
		go func() {
			for {
				time.Sleep(logFlushInterval)
				flushLogLimiters(false)
			}
		}()
	})
	return l
}

// flushLogLimiters flushes every adapter's limiter. With final set, all
// pending counts are reported and later lines are printed directly.
func flushLogLimiters(final bool) {
	if final {
		logFinalFlushed.Store(true)
	}
	flushed := map[*logLimiter]bool{}
	// With final set, a second pass reaches limiters registered by a line
	// logged while the first one ran.
	for pass := 0; pass < 2; pass++ {
		logLimitersMu.Lock()
		ls := append([]*logLimiter(nil), logLimiters...)
		logLimitersMu.Unlock()
		for _, l := range ls {
			if flushed[l] {
				continue
			}
			flushed[l] = true
			l.flush(final)
			l.unregisterIfIdle()
		}
		if !final {
			break
		}
	}
}

// hasPendingLocked reports whether the limiter holds anything a flush would
// report, or will once a window is over. l.mu must be held.
func (l *logLimiter) hasPendingLocked() bool {
	if len(l.entries) != 0 {
		return true
	}
	for _, b := range l.buckets {
		if b.dropped != 0 {
			return true
		}
	}
	return false
}

// registerIfPendingLocked lists a tracked limiter that has something to
// report. l.mu must be held; it is always taken before logLimitersMu.
func (l *logLimiter) registerIfPendingLocked() {
	if !l.tracked || l.registered || !l.hasPendingLocked() {
		return
	}
	logLimitersMu.Lock()
	logLimiters = append(logLimiters, l)
	logLimitersMu.Unlock()
	l.registered = true
}

func (l *logLimiter) unregisterIfIdle() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.registered || l.hasPendingLocked() {
		return
	}
	logLimitersMu.Lock()
	for i, other := range logLimiters {
		if other == l {
			logLimiters = append(logLimiters[:i], logLimiters[i+1:]...)
			break
		}
	}
	logLimitersMu.Unlock()
	l.registered = false
}

// exitProcess reports what the limiters still hold, then exits.
func exitProcess(code int) {
	flushLogLimiters(true)
	os.Exit(code)
}

// print writes lines outside the limiter's lock, so a slow or blocked output
// (e.g. a stdout nobody drains) does not stall callers that are only counted,
// nor errors, which go to stderr.
func (l *logLimiter) print(lines []logLine) {
	for _, line := range lines {
		l.out(line.level, line.ts, line.msg)
	}
}

func (l *logLimiter) Log(level string, msg string) {
	l.print(l.record(level, msg))
}

// record updates the limiter state for one call and returns what to print.
func (l *logLimiter) record(level string, msg string) []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	defer l.registerIfPendingLocked()
	now := l.now()

	if l.disabled || l.closed || (l.tracked && logFinalFlushed.Load()) {
		return []logLine{{level, now, msg}}
	}

	var lines []logLine
	key := level + "\x00" + normalizeLogKey(msg)
	if e, ok := l.entries[key]; ok {
		if now.Sub(e.windowStart) < l.window {
			if e.repeats == 0 {
				e.firstRepeat = now
			}
			e.repeats++
			e.lastRepeat = now
			e.lastMsg = msg
			return nil
		}
		if e.repeats > 0 {
			// Still repeating: report the window that is over and fold
			// this occurrence into the next one, so a steady flood costs
			// one line per window.
			lines = l.summary(lines, e, now)
			e.windowStart = now
			e.repeats = 1
			e.firstRepeat = now
			e.lastRepeat = now
			e.lastMsg = msg
			return lines
		}
		// It went quiet for a whole window: print it in full again.
		delete(l.entries, key)
	}

	if !l.take(level, now) {
		b := l.buckets[level]
		if b.dropped == 0 {
			b.firstDropped = now
		}
		b.dropped++
		b.lastDropped = msg
		return nil
	}

	if len(l.entries) < l.maxKeys {
		l.entries[key] = &logEntry{level: level, windowStart: now}
	}
	return append(lines, logLine{level, now, msg})
}

// take consumes one token of the level's bucket, refilling it for the time
// elapsed since the last call.
func (l *logLimiter) take(level string, now time.Time) bool {
	b, ok := l.buckets[level]
	if !ok {
		b = &logBucket{tokens: l.burst, lastRefill: now}
		l.buckets[level] = b
	}
	if elapsed := now.Sub(b.lastRefill).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.lastRefill = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (l *logLimiter) summary(lines []logLine, e *logEntry, now time.Time) []logLine {
	if e.repeats == 0 {
		return lines
	}
	return append(lines, logLine{e.level, now, fmt.Sprintf("last message repeated %d more time(s) between %s and %s: %s",
		e.repeats, e.firstRepeat.Format(time.Stamp), e.lastRepeat.Format(time.Stamp), e.lastMsg)})
}

func (l *logLimiter) droppedSummary(lines []logLine, level string, b *logBucket, now time.Time) []logLine {
	if b.dropped == 0 {
		return lines
	}
	lines = append(lines, logLine{level, now, fmt.Sprintf("log rate limit (%d/min) reached: dropped %d %s line(s) since %s, last dropped: %s",
		logRatePerMinute, b.dropped, level, b.firstDropped.Format(time.Stamp), b.lastDropped)})
	b.dropped = 0
	b.lastDropped = ""
	return lines
}

// flush reports the repeats of every message whose window is over, and the
// lines dropped by the rate limit. A message still repeating keeps being
// folded, one summary per window; one that went quiet is forgotten, so its
// next occurrence prints in full. With final set (on exit), everything
// pending is reported regardless of windows, and the limiter stops folding.
func (l *logLimiter) flush(final bool) {
	l.print(l.collect(final))
}

func (l *logLimiter) collect(final bool) []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()

	var lines []logLine
	for key, e := range l.entries {
		if !final && now.Sub(e.windowStart) < l.window {
			continue
		}
		lines = l.summary(lines, e, now)
		if e.repeats == 0 || final {
			delete(l.entries, key)
			continue
		}
		e.windowStart = now
		e.repeats = 0
	}
	for level, b := range l.buckets {
		lines = l.droppedSummary(lines, level, b, now)
	}
	if final {
		l.closed = true
	}
	return lines
}
