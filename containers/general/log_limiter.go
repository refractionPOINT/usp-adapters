package main

import (
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"
)

const (
	// logDedupWindow is how long repeats of a message are folded into one
	// summary line after the message is first printed.
	logDedupWindow = 1 * time.Minute
	// logRatePerMinute and logRateBurst bound how many lines of one level are
	// printed per minute. They only matter when many *different* messages
	// flood in; repeats of the same message are already folded by dedup.
	logRatePerMinute = 600
	logRateBurst     = 600
	// logMaxTrackedKeys bounds the dedup table. Messages past it are still
	// printed (subject to the rate limit), just not deduplicated.
	logMaxTrackedKeys = 1000
	// logFlushInterval is how often pending summaries are checked.
	logFlushInterval = 5 * time.Second

	// logDedupDisableEnv turns the limiter off: every line is printed as is.
	logDedupDisableEnv = "USP_LOG_NO_DEDUP"
)

// reIPPort matches an IPv4 or bracketed IPv6 address followed by a port. The
// port is masked in dedup keys so that the same error from the same host does
// not look unique only because each connection uses a new ephemeral port. The
// address itself is kept: a failure from another host is another message.
var reIPPort = regexp.MustCompile(`(\b\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-fA-F:.%a-zA-Z]+\]):\d{1,5}\b`)

func normalizeLogKey(msg string) string {
	return reIPPort.ReplaceAllString(msg, "$1:<port>")
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

// logLimiter sits between the adapters' DebugLog/OnWarning/OnError callbacks
// and the process output. It never discards a message without accounting for
// it: the first occurrence of a message is printed as is, repeats within
// logDedupWindow are counted and reported in a summary line carrying the most
// recent text, and lines dropped by the rate limit are reported the same way.
type logLimiter struct {
	mu       sync.Mutex
	disabled bool
	window   time.Duration
	rate     float64 // lines per second, per level
	burst    float64
	maxKeys  int
	now      func() time.Time
	out      func(level string, ts time.Time, msg string)
	entries  map[string]*logEntry
	buckets  map[string]*logBucket
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

var processLogLimiter = func() *logLimiter {
	l := newLogLimiter(printLogLine)
	if v := os.Getenv(logDedupDisableEnv); v != "" && v != "0" && v != "false" {
		l.disabled = true
	}
	return l
}()

var startLogFlusherOnce sync.Once

// startFlusher prints pending summaries in the background so a burst that
// stops is still reported without waiting for the next log line.
func (l *logLimiter) startFlusher() {
	startLogFlusherOnce.Do(func() {
		if l.disabled {
			log("log deduplication disabled by %s", logDedupDisableEnv)
			return
		}
		log("log deduplication enabled: repeats within %v are summarized, set %s=1 to disable", l.window, logDedupDisableEnv)
		go func() {
			for {
				time.Sleep(logFlushInterval)
				l.flush(false)
			}
		}()
	})
}

func (l *logLimiter) Log(level string, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()

	if l.disabled {
		l.out(level, now, msg)
		return
	}

	key := level + "\x00" + normalizeLogKey(msg)
	if e, ok := l.entries[key]; ok {
		if now.Sub(e.windowStart) < l.window {
			if e.repeats == 0 {
				e.firstRepeat = now
			}
			e.repeats++
			e.lastRepeat = now
			e.lastMsg = msg
			return
		}
		if e.repeats > 0 {
			// Still repeating: report the window that is over and fold
			// this occurrence into the next one, so a steady flood costs
			// one line per window.
			l.emitSummary(e, now)
			e.windowStart = now
			e.repeats = 1
			e.firstRepeat = now
			e.lastRepeat = now
			e.lastMsg = msg
			return
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
		return
	}

	l.out(level, now, msg)
	if len(l.entries) < l.maxKeys {
		l.entries[key] = &logEntry{level: level, windowStart: now}
	}
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

func (l *logLimiter) emitSummary(e *logEntry, now time.Time) {
	if e.repeats == 0 {
		return
	}
	l.out(e.level, now, fmt.Sprintf("last message repeated %d more time(s) between %s and %s: %s",
		e.repeats, e.firstRepeat.Format(time.Stamp), e.lastRepeat.Format(time.Stamp), e.lastMsg))
}

func (l *logLimiter) emitDropped(level string, b *logBucket, now time.Time) {
	if b.dropped == 0 {
		return
	}
	l.out(level, now, fmt.Sprintf("log rate limit (%d/min) reached: dropped %d %s line(s) since %s, last dropped: %s",
		logRatePerMinute, b.dropped, level, b.firstDropped.Format(time.Stamp), b.lastDropped))
	b.dropped = 0
	b.lastDropped = ""
}

// flush reports the repeats of every message whose window is over, and the
// lines dropped by the rate limit. A message still repeating keeps being
// folded, one summary per window; one that went quiet is forgotten, so its
// next occurrence prints in full. With final set (on exit), everything
// pending is reported regardless of windows.
func (l *logLimiter) flush(final bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()

	for key, e := range l.entries {
		if !final && now.Sub(e.windowStart) < l.window {
			continue
		}
		if e.repeats == 0 || final {
			l.emitSummary(e, now)
			delete(l.entries, key)
			continue
		}
		l.emitSummary(e, now)
		e.windowStart = now
		e.repeats = 0
	}
	for level, b := range l.buckets {
		if final || (b.dropped != 0 && now.Sub(b.firstDropped) >= l.window) {
			l.emitDropped(level, b, now)
		}
	}
}
