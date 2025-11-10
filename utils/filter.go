package utils

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/tidwall/gjson"
)

// LogFunc is a function that logs a message.
type LogFunc func(string)

// FilterPattern defines a filter rule for matching and filtering messages.
//
// Two pattern types are supported:
//
//  1. "regex" - Matches against the full message payload (TextPayload or marshaled JsonPayload)
//  2. "gjson" - Extracts a JSON field using gjson path syntax, then matches with regex (10-50x faster)
//
// Example configurations:
//
//	filters:
//	  # Full text regex matching
//	  - type: regex
//	    pattern: "health-?check"
//
//	  # Fast JSON field matching
//	  - type: gjson
//	    path: "level"
//	    pattern: "^(DEBUG|TRACE)$"
//
//	  # Nested field matching
//	  - type: gjson
//	    path: "user.name"
//	    pattern: "^test-.*"
//
//	  # Array query matching
//	  - type: gjson
//	    path: "users.#(age>45).email"
//	    pattern: ".*@test\\.com"
type FilterPattern struct {
	// Type specifies the filter pattern type: "regex" or "gjson"
	Type string `json:"type" yaml:"type"`

	// Pattern is the regex pattern to match against.
	// - For type="regex": matches against full payload (TextPayload or marshaled JsonPayload)
	// - For type="gjson": matches against the value extracted by Path
	Pattern string `json:"pattern" yaml:"pattern"`

	// Path is the gjson path expression (required for type="gjson", ignored for type="regex").
	//
	// GJSON Path Syntax Examples:
	//   "level"                     - Top-level field
	//   "user.name"                 - Nested field
	//   "items.0.id"                - Array index
	//   "items.#"                   - Array length
	//   "items.#.name"              - Extract field from all array elements
	//   "users.#(age>45)"           - First match with condition
	//   "users.#(age>45)#"          - All matches with condition
	//   "data.*.value"              - Wildcard matching
	//   "tags|@reverse|0"           - Using modifiers
	//
	// See https://github.com/tidwall/gjson/blob/master/SYNTAX.md for complete syntax reference.
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
}

// Validate checks if the FilterPattern is valid and returns an error if not.
func (fp *FilterPattern) Validate() error {
	if fp.Type != "regex" && fp.Type != "gjson" {
		return fmt.Errorf("invalid filter type %q, must be 'regex' or 'gjson'", fp.Type)
	}

	if strings.TrimSpace(fp.Pattern) == "" {
		return fmt.Errorf("pattern cannot be empty or whitespace-only")
	}

	if fp.Type == "gjson" && strings.TrimSpace(fp.Path) == "" {
		return fmt.Errorf("path is required for gjson filter type")
	}

	// Validate pattern is valid regex
	if _, err := regexp.Compile(fp.Pattern); err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	return nil
}

// PatternStats tracks statistics for a single pattern.
type PatternStats struct {
	Pattern string
	Matches uint64
}

// FilterStats contains filter statistics.
type FilterStats struct {
	TotalChecked     uint64
	TotalFiltered    uint64
	MarshalFailures  uint64
	PerPattern       []PatternStats
	LastReportTime   time.Time
}

// regexMatcher represents a compiled regex pattern matcher.
type regexMatcher struct {
	pattern *regexp.Regexp
	index   int // Index in stats array
}

// gjsonMatcher represents a compiled gjson path + regex pattern matcher.
type gjsonMatcher struct {
	path    string
	pattern *regexp.Regexp
	index   int // Index in stats array
}

// FilterEngine manages filtering logic and statistics.
type FilterEngine struct {
	// Compiled matchers by type
	regexMatchers []*regexMatcher
	gjsonMatchers []*gjsonMatcher

	// Raw patterns for stats reporting
	rawPatterns []FilterPattern

	// Statistics (one entry per pattern)
	stats           []uint64 // Per-pattern match counts (atomic)
	totalChecked    uint64   // Atomic counter
	totalFiltered   uint64   // Atomic counter
	marshalFailures uint64   // Atomic counter for JSON marshal failures

	logger         LogFunc
	mutex          sync.RWMutex
	stopReporting  chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	lastReportTime time.Time
}

// NewFilterEngine creates a new filter engine with the given patterns.
func NewFilterEngine(patterns []FilterPattern, logger LogFunc) (*FilterEngine, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("no patterns provided")
	}

	if logger == nil {
		logger = func(string) {} // No-op logger
	}

	fe := &FilterEngine{
		rawPatterns:   patterns,
		regexMatchers: make([]*regexMatcher, 0),
		gjsonMatchers: make([]*gjsonMatcher, 0),
		stats:         make([]uint64, len(patterns)),
		logger:        logger,
		stopReporting: make(chan struct{}),
		lastReportTime: time.Now(),
	}

	// Compile and categorize each pattern
	for i, pat := range patterns {
		// Validate pattern
		if err := pat.Validate(); err != nil {
			return nil, fmt.Errorf("pattern %d validation failed: %w", i, err)
		}

		// Compile regex
		re, err := regexp.Compile(pat.Pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile pattern %d: %w", i, err)
		}

		// Add to appropriate matcher list
		switch pat.Type {
		case "regex":
			fe.regexMatchers = append(fe.regexMatchers, &regexMatcher{
				pattern: re,
				index:   i,
			})
		case "gjson":
			fe.gjsonMatchers = append(fe.gjsonMatchers, &gjsonMatcher{
				path:    pat.Path,
				pattern: re,
				index:   i,
			})
		default:
			return nil, fmt.Errorf("unknown filter type %q in pattern %d", pat.Type, i)
		}
	}

	// Start background stats reporter
	fe.startStatsReporter()

	logger(fmt.Sprintf("Filter engine initialized with %d patterns (%d regex, %d gjson)",
		len(patterns), len(fe.regexMatchers), len(fe.gjsonMatchers)))

	return fe, nil
}

// startStatsReporter starts a background goroutine that logs stats every 5 minutes.
func (fe *FilterEngine) startStatsReporter() {
	fe.wg.Add(1)
	go func() {
		defer fe.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				fe.logStats(false)
			case <-fe.stopReporting:
				return
			}
		}
	}()
}

// logStats logs current statistics.
func (fe *FilterEngine) logStats(isFinal bool) {
	stats := fe.GetStats()

	if stats.TotalChecked == 0 {
		return // No activity yet
	}

	prefix := "Filter stats"
	if isFinal {
		prefix = "Final filter stats"
	} else {
		elapsed := time.Since(stats.LastReportTime)
		prefix = fmt.Sprintf("Filter stats (last %s)", elapsed.Round(time.Second))
	}

	percentage := float64(0)
	if stats.TotalChecked > 0 {
		percentage = float64(stats.TotalFiltered) / float64(stats.TotalChecked) * 100
	}

	fe.logger(fmt.Sprintf("%s: checked=%d, filtered=%d (%.2f%%)",
		prefix, stats.TotalChecked, stats.TotalFiltered, percentage))

	// Log marshal failures if any occurred
	if stats.MarshalFailures > 0 {
		fe.logger(fmt.Sprintf("  - JSON marshal failures: %d", stats.MarshalFailures))
	}

	for i, ps := range stats.PerPattern {
		if ps.Matches > 0 {
			pat := fe.rawPatterns[i]
			if pat.Type == "gjson" {
				fe.logger(fmt.Sprintf("  - Pattern %d [gjson] path=%q, pattern=%q: %d matches",
					i, pat.Path, pat.Pattern, ps.Matches))
			} else {
				fe.logger(fmt.Sprintf("  - Pattern %d [regex] pattern=%q: %d matches",
					i, pat.Pattern, ps.Matches))
			}
		}
	}

	fe.mutex.Lock()
	fe.lastReportTime = time.Now()
	fe.mutex.Unlock()
}

// GetStats returns a snapshot of current statistics.
func (fe *FilterEngine) GetStats() FilterStats {
	// Read lastReportTime with lock to avoid race condition
	fe.mutex.RLock()
	lastReport := fe.lastReportTime
	fe.mutex.RUnlock()

	stats := FilterStats{
		TotalChecked:    atomic.LoadUint64(&fe.totalChecked),
		TotalFiltered:   atomic.LoadUint64(&fe.totalFiltered),
		MarshalFailures: atomic.LoadUint64(&fe.marshalFailures),
		PerPattern:      make([]PatternStats, len(fe.rawPatterns)),
		LastReportTime:  lastReport,
	}

	for i, pattern := range fe.rawPatterns {
		// Create a string representation of the pattern
		patternStr := pattern.Pattern
		if pattern.Type == "gjson" {
			patternStr = fmt.Sprintf("gjson:%s:%s", pattern.Path, pattern.Pattern)
		}
		stats.PerPattern[i] = PatternStats{
			Pattern: patternStr,
			Matches: atomic.LoadUint64(&fe.stats[i]),
		}
	}

	return stats
}

// ShouldFilter checks if a message should be filtered out.
// Returns (true, pattern) if the message should be filtered, (false, "") otherwise.
func (fe *FilterEngine) ShouldFilter(msg *protocol.DataMessage) (bool, string) {
	atomic.AddUint64(&fe.totalChecked, 1)

	// Fast path: Check gjson patterns first (no full marshaling needed)
	if msg.JsonPayload != nil && len(fe.gjsonMatchers) > 0 {
		// Marshal JSON once for all gjson queries
		jsonBytes, err := json.Marshal(msg.JsonPayload)
		if err != nil {
			atomic.AddUint64(&fe.marshalFailures, 1)
			fe.logger(fmt.Sprintf("Failed to marshal JsonPayload for gjson filtering: %v", err))
		} else {
			jsonStr := string(jsonBytes)

			// Check all gjson patterns
			for _, gm := range fe.gjsonMatchers {
				result := gjson.Get(jsonStr, gm.path)
				if result.Exists() {
					value := result.String()
					if gm.pattern.MatchString(value) {
						atomic.AddUint64(&fe.stats[gm.index], 1)
						atomic.AddUint64(&fe.totalFiltered, 1)
						pat := fe.rawPatterns[gm.index]
						patternDesc := fmt.Sprintf("gjson(path=%q, pattern=%q)", pat.Path, pat.Pattern)
						fe.logger(fmt.Sprintf("Filtered: matched %s in JsonPayload", patternDesc))
						return true, patternDesc
					}
				}
			}
		}
	}

	// Slow path: Check regex patterns (requires full payload)
	payload := fe.extractPayload(msg)
	if payload == "" {
		return false, ""
	}

	for _, rm := range fe.regexMatchers {
		if rm.pattern.MatchString(payload) {
			atomic.AddUint64(&fe.stats[rm.index], 1)
			atomic.AddUint64(&fe.totalFiltered, 1)
			pat := fe.rawPatterns[rm.index]
			patternDesc := fmt.Sprintf("regex(%q)", pat.Pattern)
			fe.logger(fmt.Sprintf("Filtered: matched %s", patternDesc))
			return true, patternDesc
		}
	}

	return false, ""
}

// extractPayload extracts the payload from a DataMessage as a string.
func (fe *FilterEngine) extractPayload(msg *protocol.DataMessage) string {
	// Check TextPayload first
	if msg.TextPayload != "" {
		return msg.TextPayload
	}

	// Check JsonPayload
	if msg.JsonPayload != nil {
		// Marshal to JSON string
		jsonBytes, err := json.Marshal(msg.JsonPayload)
		if err != nil {
			atomic.AddUint64(&fe.marshalFailures, 1)
			fe.logger(fmt.Sprintf("Failed to marshal JsonPayload for filtering: %v", err))
			return ""
		}
		return string(jsonBytes)
	}

	// Skip binary/bundle payloads
	return ""
}

// Close stops the background stats reporter and logs final statistics.
// This method is idempotent and safe to call multiple times.
func (fe *FilterEngine) Close() {
	fe.closeOnce.Do(func() {
		close(fe.stopReporting)
		fe.wg.Wait()
		fe.logStats(true)
	})
}
