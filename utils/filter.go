package utils

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
)

// LogFunc is a function that logs a message.
type LogFunc func(string)

// PatternStats tracks statistics for a single pattern.
type PatternStats struct {
	Pattern string
	Matches uint64
}

// FilterStats contains filter statistics.
type FilterStats struct {
	TotalChecked   uint64
	TotalFiltered  uint64
	PerPattern     []PatternStats
	LastReportTime time.Time
}

// FilterEngine manages filtering logic and statistics.
type FilterEngine struct {
	patterns      []*regexp.Regexp
	rawPatterns   []string
	stats         []uint64 // Per-pattern match counts (atomic)
	totalChecked  uint64   // Atomic counter
	totalFiltered uint64   // Atomic counter
	logger        LogFunc
	mutex         sync.RWMutex
	stopReporting chan struct{}
	wg            sync.WaitGroup
	lastReportTime time.Time
}

// NewFilterEngine creates a new filter engine with the given patterns.
func NewFilterEngine(patterns []string, logger LogFunc) (*FilterEngine, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("no patterns provided")
	}

	if logger == nil {
		logger = func(string) {} // No-op logger
	}

	fe := &FilterEngine{
		rawPatterns:    patterns,
		patterns:       make([]*regexp.Regexp, 0, len(patterns)),
		stats:          make([]uint64, len(patterns)),
		logger:         logger,
		stopReporting:  make(chan struct{}),
		lastReportTime: time.Now(),
	}

	// Compile all patterns
	for i, pattern := range patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile pattern %d (%q): %w", i, pattern, err)
		}
		fe.patterns = append(fe.patterns, re)
	}

	// Start background stats reporter
	fe.startStatsReporter()

	logger(fmt.Sprintf("Filter engine initialized with %d patterns", len(patterns)))

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
		elapsed := time.Since(fe.lastReportTime)
		prefix = fmt.Sprintf("Filter stats (last %s)", elapsed.Round(time.Second))
	}

	percentage := float64(0)
	if stats.TotalChecked > 0 {
		percentage = float64(stats.TotalFiltered) / float64(stats.TotalChecked) * 100
	}

	fe.logger(fmt.Sprintf("%s: checked=%d, filtered=%d (%.2f%%)",
		prefix, stats.TotalChecked, stats.TotalFiltered, percentage))

	for _, ps := range stats.PerPattern {
		if ps.Matches > 0 {
			fe.logger(fmt.Sprintf("  - Pattern %q: %d matches", ps.Pattern, ps.Matches))
		}
	}

	fe.mutex.Lock()
	fe.lastReportTime = time.Now()
	fe.mutex.Unlock()
}

// GetStats returns a snapshot of current statistics.
func (fe *FilterEngine) GetStats() FilterStats {
	stats := FilterStats{
		TotalChecked:   atomic.LoadUint64(&fe.totalChecked),
		TotalFiltered:  atomic.LoadUint64(&fe.totalFiltered),
		PerPattern:     make([]PatternStats, len(fe.patterns)),
		LastReportTime: fe.lastReportTime,
	}

	for i, pattern := range fe.rawPatterns {
		stats.PerPattern[i] = PatternStats{
			Pattern: pattern,
			Matches: atomic.LoadUint64(&fe.stats[i]),
		}
	}

	return stats
}

// ShouldFilter checks if a message should be filtered out.
// Returns (true, pattern) if the message should be filtered, (false, "") otherwise.
func (fe *FilterEngine) ShouldFilter(msg *protocol.DataMessage) (bool, string) {
	atomic.AddUint64(&fe.totalChecked, 1)

	// Extract payload as string for matching
	payload := fe.extractPayload(msg)
	if payload == "" {
		return false, "" // Nothing to match against
	}

	// Check against all patterns
	for i, pattern := range fe.patterns {
		if pattern.MatchString(payload) {
			atomic.AddUint64(&fe.stats[i], 1)
			atomic.AddUint64(&fe.totalFiltered, 1)
			return true, fe.rawPatterns[i]
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
			fe.logger(fmt.Sprintf("Failed to marshal JsonPayload for filtering: %v", err))
			return ""
		}
		return string(jsonBytes)
	}

	// Skip binary/bundle payloads
	return ""
}

// Close stops the background stats reporter and logs final statistics.
func (fe *FilterEngine) Close() {
	close(fe.stopReporting)
	fe.wg.Wait()
	fe.logStats(true)
}
