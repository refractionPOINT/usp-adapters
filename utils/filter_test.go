package utils

import (
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient/protocol"
)

func TestFilterEngine(t *testing.T) {
	tests := []struct {
		name            string
		patterns        []string
		message         *protocol.DataMessage
		shouldFilter    bool
		matchedPattern  string
	}{
		{
			name:     "text payload matches",
			patterns: []string{"health-?check"},
			message: &protocol.DataMessage{
				TextPayload: "GET /health-check HTTP/1.1",
			},
			shouldFilter:   true,
			matchedPattern: "health-?check",
		},
		{
			name:     "text payload no match",
			patterns: []string{"health-?check"},
			message: &protocol.DataMessage{
				TextPayload: "GET /api/users HTTP/1.1",
			},
			shouldFilter:   false,
			matchedPattern: "",
		},
		{
			name:     "json payload matches",
			patterns: []string{`"level":"debug"`},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "debug",
					"message": "Processing request",
				},
			},
			shouldFilter:   true,
			matchedPattern: `"level":"debug"`,
		},
		{
			name:     "case insensitive match",
			patterns: []string{"(?i)password"},
			message: &protocol.DataMessage{
				TextPayload: "User entered PASSWORD incorrectly",
			},
			shouldFilter:   true,
			matchedPattern: "(?i)password",
		},
		{
			name:     "multiple patterns - first matches",
			patterns: []string{"health-?check", "monitoring", "ping"},
			message: &protocol.DataMessage{
				TextPayload: "GET /healthcheck HTTP/1.1",
			},
			shouldFilter:   true,
			matchedPattern: "health-?check",
		},
		{
			name:     "multiple patterns - second matches",
			patterns: []string{"health-?check", "(?i)monitoring", "ping"},
			message: &protocol.DataMessage{
				TextPayload: "Monitoring probe detected",
			},
			shouldFilter:   true,
			matchedPattern: "(?i)monitoring",
		},
		{
			name:     "complex regex",
			patterns: []string{`\b(DEBUG|TRACE)\b`},
			message: &protocol.DataMessage{
				TextPayload: "[DEBUG] Starting application",
			},
			shouldFilter:   true,
			matchedPattern: `\b(DEBUG|TRACE)\b`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logMessages []string
			logger := func(msg string) {
				logMessages = append(logMessages, msg)
			}

			fe, err := NewFilterEngine(tt.patterns, logger)
			if err != nil {
				t.Fatalf("Failed to create filter engine: %v", err)
			}
			defer fe.Close()

			shouldFilter, matchedPattern := fe.ShouldFilter(tt.message)

			if shouldFilter != tt.shouldFilter {
				t.Errorf("ShouldFilter() = %v, want %v", shouldFilter, tt.shouldFilter)
			}

			if matchedPattern != tt.matchedPattern {
				t.Errorf("matchedPattern = %q, want %q", matchedPattern, tt.matchedPattern)
			}

			// Check statistics
			stats := fe.GetStats()
			if stats.TotalChecked != 1 {
				t.Errorf("TotalChecked = %d, want 1", stats.TotalChecked)
			}

			expectedFiltered := uint64(0)
			if tt.shouldFilter {
				expectedFiltered = 1
			}
			if stats.TotalFiltered != expectedFiltered {
				t.Errorf("TotalFiltered = %d, want %d", stats.TotalFiltered, expectedFiltered)
			}
		})
	}
}

func TestFilterEngineInvalidPattern(t *testing.T) {
	logger := func(msg string) {}

	_, err := NewFilterEngine([]string{"[invalid"}, logger)
	if err == nil {
		t.Error("Expected error for invalid regex pattern, got nil")
	}
}

func TestFilterEngineStats(t *testing.T) {
	var logMessages []string
	logger := func(msg string) {
		logMessages = append(logMessages, msg)
	}

	fe, err := NewFilterEngine([]string{"pattern1", "pattern2"}, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	// Test multiple messages
	messages := []*protocol.DataMessage{
		{TextPayload: "contains pattern1 here"},
		{TextPayload: "contains pattern2 here"},
		{TextPayload: "contains pattern1 again"},
		{TextPayload: "no match"},
		{TextPayload: "another pattern1"},
	}

	for _, msg := range messages {
		fe.ShouldFilter(msg)
	}

	stats := fe.GetStats()

	if stats.TotalChecked != 5 {
		t.Errorf("TotalChecked = %d, want 5", stats.TotalChecked)
	}

	if stats.TotalFiltered != 4 {
		t.Errorf("TotalFiltered = %d, want 4", stats.TotalFiltered)
	}

	// Check per-pattern stats
	if len(stats.PerPattern) != 2 {
		t.Errorf("len(PerPattern) = %d, want 2", len(stats.PerPattern))
	}

	if stats.PerPattern[0].Matches != 3 {
		t.Errorf("Pattern 0 matches = %d, want 3", stats.PerPattern[0].Matches)
	}

	if stats.PerPattern[1].Matches != 1 {
		t.Errorf("Pattern 1 matches = %d, want 1", stats.PerPattern[1].Matches)
	}
}

func TestFilterEngineEmptyPayload(t *testing.T) {
	logger := func(msg string) {}

	fe, err := NewFilterEngine([]string{"test"}, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	// Test with empty message
	msg := &protocol.DataMessage{}
	shouldFilter, _ := fe.ShouldFilter(msg)

	if shouldFilter {
		t.Error("Empty message should not be filtered")
	}
}

func BenchmarkFilterEngine(b *testing.B) {
	logger := func(msg string) {}

	fe, err := NewFilterEngine([]string{"health-?check", "monitoring", "debug"}, logger)
	if err != nil {
		b.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	msg := &protocol.DataMessage{
		TextPayload: "GET /api/users HTTP/1.1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}

func TestFilterEngineStatsReporting(t *testing.T) {
	// This test verifies that stats are reported correctly
	// We won't test the 5-minute timer, but we'll verify the logging works

	var logMessages []string
	logger := func(msg string) {
		logMessages = append(logMessages, msg)
	}

	fe, err := NewFilterEngine([]string{"test"}, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}

	// Process some messages
	for i := 0; i < 10; i++ {
		fe.ShouldFilter(&protocol.DataMessage{TextPayload: "test message"})
	}

	// Manually trigger stats logging
	fe.logStats(true)

	// Check that final stats were logged
	foundStats := false
	for _, msg := range logMessages {
		// Check if message contains "filter stats" or "Final"
		if len(msg) > 0 {
			t.Logf("Log message: %s", msg)
			if len(msg) >= 5 && (msg[:5] == "Final" || msg[:6] == "Filter") {
				foundStats = true
				break
			}
		}
	}

	if !foundStats {
		t.Errorf("Expected stats logging, but found none. Got %d log messages total", len(logMessages))
	}

	fe.Close()

	// Give some time for final stats
	time.Sleep(10 * time.Millisecond)
}
