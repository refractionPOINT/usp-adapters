package utils

import (
	"testing"

	"github.com/refractionPOINT/go-uspclient/protocol"
)

// TestFilterPatternValidation tests the FilterPattern Validate method
func TestFilterPatternValidation(t *testing.T) {
	tests := []struct {
		name    string
		pattern FilterPattern
		wantErr bool
	}{
		{
			name:    "valid regex pattern",
			pattern: FilterPattern{Type: "regex", Pattern: "test"},
			wantErr: false,
		},
		{
			name:    "valid gjson pattern",
			pattern: FilterPattern{Type: "gjson", Path: "level", Pattern: "DEBUG"},
			wantErr: false,
		},
		{
			name:    "invalid type",
			pattern: FilterPattern{Type: "invalid", Pattern: "test"},
			wantErr: true,
		},
		{
			name:    "empty pattern",
			pattern: FilterPattern{Type: "regex", Pattern: ""},
			wantErr: true,
		},
		{
			name:    "whitespace-only pattern",
			pattern: FilterPattern{Type: "regex", Pattern: "   "},
			wantErr: true,
		},
		{
			name:    "gjson without path",
			pattern: FilterPattern{Type: "gjson", Pattern: "test"},
			wantErr: true,
		},
		{
			name:    "gjson with empty path",
			pattern: FilterPattern{Type: "gjson", Path: "", Pattern: "test"},
			wantErr: true,
		},
		{
			name:    "invalid regex pattern",
			pattern: FilterPattern{Type: "regex", Pattern: "[invalid"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pattern.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestFilterEngineRegexPatterns tests regex-based filtering
func TestFilterEngineRegexPatterns(t *testing.T) {
	tests := []struct {
		name           string
		patterns       []FilterPattern
		message        *protocol.DataMessage
		shouldFilter   bool
		matchedPattern string
	}{
		{
			name: "text payload matches",
			patterns: []FilterPattern{
				{Type: "regex", Pattern: "health-?check"},
			},
			message: &protocol.DataMessage{
				TextPayload: "GET /health-check HTTP/1.1",
			},
			shouldFilter:   true,
			matchedPattern: `regex("health-?check")`,
		},
		{
			name: "text payload no match",
			patterns: []FilterPattern{
				{Type: "regex", Pattern: "health-?check"},
			},
			message: &protocol.DataMessage{
				TextPayload: "GET /api/users HTTP/1.1",
			},
			shouldFilter:   false,
			matchedPattern: "",
		},
		{
			name: "json payload marshaled match",
			patterns: []FilterPattern{
				{Type: "regex", Pattern: `"level":"debug"`},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "debug",
					"message": "Processing request",
				},
			},
			shouldFilter:   true,
			matchedPattern: `regex("\"level\":\"debug\"")`,
		},
		{
			name: "case insensitive match",
			patterns: []FilterPattern{
				{Type: "regex", Pattern: "(?i)password"},
			},
			message: &protocol.DataMessage{
				TextPayload: "User entered PASSWORD incorrectly",
			},
			shouldFilter:   true,
			matchedPattern: `regex("(?i)password")`,
		},
		{
			name: "complex regex",
			patterns: []FilterPattern{
				{Type: "regex", Pattern: `\b(DEBUG|TRACE)\b`},
			},
			message: &protocol.DataMessage{
				TextPayload: "[DEBUG] Starting application",
			},
			shouldFilter:   true,
			matchedPattern: `regex("\\b(DEBUG|TRACE)\\b")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logMessages []string
			logger := func(msg string) {
				logMessages = append(logMessages, msg)
			}

			fe, err := NewFilterEngine(tt.patterns, FilterModeExclude, logger)
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
		})
	}
}

// TestFilterEngineGJSONBasic tests basic gjson path filtering
func TestFilterEngineGJSONBasic(t *testing.T) {
	tests := []struct {
		name           string
		patterns       []FilterPattern
		message        *protocol.DataMessage
		shouldFilter   bool
		matchedPattern string
	}{
		{
			name: "simple field match",
			patterns: []FilterPattern{
				{Type: "gjson", Path: "level", Pattern: "DEBUG"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "DEBUG",
					"message": "test",
				},
			},
			shouldFilter:   true,
			matchedPattern: `gjson(path="level", pattern="DEBUG")`,
		},
		{
			name: "nested field match",
			patterns: []FilterPattern{
				{Type: "gjson", Path: "user.name", Pattern: "admin"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"user": map[string]interface{}{
						"name": "admin",
						"id":   123,
					},
				},
			},
			shouldFilter:   true,
			matchedPattern: `gjson(path="user.name", pattern="admin")`,
		},
		{
			name: "array index access",
			patterns: []FilterPattern{
				{Type: "gjson", Path: "items.0.type", Pattern: "test"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"items": []interface{}{
						map[string]interface{}{"type": "test", "id": 1},
						map[string]interface{}{"type": "prod", "id": 2},
					},
				},
			},
			shouldFilter:   true,
			matchedPattern: `gjson(path="items.0.type", pattern="test")`,
		},
		{
			name: "field not found - no match",
			patterns: []FilterPattern{
				{Type: "gjson", Path: "nonexistent", Pattern: ".*"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level": "INFO",
				},
			},
			shouldFilter:   false,
			matchedPattern: "",
		},
		{
			name: "regex pattern on gjson value",
			patterns: []FilterPattern{
				{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level": "DEBUG",
				},
			},
			shouldFilter:   true,
			matchedPattern: `gjson(path="level", pattern="^(DEBUG|TRACE)$")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := func(msg string) {
				t.Logf("Log: %s", msg)
			}

			fe, err := NewFilterEngine(tt.patterns, FilterModeExclude, logger)
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
		})
	}
}

// TestFilterEngineGJSONQueries tests gjson query syntax
func TestFilterEngineGJSONQueries(t *testing.T) {
	tests := []struct {
		name         string
		patterns     []FilterPattern
		message      *protocol.DataMessage
		shouldFilter bool
	}{
		{
			name: "array query - single match",
			patterns: []FilterPattern{
				{Type: "gjson", Path: `users.#(age>45).name`, Pattern: ".*"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"users": []interface{}{
						map[string]interface{}{"name": "Alice", "age": 30},
						map[string]interface{}{"name": "Bob", "age": 50},
					},
				},
			},
			shouldFilter: true,
		},
		{
			name: "array query - equality",
			patterns: []FilterPattern{
				{Type: "gjson", Path: `tags.#(=="internal")`, Pattern: "internal"},
			},
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"tags": []interface{}{"internal", "test", "debug"},
				},
			},
			shouldFilter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := func(msg string) {
				t.Logf("Log: %s", msg)
			}

			fe, err := NewFilterEngine(tt.patterns, FilterModeExclude, logger)
			if err != nil {
				t.Fatalf("Failed to create filter engine: %v", err)
			}
			defer fe.Close()

			shouldFilter, _ := fe.ShouldFilter(tt.message)

			if shouldFilter != tt.shouldFilter {
				t.Errorf("ShouldFilter() = %v, want %v", shouldFilter, tt.shouldFilter)
			}
		})
	}
}

// TestFilterEngineMixedPatterns tests using both regex and gjson patterns together
func TestFilterEngineMixedPatterns(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "health-?check"},
		{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
		{Type: "gjson", Path: "user.role", Pattern: "^test-.*"},
	}

	logger := func(msg string) {
		t.Logf("Log: %s", msg)
	}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	// Test gjson match (should match first)
	msg1 := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"level":   "DEBUG",
			"message": "test message with health-check",
		},
	}
	shouldFilter, matched := fe.ShouldFilter(msg1)
	if !shouldFilter {
		t.Error("Expected gjson pattern to match DEBUG level")
	}
	if matched != `gjson(path="level", pattern="^(DEBUG|TRACE)$")` {
		t.Errorf("Expected gjson match, got %q", matched)
	}

	// Test regex match (falls back after gjson)
	msg2 := &protocol.DataMessage{
		TextPayload: "GET /health-check HTTP/1.1",
	}
	shouldFilter, matched = fe.ShouldFilter(msg2)
	if !shouldFilter {
		t.Error("Expected regex pattern to match health-check")
	}
	if matched != `regex("health-?check")` {
		t.Errorf("Expected regex match, got %q", matched)
	}

	// Test no match
	msg3 := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"level": "INFO",
		},
	}
	shouldFilter, _ = fe.ShouldFilter(msg3)
	if shouldFilter {
		t.Error("Expected no match for INFO level")
	}
}

// TestFilterEngineStats tests statistics tracking
func TestFilterEngineStats(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "pattern1"},
		{Type: "gjson", Path: "level", Pattern: "DEBUG"},
	}

	logger := func(msg string) {}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	// Test multiple messages
	messages := []*protocol.DataMessage{
		{TextPayload: "contains pattern1 here"},
		{JsonPayload: map[string]interface{}{"level": "DEBUG"}},
		{TextPayload: "contains pattern1 again"},
		{TextPayload: "no match"},
		{JsonPayload: map[string]interface{}{"level": "DEBUG"}},
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

	// Pattern 0 (regex) should have 2 matches
	if stats.PerPattern[0].Matches != 2 {
		t.Errorf("Pattern 0 matches = %d, want 2", stats.PerPattern[0].Matches)
	}

	// Pattern 1 (gjson) should have 2 matches
	if stats.PerPattern[1].Matches != 2 {
		t.Errorf("Pattern 1 matches = %d, want 2", stats.PerPattern[1].Matches)
	}
}

// TestFilterEngineConcurrency tests thread-safety with concurrent access
func TestFilterEngineConcurrency(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "pattern1"},
		{Type: "gjson", Path: "level", Pattern: "DEBUG"},
	}

	logger := func(msg string) {}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	const numGoroutines = 50
	const messagesPerGoroutine = 100

	messages := []*protocol.DataMessage{
		{TextPayload: "contains pattern1"},
		{JsonPayload: map[string]interface{}{"level": "DEBUG"}},
		{TextPayload: "no match"},
		{JsonPayload: map[string]interface{}{"level": "INFO"}},
	}

	done := make(chan bool, numGoroutines)
	start := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Goroutine panicked: %v", r)
				}
				done <- true
			}()

			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := messages[j%len(messages)]
				fe.ShouldFilter(msg)

				if j%10 == 0 {
					_ = fe.GetStats()
				}
			}
		}()
	}

	close(start)

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	stats := fe.GetStats()
	expectedTotal := uint64(numGoroutines * messagesPerGoroutine)

	if stats.TotalChecked != expectedTotal {
		t.Errorf("TotalChecked = %d, want %d", stats.TotalChecked, expectedTotal)
	}
}

// TestFilterEngineDoubleClose tests idempotent Close()
func TestFilterEngineDoubleClose(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "test"},
	}

	logger := func(msg string) {}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}

	// Close multiple times - should not panic
	fe.Close()
	fe.Close()
	fe.Close()

	t.Log("Multiple Close() calls succeeded without panic")
}

// TestFilterEngineMarshalFailures tests handling of unmarshalable JSON
func TestFilterEngineMarshalFailures(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "test", Pattern: ".*"},
	}

	var logMessages []string
	logger := func(msg string) {
		logMessages = append(logMessages, msg)
	}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	// Create a message with JsonPayload containing an unmarshalable channel
	ch := make(chan int)
	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"channel": ch,
		},
	}

	shouldFilter, _ := fe.ShouldFilter(msg)

	if shouldFilter {
		t.Error("Expected message not to be filtered after marshal failure")
	}

	// Check that failure was counted in stats
	stats := fe.GetStats()
	if stats.MarshalFailures == 0 {
		t.Error("Expected MarshalFailures > 0, got 0")
	}
}

// TestFilterEngineMarshalFailuresNoDoubleCount tests that marshal failures are not double-counted
// when both gjson and regex patterns are configured
func TestFilterEngineMarshalFailuresNoDoubleCount(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "test", Pattern: ".*"},
		{Type: "regex", Pattern: "anything"},
	}

	logger := func(msg string) {}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	// Create a message with JsonPayload containing an unmarshalable channel
	ch := make(chan int)
	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"channel": ch,
		},
	}

	fe.ShouldFilter(msg)

	stats := fe.GetStats()
	// Should be exactly 1, not 2 (no double-counting)
	if stats.MarshalFailures != 1 {
		t.Errorf("MarshalFailures = %d, want 1 (no double-counting)", stats.MarshalFailures)
	}
}

// TestFilterModeValidation tests filter mode validation
func TestFilterModeValidation(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "test"},
	}
	logger := func(msg string) {}

	// Test valid modes
	_, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Errorf("FilterModeExclude should be valid: %v", err)
	}

	_, err = NewFilterEngine(patterns, FilterModeInclude, logger)
	if err != nil {
		t.Errorf("FilterModeInclude should be valid: %v", err)
	}

	// Test empty mode defaults to exclude
	fe, err := NewFilterEngine(patterns, "", logger)
	if err != nil {
		t.Errorf("Empty mode should default to exclude: %v", err)
	}
	if fe != nil {
		fe.Close()
	}

	// Test invalid mode
	_, err = NewFilterEngine(patterns, "invalid", logger)
	if err == nil {
		t.Error("Invalid mode should return error")
	}
}

// TestFilterModeInclude tests include mode filtering
func TestFilterModeInclude(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "level", Pattern: "^(INFO|ERROR|WARN)$"},
	}

	logger := func(msg string) {
		t.Logf("Log: %s", msg)
	}

	fe, err := NewFilterEngine(patterns, FilterModeInclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	tests := []struct {
		name         string
		message      *protocol.DataMessage
		shouldFilter bool // true = filtered out, false = allowed through
	}{
		{
			name: "matching pattern - allowed through",
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "INFO",
					"message": "test",
				},
			},
			shouldFilter: false, // matches, so NOT filtered in include mode
		},
		{
			name: "non-matching pattern - filtered out",
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "DEBUG",
					"message": "test",
				},
			},
			shouldFilter: true, // doesn't match, so filtered in include mode
		},
		{
			name: "another matching pattern - allowed through",
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "ERROR",
					"message": "error occurred",
				},
			},
			shouldFilter: false, // matches, so NOT filtered in include mode
		},
		{
			name: "text payload no match - filtered out",
			message: &protocol.DataMessage{
				TextPayload: "some random text",
			},
			shouldFilter: true, // doesn't match, so filtered in include mode
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldFilter, _ := fe.ShouldFilter(tt.message)
			if shouldFilter != tt.shouldFilter {
				t.Errorf("ShouldFilter() = %v, want %v", shouldFilter, tt.shouldFilter)
			}
		})
	}
}

// TestFilterModeExclude tests exclude mode filtering (default behavior)
func TestFilterModeExclude(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
	}

	logger := func(msg string) {
		t.Logf("Log: %s", msg)
	}

	fe, err := NewFilterEngine(patterns, FilterModeExclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	tests := []struct {
		name         string
		message      *protocol.DataMessage
		shouldFilter bool // true = filtered out, false = allowed through
	}{
		{
			name: "matching pattern - filtered out",
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "DEBUG",
					"message": "test",
				},
			},
			shouldFilter: true, // matches, so filtered in exclude mode
		},
		{
			name: "non-matching pattern - allowed through",
			message: &protocol.DataMessage{
				JsonPayload: map[string]interface{}{
					"level":   "INFO",
					"message": "test",
				},
			},
			shouldFilter: false, // doesn't match, so NOT filtered in exclude mode
		},
		{
			name: "text payload no match - allowed through",
			message: &protocol.DataMessage{
				TextPayload: "some random text",
			},
			shouldFilter: false, // doesn't match, so NOT filtered in exclude mode
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldFilter, _ := fe.ShouldFilter(tt.message)
			if shouldFilter != tt.shouldFilter {
				t.Errorf("ShouldFilter() = %v, want %v", shouldFilter, tt.shouldFilter)
			}
		})
	}
}

// TestFilterModeIncludeStats tests statistics in include mode
func TestFilterModeIncludeStats(t *testing.T) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "level", Pattern: "^(INFO|ERROR)$"},
	}

	logger := func(msg string) {}

	fe, err := NewFilterEngine(patterns, FilterModeInclude, logger)
	if err != nil {
		t.Fatalf("Failed to create filter engine: %v", err)
	}
	defer fe.Close()

	messages := []*protocol.DataMessage{
		{JsonPayload: map[string]interface{}{"level": "INFO"}},  // matches - not filtered
		{JsonPayload: map[string]interface{}{"level": "DEBUG"}}, // no match - filtered
		{JsonPayload: map[string]interface{}{"level": "ERROR"}}, // matches - not filtered
		{JsonPayload: map[string]interface{}{"level": "TRACE"}}, // no match - filtered
		{JsonPayload: map[string]interface{}{"level": "INFO"}},  // matches - not filtered
	}

	for _, msg := range messages {
		fe.ShouldFilter(msg)
	}

	stats := fe.GetStats()

	if stats.TotalChecked != 5 {
		t.Errorf("TotalChecked = %d, want 5", stats.TotalChecked)
	}

	if stats.TotalFiltered != 2 {
		t.Errorf("TotalFiltered = %d, want 2 (DEBUG and TRACE filtered in include mode)", stats.TotalFiltered)
	}

	// Pattern should have 3 matches (INFO, ERROR, INFO)
	if stats.PerPattern[0].Matches != 3 {
		t.Errorf("Pattern matches = %d, want 3", stats.PerPattern[0].Matches)
	}
}

// Benchmarks

func BenchmarkFilterEngineRegex(b *testing.B) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "health-?check"},
	}

	logger := func(msg string) {}

	fe, _ := NewFilterEngine(patterns, FilterModeExclude, logger)
	defer fe.Close()

	msg := &protocol.DataMessage{
		TextPayload: "GET /api/users HTTP/1.1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}

func BenchmarkFilterEngineGJSON(b *testing.B) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
	}

	logger := func(msg string) {}

	fe, _ := NewFilterEngine(patterns, FilterModeExclude, logger)
	defer fe.Close()

	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"level":   "INFO",
			"message": "test message",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}

func BenchmarkFilterEngineGJSONMatch(b *testing.B) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
	}

	logger := func(msg string) {}

	fe, _ := NewFilterEngine(patterns, FilterModeExclude, logger)
	defer fe.Close()

	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"level":   "DEBUG",
			"message": "test message",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}

func BenchmarkFilterEngineMixed(b *testing.B) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: "health-?check"},
		{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
		{Type: "gjson", Path: "user.role", Pattern: "^test-.*"},
	}

	logger := func(msg string) {}

	fe, _ := NewFilterEngine(patterns, FilterModeExclude, logger)
	defer fe.Close()

	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"level":   "INFO",
			"message": "normal message",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}

// BenchmarkFilterEngineRegexOnJSON benchmarks the old approach: regex matching on full JSON payload
func BenchmarkFilterEngineRegexOnJSON(b *testing.B) {
	patterns := []FilterPattern{
		{Type: "regex", Pattern: `"level"\s*:\s*"(DEBUG|TRACE)"`},
	}

	logger := func(msg string) {}

	fe, _ := NewFilterEngine(patterns, FilterModeExclude, logger)
	defer fe.Close()

	// Large JSON payload to simulate real-world scenario
	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"timestamp": "2024-01-15T10:30:00Z",
			"level":     "INFO",
			"message":   "Processing request for user authentication",
			"request_id": "req-12345-abcde-67890",
			"user": map[string]interface{}{
				"id":    "user-9876",
				"name":  "John Doe",
				"email": "john.doe@example.com",
				"roles": []string{"admin", "developer"},
			},
			"metadata": map[string]interface{}{
				"ip":         "192.168.1.100",
				"user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
				"session_id": "sess-xyz-123-abc",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}

// BenchmarkFilterEngineGJSONOnJSON benchmarks the new approach: gjson path extraction + regex
func BenchmarkFilterEngineGJSONOnJSON(b *testing.B) {
	patterns := []FilterPattern{
		{Type: "gjson", Path: "level", Pattern: "^(DEBUG|TRACE)$"},
	}

	logger := func(msg string) {}

	fe, _ := NewFilterEngine(patterns, FilterModeExclude, logger)
	defer fe.Close()

	// Same large JSON payload as regex benchmark
	msg := &protocol.DataMessage{
		JsonPayload: map[string]interface{}{
			"timestamp": "2024-01-15T10:30:00Z",
			"level":     "INFO",
			"message":   "Processing request for user authentication",
			"request_id": "req-12345-abcde-67890",
			"user": map[string]interface{}{
				"id":    "user-9876",
				"name":  "John Doe",
				"email": "john.doe@example.com",
				"roles": []string{"admin", "developer"},
			},
			"metadata": map[string]interface{}{
				"ip":         "192.168.1.100",
				"user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
				"session_id": "sess-xyz-123-abc",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fe.ShouldFilter(msg)
	}
}
