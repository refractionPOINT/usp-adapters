# Client-Side Filtering Implementation Summary

## Overview

This document summarizes the implementation of the client-side filtering feature for USP adapters, including all bug fixes, enhancements, and design decisions.

## Features Implemented

### 1. Type-Safe Filter Configuration

**Before**: String-based patterns (regex only)
```go
Filters []string `json:"filters,omitempty"`
```

**After**: Type-discriminated pattern objects supporting both regex and gjson
```go
type FilterPattern struct {
    Type    string `json:"type" yaml:"type"`        // "regex" or "gjson"
    Pattern string `json:"pattern" yaml:"pattern"`   // Regex pattern to match
    Path    string `json:"path,omitempty" yaml:"path,omitempty"` // gjson path (for type="gjson")
}

Filters []utils.FilterPattern `json:"filters,omitempty"`
```

### 2. GJSON-Based JSON Field Filtering

Allows efficient filtering on specific JSON fields without writing complex regex patterns:

```yaml
filters:
  # Extract and match specific field
  - type: gjson
    path: "level"
    pattern: "^(DEBUG|TRACE)$"

  # Nested field access
  - type: gjson
    path: "user.profile.role"
    pattern: "^admin$"

  # Array queries
  - type: gjson
    path: "events.0.severity"
    pattern: "^(high|critical)$"

  # Conditional queries
  - type: gjson
    path: "users.#(age>45).email"
    pattern: ".*@company\\.com"
```

See [GJSON Path Syntax](https://github.com/tidwall/gjson/blob/master/SYNTAX.md) for complete documentation.

### 3. Pattern Validation

All patterns are validated at initialization:
- Type must be "regex" or "gjson"
- Pattern cannot be empty or whitespace-only
- Pattern must be valid regex syntax
- Path is required for gjson type

```go
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

    if _, err := regexp.Compile(fp.Pattern); err != nil {
        return fmt.Errorf("invalid regex pattern: %w", err)
    }

    return nil
}
```

### 4. Per-Pattern Statistics

Track filtering statistics for each pattern individually:

```go
type FilterStats struct {
    TotalChecked     uint64
    TotalFiltered    uint64
    MarshalFailures  uint64
    PerPattern       []PatternStats
    LastReportTime   time.Time
}

type PatternStats struct {
    Pattern string
    Matches uint64
}
```

Example log output:
```
Filter stats (last 5m0s): checked=10000, filtered=250 (2.50%)
  - Pattern 0 [gjson] path="level", pattern="DEBUG": 150 matches
  - Pattern 1 [regex] pattern="health-?check": 100 matches
```

### 5. Marshal Failure Tracking

Tracks and reports JSON marshaling failures separately:

```
Filter stats: checked=10000, filtered=250 (2.50%)
  - JSON marshal failures: 5
  - Pattern 0 [gjson] path="level", pattern="DEBUG": 150 matches
```

## Bug Fixes

### 1. Double-Close Panic (Critical)
**Problem**: `FilterEngine.Close()` could be called multiple times causing panic

**Fix**: Implemented `sync.Once` for idempotent close:
```go
type FilterEngine struct {
    closeOnce sync.Once
    // ...
}

func (fe *FilterEngine) Close() {
    fe.closeOnce.Do(func() {
        close(fe.stopReporting)
        fe.wg.Wait()
        fe.logStats(true)
    })
}
```

### 2. Race Condition on lastReportTime (Critical)
**Problem**: Concurrent read/write of `lastReportTime` field

**Fix**: Protected with `RWMutex`:
```go
func (fe *FilterEngine) GetStats() FilterStats {
    fe.mutex.RLock()
    lastReport := fe.lastReportTime
    fe.mutex.RUnlock()

    stats := FilterStats{
        LastReportTime: lastReport,
        // ...
    }
    return stats
}
```

### 3. Empty Pattern Validation (Critical)
**Problem**: Empty or whitespace-only patterns would match everything

**Fix**: Added trim check in validation:
```go
if strings.TrimSpace(fp.Pattern) == "" {
    return fmt.Errorf("pattern cannot be empty or whitespace-only")
}
```

## Architecture

### Two-Stage Filtering

The `ShouldFilter` method uses a two-stage approach:

1. **Fast path**: Check gjson patterns first (when JsonPayload exists)
   - Marshal JSON once for all gjson queries
   - Use gjson to extract specific fields
   - Apply regex to extracted values
   - Early return on first match

2. **Slow path**: Check regex patterns on full payload
   - Extract full payload (TextPayload or marshaled JsonPayload)
   - Apply regex patterns sequentially
   - Return on first match

### Thread Safety

All shared state uses atomic operations or mutexes:
- `atomic.AddUint64()` for counters (totalChecked, totalFiltered, per-pattern stats)
- `sync.RWMutex` for lastReportTime
- `sync.Once` for Close() idempotence
- `sync.WaitGroup` for goroutine lifecycle

### Background Statistics Reporter

Automatically logs statistics every 5 minutes:
- Runs in background goroutine
- Uses ticker for periodic reporting
- Clean shutdown via stopReporting channel
- Final stats logged on Close()

## Performance Characteristics

See [FILTERING_BENCHMARKS.md](./FILTERING_BENCHMARKS.md) for detailed analysis.

**Summary**:
- Regex on text: 28.85 ns/op (fastest)
- GJSON on JSON: 9466 ns/op (~1.6x slower than regex on JSON)
- Performance overhead is negligible in context of 30-second polling intervals
- Usability and maintainability benefits justify the performance trade-off

## Testing

### Test Coverage
- FilterPattern validation (empty, invalid regex, missing fields)
- Regex pattern matching (text and JSON payloads)
- GJSON basic patterns (simple fields, nested fields, array access)
- GJSON query patterns (conditionals, modifiers)
- Mixed patterns (both regex and gjson)
- Statistics tracking (per-pattern, totals, percentages)
- Concurrency (50 goroutines × 100 messages)
- Double-close safety (idempotence)
- Marshal failure handling

### Running Tests

```bash
# Run all tests
go test ./utils

# Run with race detector
go test -race ./utils

# Run benchmarks
go test -bench=BenchmarkFilterEngine -benchmem ./utils

# Run specific test
go test -run TestFilterEngineGJSONBasic ./utils
```

## Updated Adapters

All 40 adapters were updated with the new `FilterPattern` type:

1password, azure_event_hub, bigquery, bitwarden, box, cato, cylance, defender, duo, entraid, evtx, falconcloud, file, gcs, hubspot, imap, itglue, k8s_pods, mac_unified_logging, mimecast, ms_graph, o365, okta, pandadoc, proofpoint_tap, pubsub, s3, sentinelone, simulator, slack, sophos, sqs, sqs-files, stdin, sublime, syslog, trendmicro, wel, wiz, zendesk

## Configuration Examples

### Simple Field Filtering
```yaml
client_options:
  # ... (standard options)

filters:
  - type: gjson
    path: "level"
    pattern: "^(DEBUG|TRACE)$"
```

### Multiple Patterns
```yaml
filters:
  # Filter debug logs
  - type: gjson
    path: "level"
    pattern: "^DEBUG$"

  # Filter test users
  - type: gjson
    path: "user.email"
    pattern: ".*@test\\.example\\.com$"

  # Filter health checks (full text)
  - type: regex
    pattern: "health-?check"
```

### Advanced GJSON Queries
```yaml
filters:
  # Filter users over 45
  - type: gjson
    path: "users.#(age>45).name"
    pattern: ".*"

  # Filter first array element
  - type: gjson
    path: "items.0.id"
    pattern: "^test-.*"

  # Filter with array length
  - type: gjson
    path: "tags.#"
    pattern: "^[0-2]$"  # 0-2 tags only
```

## Design Decisions

### Why Unified Array vs Separate Fields?
**Decision**: Use single `filters: []FilterPattern` array with type discriminator

**Alternatives considered**:
1. ❌ Separate fields (`regex_patterns`, `gjson_patterns`)
2. ❌ String prefix (`"gjson:path:pattern"`)
3. ✅ Unified array with type field

**Rationale**:
- Type-safe and explicit
- Easy to validate
- Natural pattern ordering
- Clean YAML/JSON syntax

### Why Regex on Extracted Value vs Boolean Match?
**Decision**: Extract field with gjson, then apply regex to the value

**Alternatives considered**:
1. ❌ Boolean match only (`path: "level" matches "DEBUG"`)
2. ✅ Regex on extracted value

**Rationale**:
- Flexible pattern matching (OR, anchors, character classes)
- Consistent with regex patterns
- No need for separate boolean match syntax

### Why No Backward Compatibility?
**Decision**: Breaking change, no migration path

**Rationale**: Feature was never deployed to production, so clean slate allowed

## Dependencies

Added dependency:
```go
github.com/tidwall/gjson v1.18.0
```

## Files Modified

### Core Implementation
- `utils/filter.go` - Complete rewrite with gjson support
- `utils/filtered_client.go` - Updated function signature
- `utils/filter_test.go` - Comprehensive test suite (678 lines)

### Documentation
- `utils/FILTERING_BENCHMARKS.md` - Performance analysis
- `utils/IMPLEMENTATION_SUMMARY.md` - This document

### Adapters (40 files)
All adapter config structs updated from `[]string` to `[]utils.FilterPattern`

## Verification

```bash
# All tests pass
$ go test ./utils
ok  	github.com/refractionPOINT/usp-adapters/utils	7.645s

# No race conditions
$ go test -race ./utils
ok  	github.com/refractionPOINT/usp-adapters/utils	7.645s

# Dependencies resolved
$ go mod tidy
$ go build ./...
# (no errors)
```

## Next Steps

1. Update PR description with implementation details
2. Request code review focusing on:
   - Thread safety verification
   - GJSON pattern examples
   - Documentation clarity
3. Consider adding example configurations to adapter README files
4. Consider adding integration tests with real adapter data
