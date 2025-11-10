# Filter Engine Performance Benchmarks

## Overview

This document summarizes the performance characteristics of the client-side filtering engine implemented in `utils/filter.go`.

## Benchmark Results

```
BenchmarkFilterEngineRegex-12          	44515892	        28.85 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilterEngineGJSON-12          	 1000000	      1648 ns/op	     480 B/op	      14 allocs/op
BenchmarkFilterEngineGJSONMatch-12     	  735517	      1758 ns/op	     417 B/op	      12 allocs/op
BenchmarkFilterEngineMixed-12          	  664321	      1774 ns/op	     490 B/op	      14 allocs/op
BenchmarkFilterEngineRegexOnJSON-12    	  228279	      5842 ns/op	    1817 B/op	      31 allocs/op
BenchmarkFilterEngineGJSONOnJSON-12    	  164175	      9466 ns/op	    3555 B/op	      62 allocs/op
```

## Performance Comparison

### Regex on TextPayload
- **Speed**: 28.85 ns/op
- **Memory**: 0 B/op, 0 allocs/op
- **Use case**: Filtering plain text log messages
- **Notes**: Fastest option - no JSON marshaling overhead

### GJSON on JsonPayload  
- **Speed**: 1648-1758 ns/op (~57x slower than regex on text)
- **Memory**: 417-480 B/op, 12-14 allocs/op
- **Use case**: Filtering structured JSON by specific fields
- **Notes**: Overhead from JSON marshaling, but allows field-specific queries

### Regex vs GJSON on JSON Data

**Regex on full JSON**: 5842 ns/op, 1817 B/op, 31 allocs/op
**GJSON on specific field**: 9466 ns/op, 3555 B/op, 62 allocs/op

**Result**: GJSON is ~1.6x slower and uses ~2x more memory than regex on full JSON.

## Why Choose GJSON Despite Lower Raw Performance?

While GJSON is not faster than regex, it provides significant **usability and maintainability** benefits:

### 1. **Expressiveness**
```yaml
# GJSON: Clear and readable
filters:
  - type: gjson
    path: "level"
    pattern: "^(DEBUG|TRACE)$"

# Regex: Fragile and hard to maintain
filters:
  - type: regex
    pattern: "\"level\"\\s*:\\s*\"(DEBUG|TRACE)\""
```

### 2. **Safety**
- No need to escape JSON syntax characters
- Immune to whitespace variations in JSON formatting
- Handles nested fields naturally: `user.profile.role`

### 3. **Advanced Queries**
GJSON supports queries impossible with regex:
```yaml
# Extract email from first user over 45
- type: gjson
  path: "users.#(age>45).email"
  pattern: ".*@company\\.com"

# Get value from nested array
- type: gjson
  path: "events.0.metadata.severity"
  pattern: "^(high|critical)$"
```

### 4. **Performance in Context**

At 9.4 microseconds per operation, GJSON filtering is still extremely fast:
- Typical adapter polling: every 30 seconds
- Filtering 1,000 events: ~9.4ms total
- Network/API latency: 100-1000ms typical
- **Filtering overhead**: <1% of total request time

## Recommendations

1. **Use GJSON for structured JSON filtering**: The usability benefit far outweighs the 1.6x performance cost
2. **Use regex for text-based filtering**: When filtering plain text logs or when you need full-payload regex matching
3. **Mix both types**: You can combine gjson and regex patterns in the same configuration

## Example Configuration

```yaml
filters:
  # Fast field-based filtering with GJSON
  - type: gjson
    path: "level"
    pattern: "^(DEBUG|TRACE)$"
  
  # Nested field filtering
  - type: gjson
    path: "user.role"
    pattern: "^test-.*"
  
  # Full payload regex (for text or complex patterns)
  - type: regex
    pattern: "health-?check"
```

## Testing

Run benchmarks:
```bash
go test -bench=BenchmarkFilterEngine -benchmem ./utils
```

Run with race detector:
```bash
go test -race ./utils
```
