# ADR: Conversion Strategy for C Types in Column Data

## Status
Revised - 2025-07-31

## Context
The qdb-api-go library implements column data types that must be passed to C APIs. After extensive testing and debugging a critical memory safety bug in timestamp handling, we discovered that the optimal conversion strategy depends on the data type characteristics. The bug manifested when deferring conversion of timestamps created a temporary slice that went out of scope before the C call, violating CGO memory safety rules.

## Decision
We will use different conversion strategies based on data type characteristics:

1. **Simple value types (int64, float64, timestamp)**: Store in C-compatible format to enable zero-copy pinning
2. **Complex types (string, blob)**: Store in Go format and defer conversion due to individual allocation requirements

## Consequences
**Benefits:**
- Maximum performance through zero-copy for numeric types
- Memory safety by avoiding temporary allocations
- Clear architectural rules based on type characteristics
- Simplified pinning strategy for value types

**Tradeoffs:**
- Different patterns for different types (justified by their inherent differences)
- Timestamps stored as C types internally (hidden from API users)
- Slightly more complex mental model (but clearer rationale)

## Implementation

### Simple Value Types Pattern
For int64, float64, and timestamps - types that have direct C equivalents:

```
GO SLICE                C-COMPATIBLE SLICE           C API
[][][][][]    =====>    [][][][][]      --pin-->    C function
                        (same memory)
```

```go
// Store directly in C-compatible format
type ColumnDataTimestamp struct{ xs []C.qdb_timespec_t }

func NewColumnDataTimestamp(ts []time.Time) ColumnDataTimestamp {
    // Convert early to C format
    return ColumnDataTimestamp{xs: TimeSliceToQdbTimespec(ts)}
}

func (cd *ColumnDataTimestamp) PinToC(h HandleType) (PinnableBuilder, func()) {
    // Zero-copy: pin the entire slice
    base := &cd.xs[0]
    return NewPinnableBuilderSingle(base, func() unsafe.Pointer {
        return unsafe.Pointer(base)
    }), func() {}
}
```

### Complex Types Pattern
For strings and blobs - types requiring individual allocations:

```
GO SLICE               PINNING PHASE              C ENVELOPES
["str1"]                                         [C.envelope{ptr, len}]
["str2"]    --pin-->   Pin each string  ---->    [C.envelope{ptr, len}]
["str3"]               individually              [C.envelope{ptr, len}]
```

```go
// Store in Go format
type ColumnDataString struct{ xs []string }

func NewColumnDataString(xs []string) ColumnDataString {
    return ColumnDataString{xs: xs}
}

func (cd *ColumnDataString) PinToC(h HandleType) (PinnableBuilder, func()) {
    // Must defer: each string needs individual pinning
    objects := make([]interface{}, 0, len(cd.xs))
    for _, str := range cd.xs {
        if len(str) > 0 {
            objects = append(objects, unsafe.StringData(str))
        }
    }
    // ... create C envelopes after pinning ...
}
```

## Technical Rationale

### Why Different Strategies?

The fundamental difference lies in memory layout and CGO requirements:

1. **Simple Value Types**:
   - Have identical memory representation in Go and C
   - Can be passed as contiguous arrays
   - Single pin operation covers entire slice
   - Zero-copy provides significant performance benefits

2. **Complex Types**:
   - Require individual memory management (each string is separate)
   - Need C envelope structures with pointers and lengths
   - Must pin each element individually
   - Deferred conversion avoids premature pointer creation

### The Memory Safety Bug

When we tried to defer timestamp conversion:
```go
// BROKEN: Creates temporary slice
func (cd *ColumnDataTimestamp) PinToC(h HandleType) (PinnableBuilder, func()) {
    cSpecs := TimeSliceToQdbTimespec(cd.xs)  // Local variable!
    // cSpecs goes out of scope when function returns
    // but we return a pointer into it - VIOLATION!
}
```

The fix is to store timestamps in C format from the start, ensuring the memory remains valid.

### Performance Impact

Benchmarks show the differentiated approach provides optimal performance:
- Simple types: Zero allocations, zero copies during Push
- Complex types: Zero-copy for the actual data (only envelope allocation)
- Batch operations: Amortized cost across thousands of values

## API Design

Despite different internal strategies, the public API remains consistent:
```go
// Users always work with Go types
timestamps := []time.Time{time.Now(), time.Now().Add(time.Hour)}
col := NewColumnDataTimestamp(timestamps)

// Retrieval returns Go types
data, _ := GetColumnDataTimestamp(col)  // []time.Time, not C types
```

## Guidelines for New Types

When adding new column types, choose the strategy based on:

**Use C-Compatible Storage When:**
- Type has direct C equivalent (same memory layout)
- Can be represented as contiguous array
- No internal pointers or variable-length data
- Examples: int32, uint64, fixed-size structs

**Use Deferred Conversion When:**
- Type requires individual allocations
- Has variable-length data
- Contains Go pointers or interfaces
- Examples: []byte slices, custom structs with strings

## References
- CGO pointer passing rules: golang.org/cmd/cgo/#hdr-Passing_pointers
- ADR-001: Centralized pinning architecture
- Issue: Timestamp memory safety bug (2025-07-30)
- Benchmarks: writer_benchmark_test.go showing performance characteristics
