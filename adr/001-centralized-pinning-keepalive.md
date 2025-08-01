# ADR: Zero-Copy String/Blob Data with unsafe.StringData() and unsafe.SliceData()

## Status
Proposed - 2025-07-28
Revised - 2025-07-28 (Added closure-based deferred population pattern)
Revised - 2025-07-29 (Pivoted to zero-copy implementation for string/blob data)

## Context
Production systems processing hundreds of billions to trillions of data points per day face critical performance and safety challenges. While the centralized pinning pattern solved memory safety issues, performance analysis reveals a major inefficiency:

**Current Implementation Performance Issues:**
1. **2x Memory Usage**: All string and blob data is copied from Go memory to C memory using `qdbAllocAndCopyBytes`, doubling memory consumption
2. **O(n) Copy Overhead**: Every string/blob operation incurs full data copy, creating significant CPU overhead for large datasets
3. **Memory Fragmentation**: Frequent allocations and deallocations of large buffers cause heap fragmentation

**Example of Current Inefficiency:**
```go
// Current implementation in ColumnDataBlob.PinToC()
for i, blob := range cd.xs {
    if len(blob) > 0 {
        // INEFFICIENT: Full copy of blob data
        blobPtr := qdbAllocAndCopyBytes(h, blob)  // Allocates new memory and copies
        envelopeSlice[i].content = blobPtr
        envelopeSlice[i].content_length = C.qdb_size_t(len(blob))
    }
}
```

For a 1GB blob dataset, this creates an additional 1GB of C-allocated memory, plus the CPU cycles to copy every byte.

## Decision
We will implement zero-copy string/blob data transfer using Go's officially endorsed `unsafe.StringData()` and `unsafe.SliceData()` functions:

1. **Use unsafe.StringData()** for strings to get a pinnable pointer to the underlying string data
2. **Use unsafe.SliceData()** for []byte blobs to get a pinnable pointer to the underlying slice data
3. **Pin the original Go memory** instead of copying to C-allocated buffers
4. **Maintain the existing PinnableBuilder pattern** for deferred C structure population
5. **Keep runtime.KeepAlive** to ensure garbage collector doesn't collect pinned objects

**Key Design Constraints:**
- The QuasarDB C API is guaranteed to be invoked synchronously and single-threaded
- No backwards compatibility requirements - we can change internal implementations freely
- Focus on highest performance solution while maintaining memory safety

## Consequences
**Benefits:**
- **~50% Memory Reduction**: Eliminates duplicate copies of string/blob data
- **Zero Copy Overhead**: Direct pointer passing removes O(n) copy operations
- **Improved Cache Locality**: Working with original Go memory improves CPU cache utilization
- **Reduced Heap Fragmentation**: Fewer allocations mean less memory fragmentation
- **Maintains Safety**: Still uses proper pinning and KeepAlive patterns

**Tradeoffs:**
- **Increased Complexity**: Must carefully manage pointer arithmetic with unsafe operations
- **Go Version Requirement**: Requires Go 1.20+ for unsafe.StringData/SliceData
- **Testing Burden**: Requires thorough testing with memory sanitizers

## Implementation

### Phase 1: Update ColumnDataBlob.PinToC()
```go
func (cd *ColumnDataBlob) PinToC(h HandleType) (PinnableBuilder, func()) {
    if len(cd.xs) == 0 {
        return PinnableBuilder{}, func() {}
    }
    
    // Allocate C envelope for blob descriptors (not the data itself)
    envelope := qdbAllocBuffer[C.qdb_blob_t](h, len(cd.xs))
    envelopeSlice := unsafe.Slice(envelope, len(cd.xs))
    
    // Build list of objects to pin (the actual blob data)
    pinnableObjects := make([]interface{}, 0, len(cd.xs))
    for i, blob := range cd.xs {
        if len(blob) > 0 {
            pinnableObjects = append(pinnableObjects, &blob[0])
        }
    }
    
    return PinnableBuilder{
        Object: pinnableObjects,  // Pin the blob data itself
        Builder: func() unsafe.Pointer {
            // Populate C structures AFTER pinning
            for i, blob := range cd.xs {
                if len(blob) > 0 {
                    envelopeSlice[i].content = unsafe.Pointer(unsafe.SliceData(blob))
                    envelopeSlice[i].content_length = C.qdb_size_t(len(blob))
                } else {
                    envelopeSlice[i].content = nil
                    envelopeSlice[i].content_length = 0
                }
            }
            return unsafe.Pointer(envelope)
        },
    }, func() {
        qdbReleasePointer(h, unsafe.Pointer(envelope))
    }
}
```

### Phase 2: Update ColumnDataString.PinToC()
```go
func (cd *ColumnDataString) PinToC(h HandleType) (PinnableBuilder, func()) {
    if len(cd.xs) == 0 {
        return PinnableBuilder{}, func() {}
    }
    
    // Allocate C envelope for string descriptors (not the data itself)
    envelope := qdbAllocBuffer[C.qdb_string_t](h, len(cd.xs))
    envelopeSlice := unsafe.Slice(envelope, len(cd.xs))
    
    // No need to collect pinnable objects for strings
    // Strings are immutable and tracked by runtime
    
    return PinnableBuilder{
        Object: cd.xs,  // Pin the string slice itself
        Builder: func() unsafe.Pointer {
            // Populate C structures AFTER pinning
            for i, str := range cd.xs {
                if len(str) > 0 {
                    envelopeSlice[i].data = (*C.char)(unsafe.Pointer(unsafe.StringData(str)))
                    envelopeSlice[i].length = C.qdb_size_t(len(str))
                } else {
                    envelopeSlice[i].data = nil
                    envelopeSlice[i].length = 0
                }
            }
            return unsafe.Pointer(envelope)
        },
    }, func() {
        qdbReleasePointer(h, unsafe.Pointer(envelope))
    }
}
```

### Phase 3: Enable Comprehensive Testing
1. Add benchmark tests comparing old vs new implementation
2. Enable strict memory checking: `GOEXPERIMENT=cgocheck2 GODEBUG=invalidptr=1`
3. Run stress tests with large datasets (GB+ of blob/string data)
4. Verify with race detector: `go test -race`
5. Profile memory usage to confirm 50% reduction

## Technical Rationale

### Why Zero-Copy Is Now Safe and Optimal

The combination of Go 1.20+ features and our existing safety patterns enables zero-copy:

1. **unsafe.StringData() and unsafe.SliceData()**: Official Go functions that return pointers to the underlying data that can be safely pinned
2. **Immutable String Guarantee**: Go strings are immutable, making it safe to pass string data pointers to C
3. **Synchronous API Contract**: QuasarDB C API calls are synchronous and single-threaded, eliminating concurrency concerns
4. **Existing Safety Infrastructure**: Our PinnableBuilder pattern already ensures proper pin-before-use ordering

### Performance Impact Analysis

**Current Copy-Based Approach:**
```go
// Every string/blob operation allocates and copies
blobPtr := qdbAllocAndCopyBytes(h, blob)  // O(n) copy + allocation

// For 1GB dataset:
// - 1GB original Go memory
// - 1GB copied C memory
// - Total: 2GB memory usage
// - Plus: CPU cycles for copying 1GB
```

**New Zero-Copy Approach:**
```go
// Direct pointer to existing Go memory
envelopeSlice[i].content = unsafe.Pointer(unsafe.SliceData(blob))  // O(1) pointer assignment

// For 1GB dataset:
// - 1GB original Go memory
// - Small envelope structs (~16 bytes per blob)
// - Total: ~1GB memory usage
// - Zero copy overhead
```

### Memory Safety with Zero-Copy

The zero-copy approach maintains all existing safety guarantees:

1. **Pin-Before-Use**: The PinnableBuilder pattern ensures pointers are pinned before being stored in C structures
2. **KeepAlive Protection**: Original data structures remain alive throughout C execution
3. **No String Mutation**: Go's string immutability guarantee prevents any C code from modifying string data
4. **Synchronous Execution**: Single-threaded C API calls eliminate race conditions

### Critical Safety Validation

```go
// The 5-phase pattern still applies with zero-copy:
func (w *Writer) Push(h HandleType) error {
    var pinner runtime.Pinner
    defer pinner.Unpin()
    
    // Phase 1: Collect builders (including string/blob data)
    // Phase 2: Pin all objects (now pins original Go memory)
    // Phase 3: Execute builders (populates with unsafe.StringData/SliceData)
    // Phase 4: Call C API
    // Phase 5: KeepAlive ensures data survives
}
```

### Why This Approach Is Superior

1. **Performance Critical**: For systems processing trillions of data points, eliminating copy overhead is essential
2. **Memory Efficient**: 50% reduction in memory usage allows processing larger datasets
3. **Cache Friendly**: Working with original Go memory improves CPU cache hit rates
4. **Officially Endorsed**: unsafe.StringData/SliceData are the Go team's recommended approach for zero-copy CGO

### Validation Requirements

All implementations must pass:
```bash
# Memory safety validation
GOEXPERIMENT=cgocheck2 GODEBUG=invalidptr=1 go test ./...

# Race condition detection
go test -race ./...

# Performance benchmarks showing 50% memory reduction
go test -bench=BenchmarkStringBlob -benchmem

# Static analysis
go vet -cgo ./...
```

## References
- Go 1.20 Release Notes: Introduction of unsafe.StringData and unsafe.SliceData
- Go runtime documentation on Pinner and KeepAlive
- CGO pointer passing rules (Go 1.23+) with pinning requirements
- QuasarDB C API synchronous execution guarantees
- Go issue #40701: unsafe.StringData/SliceData design rationale
- Go wiki: CGO pointer passing rules and zero-copy patterns