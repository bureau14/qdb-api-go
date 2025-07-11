package qdb

/*
	#include <stdlib.h>
        #include <string.h> // for memcpy
	#include <qdb/client.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

// All available columns we support
var columnTypes = [...]TsColumnType{TsColumnInt64, TsColumnDouble, TsColumnTimestamp, TsColumnBlob, TsColumnString}

// convertToCharStarStar converts []string to C char**
// In: toConvert []string - strings to convert
// Out: unsafe.Pointer - C char** array
// Ex: convertToCharStarStar([]string{"a","b"}) → char**
func convertToCharStarStar(toConvert []string) unsafe.Pointer {
	var v *C.char
	ptrSize := unsafe.Sizeof(v)
	size := len(toConvert)
	data := C.malloc(C.size_t(size) * C.size_t(ptrSize))
	for i := 0; i < size; i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(data) + uintptr(i)*ptrSize))
		*element = (*C.char)(convertToCharStar(toConvert[i]))
	}
	return data
}

// releaseCharStarStar frees C char** array
// In: data unsafe.Pointer - char** to free
//
//	size int - array length
//
// Ex: releaseCharStarStar(ptr, 2)
func releaseCharStarStar(data unsafe.Pointer, size int) {
	var v *C.char
	ptrSize := unsafe.Sizeof(v)
	for i := 0; i < size; i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(data) + uintptr(i)*ptrSize))
		releaseCharStar(*element)
	}
	C.free(data)
}

// convertToCharStar converts Go string to C char*
// In: toConvert string - string to convert
// Out: *C.char - C string or nil
// Ex: convertToCharStar("hello") → *C.char
func convertToCharStar(toConvert string) *C.char {
	if len(toConvert) == 0 {
		return nil
	}
	return C.CString(toConvert)
}

// releaseCharStar frees C string
// In: data *C.char - string to free
// Ex: releaseCharStar(cStr)
func releaseCharStar(data *C.char) {
	if data != nil {
		C.free(unsafe.Pointer(data))
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// generateAlias creates random string ID
// In: n int - length
// Out: string - random alias
// Ex: generateAlias(16) → "aBcDeFgHiJkLmNoP"
func generateAlias(n int) string {
	b := make([]byte, n)

	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// generateDefaultAlias creates 16-char alias
// Out: string - 16-char random ID
// Ex: generateDefaultAlias() → "aBcDeFgHiJkLmNoP"
func generateDefaultAlias() string {
	return generateAlias(16)
}

// generateColumnName creates random column name
// Out: string - 16-char name
// Ex: generateColumnName() → "tempColABCDEFGH"
func generateColumnName() string {
	return generateAlias(16)
}

// randomColumnType picks random column type
// Out: TsColumnType - random type
// Ex: randomColumnType() → TsColumnDouble
func randomColumnType() TsColumnType {
	n := rand.Intn(len(columnTypes))

	return columnTypes[n]
}

// generateColumnNames creates n column names
// In: n int - count
// Out: []string - unique names
// Ex: generateColumnNames(3) → ["col1","col2","col3"]
func generateColumnNames(n int) []string {
	var ret []string = make([]string, n)

	for i := range ret {
		ret[i] = generateColumnName()
	}

	return ret
}

// Generate writer column info for exactly `n` columns.
func generateWriterColumns(n int) []WriterColumn {
	var ret []WriterColumn = make([]WriterColumn, n)

	for i := range ret {
		cname := generateColumnName()
		ctype := randomColumnType()
		ret[i] = WriterColumn{cname, ctype}
	}

	return ret
}

func generateWriterColumnsOfAllTypes() []WriterColumn {
	// Generate column information for each available column type.
	var ret []WriterColumn = make([]WriterColumn, len(columnTypes))

	for i, ctype := range columnTypes {
		cname := generateColumnName()
		ret[i] = WriterColumn{cname, ctype}
	}

	return ret
}

// Similar to `generateWriterColumns`, but ensures all columns are of the specified
// type.
func generateWriterColumnsOfType(n int, ctype TsColumnType) []WriterColumn {
	// Lazy approach: just generate using random column types, then overwrite
	ret := generateWriterColumns(n)

	for i := range ret {
		ret[i].ColumnType = ctype
	}

	return ret
}

// Takes an array of WriterColumns and converts it to TsColumnInfo, which can then be
// used to e.g. create a table.
func convertWriterColumnsToColumnInfo(xs []WriterColumn) []TsColumnInfo {
	ret := make([]TsColumnInfo, len(xs))

	for i := range xs {
		if xs[i].ColumnType == TsColumnSymbol {
			// We just generate a random alias for the symbol table name
			ret[i] = NewSymbolColumnInfo(xs[i].ColumnName, generateDefaultAlias())
		} else {
			ret[i] = NewTsColumnInfo(xs[i].ColumnName, xs[i].ColumnType)
		}
	}

	return ret
}

func generateColumnInfosOfType(n int, ctype TsColumnType) []TsColumnInfo {
	return convertWriterColumnsToColumnInfo(generateWriterColumnsOfType(n, ctype))
}

func generateColumnInfosOfAllTypes() []TsColumnInfo {
	return convertWriterColumnsToColumnInfo(generateWriterColumnsOfAllTypes())
}

func generateColumnInfos(n int) []TsColumnInfo {
	return convertWriterColumnsToColumnInfo(generateWriterColumns(n))
}

func createTableOfColumnInfos(handle HandleType, columnInfos []TsColumnInfo, shardSize time.Duration) (TimeseriesEntry, error) {
	tableName := generateDefaultAlias()

	table := handle.Table(tableName)
	err := table.Create(shardSize, columnInfos...)
	if err != nil {
		return TimeseriesEntry{}, err
	}

	return table, nil
}

func createTableOfColumnInfosAndDefaultShardSize(handle HandleType, columns []TsColumnInfo) (TimeseriesEntry, error) {
	var duration time.Duration = 86400 * 1000 * 1000 * 1000 // 1 day
	return createTableOfColumnInfos(handle, columns, duration)
}

// Takes writer columns and a creates a table that matches the format. Returns the table
// object that was created.
func createTableOfWriterColumns(handle HandleType, columns []WriterColumn, shardSize time.Duration) (TimeseriesEntry, error) {
	columnInfos := convertWriterColumnsToColumnInfo(columns)

	return createTableOfColumnInfos(handle, columnInfos, shardSize)
}

func createTableOfWriterColumnsAndDefaultShardSize(handle HandleType, columns []WriterColumn) (TimeseriesEntry, error) {
	var duration time.Duration = 86400 * 1000 * 1000 * 1000 // 1 day
	return createTableOfWriterColumns(handle, columns, duration)
}

// Generates artifical writer data for a single column
func generateWriterData(n int, column WriterColumn) (ColumnData, error) {
	switch column.ColumnType {
	case TsColumnBlob:
		cdBlob := NewColumnDataBlob(make([][]byte, n))
		return &cdBlob, nil
	case TsColumnSymbol:
		fallthrough
	case TsColumnString:
		cdStr := NewColumnDataString(make([]string, n))
		return &cdStr, nil
	case TsColumnInt64:
		cdInt := NewColumnDataInt64(make([]int64, n))
		return &cdInt, nil
	case TsColumnDouble:
		cdDbl := NewColumnDataDouble(make([]float64, n))
		return &cdDbl, nil
	case TsColumnTimestamp:
		cdTs := NewColumnDataTimestamp(make([]time.Time, n))
		return &cdTs, nil
	}

	return nil, fmt.Errorf("Unrecognized column type: %v", column.ColumnType)
}

// Generates artificial data to be inserted for each column.
func generateWriterDatas(n int, columns []WriterColumn) ([]ColumnData, error) {
	ret := make([]ColumnData, len(columns))

	for i, column := range columns {
		cd, err := generateWriterData(n, column)
		if err != nil {
			return nil, fmt.Errorf("generateWriterData failed for column %d: %w", i, err)
		}
		ret[i] = cd
	}

	return ret, nil
}

// Generates an time index
func generateIndex(n int, start time.Time, step time.Duration) []time.Time {
	var ret []time.Time = make([]time.Time, n)

	for i := range ret {
		nsec := step.Nanoseconds() * int64(i)
		ret[i] = start.Add(time.Duration(nsec))
	}

	return ret
}

// Generates an index with a default start date and step
func generateDefaultIndex(n int) []time.Time {
	var start time.Time = time.Unix(1745514000, 0).UTC() // 2025-04-25
	var duration time.Duration = 100 * 1000 * 1000       // 100ms

	return generateIndex(n, start, duration)
}

func charStarArrayToSlice(strings **C.char, length int) []*C.char {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof((*C.char)(nil))]*C.char)(unsafe.Pointer(strings))[:length:length]
}

// qdbAllocBytes allocates a raw byte buffer of the specified size via the QDB C API.
// Caller must explicitly call qdbReleasePointer() (or qdbRelease[T]()) to free the memory.
//
// Key decisions and trade-offs:
//   - Direct C allocation avoids Go heap overhead when QDB needs to manage memory itself.
//   - Returning unsafe.Pointer ensures generic usage for any byte-based buffer.
//   - Minimal runtime checks: only verifies non-nil pointer after allocation.
//
// Assumptions:
//   - HandleType h is valid and initialized.
//   - totalBytes > 0; otherwise QDB C API may return nil or error.
//
// Performance implications:
//   - Zero Go allocations beyond the pointer itself; allocation happens in C runtime.
//   - Caller must pay the cost of a C allocation and later free via qdbReleasePointer.
//
// Usage example:
//
//	// Allocate buffer for 1024 bytes:
//	rawPtr, err := qdbAllocBytes(h, 1024)
//	if err != nil {
//	    panic(err) // allocation failure is unrecoverable in this context
//	}
//
//	// ... Work with `rawptr`, then free underlying buffer:
//
//	qdbReleasePointer(h, rawPtr)
func qdbAllocBytes(h HandleType, totalBytes int) (unsafe.Pointer, error) {
	var basePtr unsafe.Pointer
	errCode := C.qdb_alloc_buffer(h.handle, C.qdb_size_t(totalBytes), &basePtr)
	err := makeErrorOrNil(errCode)
	if err != nil {
		return nil, err
	}

	if basePtr == nil {
		return nil, fmt.Errorf("qdbAllocBytes: returned nil pointer")
	}

	return basePtr, nil
}

// qdbAllocBuffer allocates a typed buffer of `count` elements via qdbAllocBytes.
// Calculates total size as sizeof(T) * count, then returns *T pointer to the C-allocated memory.
//
// Key decisions and trade-offs:
//   - Abstracts qdbAllocBytes for any element type T, removing boilerplate size computation.
//   - Returns *T so caller can index as a Go slice only after safe conversion (e.g., via castSlice).
//
// Assumptions:
//   - count > 0; otherwise totalSize is zero or negative, leading to allocation failure.
//   - T has no padding or alignment differences beyond what C expects.
//
// Performance implications:
//   - One C allocation of totalSize bytes; zero Go allocations except pointer itself.
//   - Caller can reinterpret returned *T as slice header via unsafe.Slice, avoiding extra copy.
//
// Usage example:
//
//	var data C.qdb_struct_t
//	ver err error
//
//	data.ptr_count = 100
//	data.ptr, err = qdbAllocBuffer[C.qdb_int_t](h, 100)
//	if err != nil {
//	    panic(err) // cannot proceed without buffer
//	}
//
//	// ... Work with `data`, then free underlying buffer:
//
//	qdbRelease(h, ptr)
func qdbAllocBuffer[T any](h HandleType, count int) (*T, error) {
	totalSize := int(unsafe.Sizeof(*new(T))) * count
	ptr, err := qdbAllocBytes(h, totalSize)
	if err != nil {
		return nil, err
	}

	return (*T)(ptr), nil
}

// qdbAllocAndCopyBytes allocates a buffer via C.qdb_copy_alloc_buffer and copies the provided Go slice into it.
// Returns an arbitrary unsafe.Pointer to C-managed memory. Caller must free via qdbReleasePointer.
//
// Key decisions and trade-offs:
//   - Uses QDB’s optimized copy-and-allocate routine when copying from Go memory into C.
//   - Reduces overhead of separate allocation + manual copy; C API may use optimized memcpy.
//
// Assumptions:
//   - src slice length > 0; zero-length slices are rejected to avoid ambiguous allocations.
//   - Elements of src are trivially copyable as raw bytes (no Go pointers inside).
//
// Performance implications:
//   - Single C call handles both allocation and memory copy of totalSize bytes.
//   - Zero Go allocations, minimal Go runtime overhead.
//
// Usage example:
//
//	// Copy a []byte into C-managed memory:
//	data := []byte("hello QDB")
//	ptr, err := qdbAllocAndCopyBytes[byte](h, data)
//
//	if err != nil {
//	    panic(err) // cannot proceed without valid buffer
//	}
//
//	// ... Work with ptr, then free underlying buffer:
//
//	qdbRelease(h, ptr)
func qdbAllocAndCopyBytes[T any](h HandleType, src []T) (unsafe.Pointer, error) {
	n := len(src)
	if n == 0 {
		return nil, fmt.Errorf("source slice is empty; cannot allocate buffer")
	}

	totalSize := int(unsafe.Sizeof(src[0])) * n

	var basePtr unsafe.Pointer
	errCode := C.qdb_copy_alloc_buffer(h.handle, unsafe.Pointer(&src[0]), C.qdb_size_t(totalSize), &basePtr)
	err := makeErrorOrNil(errCode)
	if err != nil {
		return nil, err
	}

	if basePtr == nil {
		return nil, fmt.Errorf("qdbAllocAndCopyBytes: returned nil pointer")
	}

	return basePtr, nil
}

// qdbAllocAndCopyBuffer allocates C-managed memory and copies a Go slice into it, returning a typed pointer *Dst.
// Internally calls qdbAllocAndCopyBytes for allocation+copy, then casts to *Dst directly.
//
// Key decisions and trade-offs:
//   - Encapsulates primitive allocation+copy into typed pointer, reducing caller boilerplate.
//   - Dst type must match underlying byte representation of Src elements.
//
// Assumptions:
//   - Src and Dst element types have identical size and layout.
//   - len(src) > 0 to avoid zero-length ambiguous allocations.
//
// Performance implications:
//   - One C API call for allocation and bulk copy; zero Go heap allocations except pointer itself.
//   - Caller can immediately use returned *Dst, then release via qdbRelease when done.
//
// Usage example:
//
//      // Copy []int32 into a C-managed buffer typed as *int32
//      src := []int32{1, 2, 3, 4}
//      ptr, err := qdbAllocAndCopyBuffer[int32, int32](h, src)
//      if err != nil {
//          panic(err)
//      }
//
//      // ... Work with ptr, then free underlying buffer:
//
//      qdbRelease(h, ptr)

func qdbAllocAndCopyBuffer[Src, Dst any](h HandleType, src []Src) (*Dst, error) {
	basePtr, err := qdbAllocAndCopyBytes(h, src)
	if err != nil {
		return nil, err
	}

	return (*Dst)(basePtr), nil
}

// qdbRelease frees C-managed memory given a typed pointer *T.
// Under the hood, calls qdbReleasePointer with unsafe.Pointer for generic release logic.
//
// Key decisions and trade-offs:
//   - Provides type-safe wrapper so caller does not need unsafe.Pointer directly.
//   - Minimal overhead: just an inline call to qdbReleasePointer.
//
// Assumptions:
//   - ptr is non-nil and was allocated by QDB C API (via qdbAllocBytes or qdbAllocAndCopyBuffer).
//   - Releasing the wrong pointer type leads to undefined behavior in C runtime.
//
// Performance implications:
//   - Single C call to free; negligible Go overhead.
//
// Usage example:
//
//	// After allocating via qdbAllocBuffer or qdbAllocAndCopyBuffer:
//	ptr, _ := qdbAllocBuffer[int64](h, 100)
//
//	// ... Work with ptr, then free underlying buffer:
//
//	qdbRelease(h, ptr) // frees underlying C memory
func qdbRelease[T any](h HandleType, ptr *T) {
	qdbReleasePointer(h, unsafe.Pointer(ptr))
}

// qdbReleasePointer frees C-managed memory given an unsafe.Pointer.
// Use this when raw unsafe.Pointer was returned from qdbAllocBytes or qdbAllocAndCopyBytes.
//
// Key decisions and trade-offs:
//   - Low-level release interface for maximum flexibility.
//   - Caller must track pointers manually, no type safety.
//
// Assumptions:
//   - ptr is non-nil and points to memory allocated by QDB C API.
//   - Double-free or freeing non-QDB memory results in undefined behavior.
//
// Performance implications:
//   - Single C call; negligible Go runtime cost.
//
// Usage example:
//
//	rawPtr, _ := qdbAllocBytes(h, 512)
//
//	// ... use rawPtr ...
//
//	qdbReleasePointer(h, rawPtr) // release when finished
func qdbReleasePointer(h HandleType, ptr unsafe.Pointer) {
	C.qdb_release(h.handle, ptr)
}

// qdbCopyString allocates a C-style null-terminated copy of a Go string via QDB C API.
// Returns *C.char which must be released via qdbReleasePointer when no longer needed.
//
// Key decisions and trade-offs:
//   - Uses qdbAllocAndCopyBytes to leverage QDB’s allocator and optimized memory copy.
//   - Appends a NUL terminator explicitly to satisfy C string conventions.
//   - Avoids Go heap for large strings, reducing garbage collector pressure.
//
// Assumptions:
//   - len(s) > 0; empty string is invalid because no allocation needed or returned pointer would point to just NUL.
//   - Caller must free returned *C.char via qdbReleasePointer to avoid memory leak.
//
// Performance implications:
//   - Single allocation+copy in C; O(len(s)) time, zero Go heap allocation beyond pointer.
//   - Ideal when passing large strings into C code repeatedly.
//
// Usage example:
//
//	// Copy a Go string into C-managed memory:
//	cStr, err := qdbCopyString(h, \"SELECT * FROM table\")
//	if err != nil {
//	    panic(err)
//	}
//	// Use cStr with QDB C API, then free:
//	qdbReleasePointer(h, unsafe.Pointer(cStr))
func qdbCopyString(h HandleType, s string) (*C.char, error) {
	if len(s) == 0 {
		return nil, fmt.Errorf("cannot allocate empty string")
	}

	buf := append(unsafe.Slice(unsafe.StringData(s), len(s)), 0)

	ptr, err := qdbAllocAndCopyBytes(h, buf)
	if err != nil {
		return nil, fmt.Errorf("qdbCopyString: allocation failed: %w", err)
	}

	return (*C.char)(unsafe.Pointer(ptr)), nil
}

// pinStringBytes appends a NUL terminator to *s (if missing), pins the backing
// byte array and returns a *C.char pointing at it.
// Decision rationale:
//   - Re-uses Go storage → avoids extra C allocation and copy.
//
// Key assumptions:
//   - p lifetime ≥ any C usage of the returned pointer.
//   - *s is a valid Go string; mutation is limited to optional NUL append.
//
// Performance trade-offs:
//   - One O(len(s)) copy only when the NUL terminator must be appended.
//
// Usage example:
//
//	// var p runtime.Pinner
//	cStr := pinStringBytes(&p, &tableName)
//	defer p.Unpin()
func pinStringBytes(p *runtime.Pinner, s *string) *C.char {
	if s == nil {
		return nil
	}
	// Make sure the string ends with '\0'
	if len(*s) == 0 || (*s)[len(*s)-1] != 0 {
		*s = *s + "\x00"
	}
	ptr := unsafe.StringData(*s) // pointer into Go heap
	p.Pin(ptr)                   // keep it immovable until Unpin
	return (*C.char)(unsafe.Pointer(ptr))
}

// castSlice performs a zero-copy reinterpretation from a slice of type []From to []To.
//
// Decision rationale:
//   - Avoiding memory copies in performance-critical code paths drastically reduces runtime overhead.
//   - Typical scenarios involve billions of operations per second, where unnecessary copying significantly affects throughput.
//
// Critical caller assumptions (must hold true to ensure correctness and safety):
//   - Types `From` and `To` share identical memory layouts, including size, alignment, and binary representation.
//   - Caller explicitly guarantees these assumptions; violations lead to undefined behavior.
//
// Safety and correctness checks (returns an error explicitly if):
//   - The input slice is empty (taking the address of the first element would panic).
//   - The sizes of `From` and `To` differ (this would immediately result in memory corruption).
//
// Edge cases handled explicitly:
//   - If input is nil, returns nil to simplify safe method chaining.
//
// Performance implications:
//   - Zero-copy and zero-allocation behavior; execution time is negligible even at extremely high call rates.
//   - Optimal for extremely hot, performance-critical code paths.
//
// Usage context and explicit example:
//
//	// Example: reinterpret a slice of externally managed C integers as Go int64 slice without copying
//	var cInts []C.qdb_int_t = fetchDataFromC()
//
//	goInts, err := castSlice[C.qdb_int_t, int64](cInts)
//	if err != nil {
//	    panic(err) // explicit panic ensures strict enforcement of critical safety guarantees
//	}
//
//	// goInts directly references the original memory. If the memory lifetime is uncertain,
//	// explicitly copy the data with copySlice(goInts) to guarantee memory safety.
func castSlice[From, To any](input []From) ([]To, error) {
	var from From
	var to To

	if reflect.TypeOf(from).Size() != reflect.TypeOf(to).Size() {
		return nil, fmt.Errorf("unsafeCastSlice: source and destination types differ in size: %d != %d", reflect.TypeOf(from).Size(), reflect.TypeOf(to).Size())
	}

	if input == nil {
		return nil, nil
	}

	if len(input) == 0 {
		return nil, fmt.Errorf("unsafeCastSlice: input slice is empty")
	}

	ptr := unsafe.Pointer(&input[0])

	return unsafe.Slice((*To)(ptr), len(input)), nil
}

// copySlice creates a fully Go-managed, independent copy of the provided slice.
//
// Decision rationale:
//   - Using explicit copying ensures that the returned slice has a predictable lifetime and is safe
//     against external mutations (e.g., from externally-managed C memory).
//
// Key assumptions:
//   - Caller intentionally requests a deep copy to achieve memory isolation.
//
// Performance trade-offs:
//   - Introduces O(n) memory-copy overhead. This operation leverages Go's optimized runtime implementation
//     (typically via memmove), but repeated use on extremely hot code paths may become measurable.
//
// Usage context and example:
//
//	// Example scenario: safely copying an unsafe slice obtained via cPointerArrayToSliceUnsafe.
//	var cPtr *C.qdb_int_t
//	var rowCount int64
//
//	unsafeSlice, err := cPointerArrayToSliceUnsafe[C.qdb_int_t, int64](unsafe.Pointer(cPtr), rowCount)
//	if err != nil {
//	    panic(err) // strict enforcement of precondition validity
//	}
//
//	safeSlice := copySlice(unsafeSlice) // safe, Go-managed copy
func copySlice[T any](xs []T) []T {
	ret := make([]T, len(xs))
	copy(ret, xs)
	return ret
}

// cPointerArrayToSliceUnsafe converts a raw C-style memory array (void*) directly into a Go slice without copying.
//
// Decision rationale:
//   - Zero-copy reinterpretation achieves optimal performance critical to high-throughput, latency-sensitive code paths.
//   - Use when external memory lifecycle management is strictly controlled and well-understood by the caller.
//
// Critical preconditions (must be strictly maintained by the caller):
//   - Types `From` and `To` have precisely matching binary layouts (size, alignment, representation).
//   - The input pointer (`xs`) is valid, non-nil, and points to a memory region with at least `n` elements.
//   - Caller explicitly manages the lifetime of referenced memory to prevent undefined behavior.
//
// Safety considerations:
//   - This function intentionally bypasses Go's memory safety. Violating these assumptions leads to severe
//     memory corruption and/or undefined behavior.
//
// Performance implications:
//   - Zero-copy yields maximum throughput with negligible latency.
//
// Usage context and example:
//
//	// Example scenario: directly mapping externally managed (C) memory to Go without copying.
//	var cPtr *C.qdb_double_t
//	var rowCount int64
//
//	unsafeSlice, err := cPointerArrayToSliceUnsafe[C.qdb_double_t, float64](unsafe.Pointer(cPtr), rowCount)
//	if err != nil {
//	    panic(err) // enforce correctness and explicit caller guarantees
//	}
//
//	// Use `unsafeSlice` directly, ensuring the original C memory remains valid throughout.
//	// If independent Go memory is later needed, explicitly invoke copySlice(unsafeSlice).
func cPointerArrayToSliceUnsafe[From, To any](xs unsafe.Pointer, n int64) ([]To, error) {
	if xs == nil {
		return nil, fmt.Errorf("cPointerArrayToSliceUnsafe: input pointer is nil")
	}

	ptr := (*From)(xs)
	slice, err := castSlice[From, To](unsafe.Slice(ptr, n))
	if err != nil {
		return nil, fmt.Errorf("cPointerArrayToSliceUnsafe: %v", err)
	}

	return slice, nil
}

// cPointerArrayToSlice safely converts a raw C-style memory array (void*) into a fully Go-managed slice.
//
// Decision rationale:
//   - Explicitly copies external memory, providing safe memory isolation from external sources.
//   - Recommended when simplicity of memory management outweighs raw throughput considerations.
//
// Key assumptions:
//   - Caller prioritizes memory safety over absolute maximum performance.
//
// Performance trade-offs:
//   - Introduces explicit O(n) memory-copy overhead. Leveraging Go runtime optimizations (memmove),
//     the overhead remains moderate but measurable under very high load.
//
// Safety and memory considerations:
//   - Resulting slice lifetime is independent of the original external memory. The original memory can safely be freed after use.
//
// Usage context and example:
//
//	// Example scenario: converting external (C-managed) memory safely to Go-managed memory.
//	var cPtr *C.qdb_timespec_t
//	var rowCount int64
//
//	safeSlice, err := cPointerArrayToSlice[C.qdb_timespec_t, time.Time](unsafe.Pointer(cPtr), rowCount)
//	if err != nil {
//	    panic(err) // explicit check enforces runtime correctness assumptions
//	}
//
//	// safeSlice is now fully Go-managed, independent of original C memory allocation.
//	// Original memory can safely be released after conversion.
func cPointerArrayToSlice[From, To any](xs unsafe.Pointer, n int64) ([]To, error) {
	ret, err := cPointerArrayToSliceUnsafe[From, To](xs, n)
	if err != nil {
		return nil, fmt.Errorf("cPointerArrayToSlice: %v", err)
	}

	return copySlice(ret), nil
}

// sliceEnsureCapacity ensures that the provided slice has a capacity of at least n.
// If xs is nil, it returns a new slice of length 0 and capacity n.
// If cap(xs) ≥ n, it returns xs unchanged. Otherwise, it allocates a new slice
// with the same length as xs but capacity n, copies the elements, and returns it.
//
// Trade-offs and performance implications:
//   - If cap(xs) < n, this performs one allocation of size n×sizeof(E) and one bulk copy O(len(xs)).
//   - If cap(xs) ≥ n, there is zero allocation and zero copy overhead.
//   - Avoids repeated reallocations when appending to slices in hot loops by pre-reserving capacity.
//
// Assumptions:
//   - xs can be nil or a valid slice. A nil xs is treated like an empty slice of length 0.
//   - n ≥ 0; negative n is undefined behavior (caller responsibility).
//
// Usage example:
//
//	// Case 1: xs is nil, want capacity 100
//	var xs []int64
//	xs = sliceEnsureCapacity[int64](xs, 100)
//	// Now len(xs)==0, cap(xs)==100
//
//	// Case 2: xs has len 5, cap 5, want capacity 20
//	xs = []int64{1, 2, 3, 4, 5}
//	xs = sliceEnsureCapacity[int64](xs, 20)
//	// Now len(xs)==5, cap(xs)==20, original elements preserved
//
//	// Case 3: xs already has cap ≥ n
//	xs = make([]int64, 3, 50)
//	xs = sliceEnsureCapacity[int64](xs, 20)
//	// cap(xs) was 50, ≥20, so xs returned unchanged
func sliceEnsureCapacity[E any](xs []E, n int) []E {
	// Treat nil like an empty slice; allocate new slice of length 0, capacity n.
	if xs == nil {
		return make([]E, 0, n)
	}

	// If existing capacity is sufficient, return unchanged.
	if cap(xs) >= n {
		return xs
	}

	// Need a larger-capacity slice: allocate with same length, new capacity n.
	ys := make([]E, len(xs), n)
	copy(ys, xs) // bulk copy of existing elements
	return ys
}

// ifaceDataPtr extracts the data-word (pointer to the concrete value) from an
// interface value and returns it as unsafe.Pointer.
// Decision rationale:
//   - Enables zero-allocation fast paths (e.g. appendDataUnsafe) by avoiding
//     reflect-based conversions.
//
// Key assumptions:
//   - Go interface layout is two machine words: (itab, data); stable for Go ≥1.20.
//   - i is non-nil; if nil, the returned pointer is nil.
//   - The concrete value referenced by the returned pointer outlives all uses
//     and is not moved by the GC (caller responsibility).
//
// Performance trade-offs:
//   - O(1), zero allocations, but completely bypasses the type system; misuse
//     causes hard-to-debug memory corruption.
//
// Usage example:
//
//	// Unsafe cast without the runtime type-check:
//	other := (*ColumnDataInt64)(ifaceDataPtr(cd))
func ifaceDataPtr(i interface{}) unsafe.Pointer {
	type iface struct {
		tab  unsafe.Pointer
		data unsafe.Pointer
	}

	return (*iface)(unsafe.Pointer(&i)).data
}

// JSONPath wraps parsed JSON data and provides dot-notation path navigation.
// Designed as a minimal replacement for gabs.Container to eliminate external dependencies.
//
// Decision rationale:
// - Avoids external dependency on gabs library for simple JSON path navigation.
// - Provides familiar API to minimize migration effort.
//
// Key assumptions:
// - JSON is already parsed into map[string]interface{} or compatible structure.
// - Path strings use dot notation (e.g., "parent.child.value").
// - Type assertions are caller's responsibility after navigation.
//
// Performance trade-offs:
// - Path parsing allocates a string slice for split segments.
// - Each navigation step performs type assertion and map lookup.
// - Suitable for config/metadata access, not hot paths.
type JSONPath struct {
	data interface{}
}

// parseJSON parses JSON bytes and returns a JSONPath wrapper for navigation.
// Mirrors gabs.ParseJSON functionality to ease migration from external dependency.
//
// Decision rationale:
// - Provides drop-in replacement for gabs.ParseJSON in existing code.
// - Uses standard library json.Unmarshal for robust parsing.
//
// Key assumptions:
// - Input is valid JSON; malformed JSON returns error.
// - Root is typically object (map) or array; primitives are valid but less useful.
//
// Performance trade-offs:
// - Standard json.Unmarshal performance characteristics apply.
// - Allocates interface{} tree structure proportional to JSON complexity.
//
// Usage example:
// // Parse config JSON and navigate to nested field:
// parsed, err := parseJSON(configBytes)
//
//	if err != nil {
//	    return err
//	}
//
// listenAddr := parsed.Path("local.network.listen_on").Data().(string)
func parseJSON(data []byte) (*JSONPath, error) {
	var result interface{}
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return &JSONPath{data: result}, nil
}

// Path navigates to a nested field using dot notation and returns a new JSONPath.
// Returns JSONPath with nil data if path cannot be resolved.
//
// Decision rationale:
// - Matches gabs.Path behavior for compatibility.
// - Returns wrapper even on failure to allow safe chaining.
//
// Key assumptions:
// - Path segments separated by dots map to object keys.
// - Intermediate values must be map[string]interface{} to continue traversal.
// - Arrays/slices not supported in path notation (differs from full gabs).
//
// Performance trade-offs:
// - O(n) where n is number of path segments.
// - String split allocates; consider caching if called repeatedly with same paths.
//
// Usage example:
// // Navigate nested config:
// dbPath := config.Path("local.depot.rocksdb.root").Data()
//
//	if dbPath == nil {
//	    // handle missing config key
//	}
func (j *JSONPath) Path(path string) *JSONPath {
	if j.data == nil {
		return &JSONPath{data: nil}
	}

	current := j.data
	segments := strings.Split(path, ".")

	for _, segment := range segments {
		switch v := current.(type) {
		case map[string]interface{}:
			next, exists := v[segment]
			if !exists {
				return &JSONPath{data: nil}
			}
			current = next
		default:
			// Cannot traverse non-map types
			return &JSONPath{data: nil}
		}
	}

	return &JSONPath{data: current}
}

// Data returns the underlying data at this path location.
// Returns nil if path was not found during navigation.
//
// Decision rationale:
// - Matches gabs.Data API for drop-in compatibility.
// - Allows caller to perform type assertions as needed.
//
// Key assumptions:
// - Caller handles nil checks before type assertion.
// - Type assertion panics are caller's responsibility.
//
// Usage example:
// // Get string value with type assertion:
//
//	if value := parsed.Path("key").Data(); value != nil {
//	    strValue := value.(string)
//	}
func (j *JSONPath) Data() interface{} {
	return j.data
}
