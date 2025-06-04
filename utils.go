package qdb

/*
	#include <stdlib.h>
        #include <string.h> // for memcpy
	#include <qdb/client.h>
*/
import "C"
import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"time"
	"unsafe"
)

// All available columns we support
var columnTypes = [...]TsColumnType{TsColumnInt64, TsColumnDouble, TsColumnTimestamp, TsColumnBlob, TsColumnString}

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

func releaseCharStarStar(data unsafe.Pointer, size int) {
	var v *C.char
	ptrSize := unsafe.Sizeof(v)
	for i := 0; i < size; i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(data) + uintptr(i)*ptrSize))
		releaseCharStar(*element)
	}
	C.free(data)
}

func convertToCharStar(toConvert string) *C.char {
	if len(toConvert) == 0 {
		return nil
	}
	return C.CString(toConvert)
}

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

// Returns a default-size alias (16 characters
func generateDefaultAlias() string {
	return generateAlias(16)
}

func generateColumnName() string {
	return generateAlias(16)
}

// Returns a random column type
func randomColumnType() TsColumnType {
	n := rand.Intn(len(columnTypes))

	return columnTypes[n]

}

// Generates names for exactly `n` column names
func generateColumnNames(n int) []string {
	var ret []string = make([]string, n)

	for i, _ := range ret {
		ret[i] = generateColumnName()
	}

	return ret
}

// Generate writer column info for exactly `n` columns.
func generateWriterColumns(n int) []WriterColumn {

	var ret []WriterColumn = make([]WriterColumn, n)

	for i, _ := range ret {
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

	for i, _ := range ret {
		ret[i].ColumnType = ctype
	}

	return ret
}

// Takes an array of WriterColumns and converts it to TsColumnInfo, which can then be
// used to e.g. create a table.
func convertWriterColumnsToColumnInfo(xs []WriterColumn) []TsColumnInfo {
	ret := make([]TsColumnInfo, len(xs))

	for i, _ := range xs {
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

func generateWriterDataInt64(n int) WriterData {
	xs := make([]int64, n)

	for i, _ := range xs {
		xs[i] = rand.Int63()
	}

	return NewWriterDataInt64(xs)
}

func generateWriterDataDouble(n int) WriterData {
	xs := make([]float64, n)

	for i, _ := range xs {
		xs[i] = rand.NormFloat64()
	}

	return NewWriterDataDouble(xs)
}

func generateWriterDataTimestamp(n int) WriterData {
	// XXX(leon): should be improved to be more random, instead
	//            we're reusing the code that generates the index here.
	return NewWriterDataTimestamp(generateDefaultIndex(n))
}

func generateWriterDataBlob(n int) WriterData {
	xs := make([][]byte, n)

	for i, _ := range xs {
		// Hard-coded 16 byte blobs, could be randomized.
		x := make([]byte, 16)
		n_, err := rand.Read(x)

		if err != nil {
			panic(err)
		}

		if n_ != len(x) {
			panic(fmt.Sprintf("Random generator did not return the amount of bytes we expected to be read: %v", n_))
		}

		xs[i] = x
	}

	return NewWriterDataBlob(xs)
}

func generateWriterDataString(n int) WriterData {
	xs := make([]string, n)

	for i, _ := range xs {
		// We just defer to generateAlias(), which already generates random strings.
		// As with blobs, we'll use a hardcoded 16 length
		xs[i] = generateAlias(16)
	}

	return NewWriterDataString(xs)
}

// Generates artifical writer data for a single column
func generateWriterData(n int, column WriterColumn) (WriterData, error) {
	switch column.ColumnType {
	case TsColumnBlob:
		return generateWriterDataBlob(n), nil
	case TsColumnSymbol:
		// Symbols are represented as strings to the user
		fallthrough
	case TsColumnString:
		return generateWriterDataString(n), nil
	case TsColumnInt64:
		return generateWriterDataInt64(n), nil
	case TsColumnDouble:
		return generateWriterDataDouble(n), nil
	case TsColumnTimestamp:
		return generateWriterDataTimestamp(n), nil
	}

	return nil, fmt.Errorf("Unrecognized column type: %v", column.ColumnType)
}

// Generates artificial data to be inserted for each column.
func generateWriterDatas(n int, columns []WriterColumn) ([]WriterData, error) {
	var ret []WriterData = make([]WriterData, len(columns))

	for i, column := range columns {
		data, err := generateWriterData(n, column)

		if err != nil {
			return nil, fmt.Errorf("generateWriterData failed for column %d (%v): %w", i, column.ColumnType, err)
		}

		ret[i] = data
	}

	return ret, nil
}

// Generates an time index
func generateIndex(n int, start time.Time, step time.Duration) []time.Time {
	var ret []time.Time = make([]time.Time, n)

	for i, _ := range ret {
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

// qdbAllocBytes directly allocates a buffer of the specified byte size.
// You must explicitly call qdb_release() to free the allocated memory.
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

// qdbAllocBuffer allocates a typed buffer via qdbAllocBytes.
// It explicitly calculates buffer size based on the provided element type and count.
func qdbAllocBuffer[T any](h HandleType, count int) (*T, error) {
	totalSize := int(unsafe.Sizeof(*new(T))) * count
	ptr, err := qdbAllocBytes(h, totalSize)

	if err != nil {
		return nil, err
	}

	return (*T)(ptr), nil
}

// qdbAllocAndCopyBytes allocates memory using C.qdb_copy_alloc_buffer, and returns
// and arbitrary pointer (unsafe.Pointer)
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

// qdbAllocAndCopyBytes allocates memory using qdbAllocBytes, copies the provided Go slice into it,
// and returns a typed pointer (*Dst). This function safely encapsulates unsafe memory conversions.
// Caller is responsible for releasing allocated memory.
func qdbAllocAndCopyBuffer[Src any, Dst any](h HandleType, src []Src) (*Dst, error) {

	basePtr, err := qdbAllocAndCopyBytes(h, src)
	if err != nil {
		return nil, err
	}

	return (*Dst)(basePtr), nil
}

func qdbRelease[T any](h HandleType, ptr *T) {
	qdbReleasePointer(h, unsafe.Pointer(ptr))
}

func qdbReleasePointer(h HandleType, ptr unsafe.Pointer) {
	C.qdb_release(h.handle, ptr)
}

// Copies a Go string and returns a `char const *`-like string. Allocates memory using
// the QDB C API's memory handler for high performance memory allocation. Must be released
// by the user using `qdbRelease()` when done.
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
func castSlice[From any, To any](input []From) ([]To, error) {
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
func cPointerArrayToSliceUnsafe[From any, To any](xs unsafe.Pointer, n int64) ([]To, error) {
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
func cPointerArrayToSlice[From any, To any](xs unsafe.Pointer, n int64) ([]To, error) {
	ret, err := cPointerArrayToSliceUnsafe[From, To](xs, n)
	if err != nil {
		return nil, fmt.Errorf("cPointerArrayToSlice: %v", err)
	}

	return copySlice(ret), nil
}
