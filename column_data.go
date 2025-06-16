package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"
	"runtime"
)

// ColumnData unifies both Reader and Writer data interfaces.
// It represents any column-typed data that can be both read (into Go slices)
// and written (to C via toNative).
//
// This mirrors what the QuasarDB C API does, except that the QuasarDB C API
// just aliases reader to writer:
//
//	typedef qdb_exp_batch_push_table_data_t qdb_bulk_reader_table_data_t;
//
// As seen in `qdb/include/qdb/ts.h`. As such, you'll see implementations of
// this interface use `qdb_exp_batch_push...`-types, but they work for both
// reader and writer.
type ColumnData interface {
	// Returns the type of data for this column
	ValueType() TsValueType

	// Ensures the underlying data pre-allocates a certain capacity, useful when we know
	// we will have multiple incremental allocations, e.g. when we use it in combination
	// with appendData.
	EnsureCapacity(n int)

	// Ensures underlying data is reset to 0
	Clear()

	// Returns the number of items in the array
	Length() int

	// Appends another chunk of `ColumnData` to this. Validates that they are of identical types.
	// The intent of this function is the "merge" multiple chunks of ColumnData together in case
	// the user calls FetchAll().
	//
	// Returns error if ColumnData is not of the same concrete type.
	appendData(data ColumnData) error

	// Unsafe variant of appendData(), does not check for type safety
	appendDataUnsafe(data ColumnData)

	// Zero-copy / pin path.  The returned release-fn **must always be
	// called** by the caller once the C function that consumed the buffer
	// has returned (typically right after qdb_exp_batch_push_with_options).
	//
	//   p       – caller-owned pinner that outlives the C call.
	//   h       – handle, in case a tiny C allocation is still required
	//             (e.g., the []qdb_string_t envelope for strings).
	//
	//   ptr     – address to be stored in qdb_exp_batch_push_column_t.data
	//             (never nil; on error PinToC returns (nil, nil, err)).
	//
	//   release – releases any C allocations done inside PinToC;
	//             it is a no-op for “pure pin” cases.
	//
	// Implementations of this function always pin memory to the pinner whenever
	// possible.
	PinToC(p *runtime.Pinner, h HandleType) (ptr unsafe.Pointer, release func())
}

// --- Int64 ---------------------------------------------------------

type ColumnDataInt64 struct {
	xs []int64
}


// Length reports the number of int64 values held in the column.
func (cd *ColumnDataInt64) Length() int {
	return len(cd.xs)
}

// ValueType implements ColumnData.ValueType.
func (cd *ColumnDataInt64) ValueType() TsValueType {
	return TsValueInt64
}

// EnsureCapacity pre-allocates capacity n to reduce reallocations.
func (cd *ColumnDataInt64) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets the slice to length 0 while keeping capacity.
func (cd *ColumnDataInt64) Clear() {
	cd.xs = make([]int64, 0)
}

// PinToC exposes the Go slice to C without copying:
//   – pins the slice base so it stays immovable
//   – returns a no-op release closure
func (cd *ColumnDataInt64) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	if len(cd.xs) == 0 {
		return nil, func() {}
	}
	base := &cd.xs[0]
	p.Pin(base)
	return unsafe.Pointer(base), func() {}
}

// appendData concatenates another ColumnDataInt64 after type checking.
func (cd *ColumnDataInt64) appendData(data ColumnData) error {
	other, ok := data.(*ColumnDataInt64)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ColumnDataInt64, got %T", data)
	}

	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe performs the same concatenation without RTTI checks.
func (cd *ColumnDataInt64) appendDataUnsafe(data ColumnData) {
	other := (*ColumnDataInt64)(ifaceDataPtr(data))
	cd.xs = append(cd.xs, other.xs...)
}

// newColumnDataInt64 wraps an existing []int64 in ColumnDataInt64.
func newColumnDataInt64(xs []int64) ColumnDataInt64 {
	return ColumnDataInt64{xs: xs}
}

/*
newColumnDataInt64FromNative constructs a ColumnDataInt64 by copying
int64 values from a C buffer into Go-managed memory.

Decision rationale:
  - Copy to Go slice ensures memory safety if C buffer is later released.
  - Early validation of data_type and length prevents out-of-bounds.

Key assumptions:
  - xs.data_type == C.qdb_ts_column_int64.
  - n > 0 and xs.data pointer is non-nil.

Performance trade-offs:
  - O(n) copy cost, acceptable for safety and simplicity.

Usage example:

	// arr is a C.qdb_exp_batch_push_column_t from qdb_ts_read_columns
	cd, err := newColumnDataInt64FromNative("myCol", arr, length)
	if err != nil {
	    // handle error
	}
	data := cd.Data()
*/
func newColumnDataInt64FromNative(name string, xs C.qdb_exp_batch_push_column_t, n int) (ColumnDataInt64, error) {

	// Ensure the column type matches int64; mismatches lead to invalid reads.
	if xs.data_type != C.qdb_ts_column_int64 {
		return ColumnDataInt64{},
			fmt.Errorf("newColumnDataInt64FromNative: expected data_type int64, got %v", xs.data_type)
	}
	// Reject non-positive lengths; indexing with n <= 0 is invalid.
	if n <= 0 {
		return ColumnDataInt64{},
			fmt.Errorf("newColumnDataInt64FromNative: invalid column length %d", n)
	}

	// Extract the raw C pointer from xs.data[0] (void* array). Casting via unsafe.Pointer
	// ensures portability across architectures (pointer size differences).
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if rawPtr == nil {
		return ColumnDataInt64{},
			fmt.Errorf("newColumnDataInt64FromNative: nil data pointer for column %q", name)
	}

	// SAFELY convert the C pointer array to a fully Go-managed []int64 slice.
	// cPointerArrayToSlice enforces:
	//   - runtime check that sizeof(C.qdb_int_t) == sizeof(int64)
	//   - bounds validation on length
	//   - explicit copy from C memory into Go memory
	// This guarantees memory safety even if the original C buffer is freed afterwacd.
	goSlice, err := cPointerArrayToSlice[C.qdb_int_t, int64](rawPtr, int64(n))
	if err != nil {
		return ColumnDataInt64{}, fmt.Errorf("newColumnDataInt64FromNative: %v", err)
	}

	// Wrap the Go-managed []int64 in a ColumnDataInt64 and return.
	// Using newColumnDataInt64 centralizes ColumnDataInt64 construction logic.
	return newColumnDataInt64(goSlice), nil
}

/*
GetColumnDataInt64 performs a safe extraction of []int64 from ColumnData.

Decision rationale:
  - Runtime type assertion prevents misuse on wrong ColumnData implementations.

Key assumptions:
  - cd is of concrete type *ColumnDataInt64.

Usage example:

	xs, err := GetColumnDataInt64(cd)
	if err != nil {
	    // handle type mismatch
	}
*/
func GetColumnDataInt64(cd ColumnData) ([]int64, error) {
	v, ok := cd.(*ColumnDataInt64)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataInt64: type mismatch, expected ColumnDataInt64, got %T", cd)
	}
	return v.xs, nil
}

/*
GetColumnDataInt64Unsafe returns the []int64 without any type checks.

Decision rationale:
  - Bypasses runtime assertion for hot paths where type is guaranteed by caller.

Key assumptions:
  - cd must be a *ColumnDataInt64, otherwise behaviour is undefined.

Usage example:

	xs := GetColumnDataInt64Unsafe(cd)
*/
func GetColumnDataInt64Unsafe(cd ColumnData) []int64 {
	return (*ColumnDataInt64)(ifaceDataPtr(cd)).xs
}

// --- Double -------------------------------------------------------

type ColumnDataDouble struct{ xs []float64 }

// Length reports the number of float64 values held in the column.
func (cd *ColumnDataDouble) Length() int {
	return len(cd.xs)
}

// ValueType implements ColumnData.ValueType.
func (cd *ColumnDataDouble) ValueType() TsValueType {
	return TsValueDouble
}

// EnsureCapacity pre-allocates capacity n to reduce reallocations.
func (cd *ColumnDataDouble) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets the slice to length 0 while keeping capacity.
func (cd *ColumnDataDouble) Clear() {
	cd.xs = make([]float64, 0)
}

// appendData concatenates another ColumnDataDouble after type checking.
func (cd *ColumnDataDouble) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataDouble)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataDouble, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe performs the same concatenation without RTTI checks.
func (cd *ColumnDataDouble) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataDouble)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC exposes the Go slice to C without copying:
//   – pins the slice base so it stays immovable
//   – returns a no-op release closure
func (cd *ColumnDataDouble) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	if len(cd.xs) == 0 {
		return nil, func() {}
	}
	base := &cd.xs[0]
	p.Pin(base)
	return unsafe.Pointer(base), func() {}
}

// newColumnDataDouble wraps an existing []float64 in ColumnDataDouble.
func newColumnDataDouble(xs []float64) ColumnDataDouble { return ColumnDataDouble{xs: xs} }
func newColumnDataDoubleFromNative(
	name string, xs C.qdb_exp_batch_push_column_t, n int,
) (ColumnDataDouble, error) {
	if xs.data_type != C.qdb_ts_column_double {
		return ColumnDataDouble{}, fmt.Errorf("expected double, got %v", xs.data_type)
	}
	if n <= 0 {
		return ColumnDataDouble{}, fmt.Errorf("invalid length %d", n)
	}
	raw := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if raw == nil {
		return ColumnDataDouble{}, fmt.Errorf("nil data ptr for %q", name)
	}
	goSlice, err := cPointerArrayToSlice[C.double, float64](raw, int64(n))
	if err != nil {
		return ColumnDataDouble{}, err
	}
	return newColumnDataDouble(goSlice), nil
}
func GetColumnDataDouble(cd ColumnData) ([]float64, error) {
	v, ok := cd.(*ColumnDataDouble)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataDouble: expected ColumnDataDouble, got %T", cd)
	}
	return v.xs, nil
}
func GetColumnDataDoubleUnsafe(cd ColumnData) []float64 {
	return (*ColumnDataDouble)(ifaceDataPtr(cd)).xs
}

// --- Timestamp ----------------------------------------------------

type ColumnDataTimestamp struct{ xs []C.qdb_timespec_t }

// Length reports the number of timestamp values held in the column.
func (cd *ColumnDataTimestamp) Length() int {
	return len(cd.xs)
}

// ValueType implements ColumnData.ValueType.
func (cd *ColumnDataTimestamp) ValueType() TsValueType {
	return TsValueTimestamp
}

// EnsureCapacity pre-allocates capacity n to reduce reallocations.
func (cd *ColumnDataTimestamp) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets the slice to length 0 while keeping capacity.
func (cd *ColumnDataTimestamp) Clear() {
	cd.xs = make([]C.qdb_timespec_t, 0)
}

// appendData concatenates another ColumnDataTimestamp after type checking.
func (cd *ColumnDataTimestamp) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataTimestamp)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataTimestamp, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe performs the same concatenation without RTTI checks.
func (cd *ColumnDataTimestamp) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataTimestamp)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC exposes the Go slice to C without copying:
//   – pins the slice base so it stays immovable
//   – returns a no-op release closure
func (cd *ColumnDataTimestamp) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	if len(cd.xs) == 0 {
		return nil, func() {}
	}
	base := &cd.xs[0]
	p.Pin(base)
	return unsafe.Pointer(base), func() {}
}

// newColumnDataTimestamp wraps a []time.Time as ColumnDataTimestamp.
func newColumnDataTimestamp(ts []time.Time) ColumnDataTimestamp {
	return ColumnDataTimestamp{xs: TimeSliceToQdbTimespec(ts)}
}

/*
newColumnDataTimestampFromNative constructs a ColumnDataTimestamp by copying
C.qdb_timespec_t values from a C buffer into Go-managed memory.

Decision rationale:
  - Copy to Go slice ensures memory safety if C buffer is later released.
  - Early validation of data_type and length prevents out-of-bounds.

Key assumptions:
  - xs.data_type == C.qdb_ts_column_timestamp.
  - n > 0 and xs.data pointer is non-nil.

Performance trade-offs:
  - O(n) copy cost, acceptable for safety and simplicity.

Usage example:

	// arr is a C.qdb_exp_batch_push_column_t from qdb_ts_read_columns
	cd, err := newColumnDataTimestampFromNative("myCol", arr, length)
	if err != nil {
	    // handle error
	}
	data := cd.Data()
*/
func newColumnDataTimestampFromNative(
	name string, xs C.qdb_exp_batch_push_column_t, n int,
) (ColumnDataTimestamp, error) {
	if xs.data_type != C.qdb_ts_column_timestamp {
		return ColumnDataTimestamp{}, fmt.Errorf("expected timestamp, got %v", xs.data_type)
	}
	if n <= 0 {
		return ColumnDataTimestamp{}, fmt.Errorf("invalid length %d", n)
	}
	raw := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if raw == nil {
		return ColumnDataTimestamp{}, fmt.Errorf("nil data ptr for %q", name)
	}
	specs, err := cPointerArrayToSlice[C.qdb_timespec_t, C.qdb_timespec_t](raw, int64(n))
	if err != nil {
		return ColumnDataTimestamp{}, err
	}
	return ColumnDataTimestamp{xs: specs}, nil
}

// GetColumnDataTimestamp extracts a []time.Time copy from ColumnData.
func GetColumnDataTimestamp(cd ColumnData) ([]time.Time, error) {
	xs, err := GetColumnDataTimestampNative(cd)
	if err != nil {
		return nil, err
	}

	return QdbTimespecSliceToTime(xs), nil
}

// GetColumnDataTimestampNative extracts a []C.qdb_timespec_t from ColumnData.
func GetColumnDataTimestampNative(cd ColumnData) ([]C.qdb_timespec_t, error) {
	v, ok := cd.(*ColumnDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataTimestamp: expected ColumnDataTimestamp, got %T", cd)
	}
	return v.xs, nil
}

// GetColumnDataTimestampUnsafe returns a []time.Time without type checks.
func GetColumnDataTimestampUnsafe(cd ColumnData) []time.Time {
	return QdbTimespecSliceToTime(GetColumnDataTimestampNativeUnsafe(cd))
}

// GetColumnDataTimestampNativeUnsafe returns a []C.qdb_timespec_t without type checks.
func GetColumnDataTimestampNativeUnsafe(cd ColumnData) []C.qdb_timespec_t {
	return (*ColumnDataTimestamp)(ifaceDataPtr(cd)).xs
}

// --- Blob ---------------------------------------------------------

type ColumnDataBlob struct{ xs [][]byte }

// Length reports the number of blob values held in the column.
func (cd *ColumnDataBlob) Length() int {
	return len(cd.xs)
}

// ValueType implements ColumnData.ValueType.
func (cd *ColumnDataBlob) ValueType() TsValueType {
	return TsValueBlob
}

// EnsureCapacity pre-allocates capacity n to reduce reallocations.
func (cd *ColumnDataBlob) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets the slice to length 0 while keeping capacity.
func (cd *ColumnDataBlob) Clear() {
	cd.xs = make([][]byte, 0)
}

// appendData concatenates another ColumnDataBlob after type checking.
func (cd *ColumnDataBlob) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataBlob)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataBlob, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe performs the same concatenation without RTTI checks.
func (cd *ColumnDataBlob) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataBlob)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC builds a C envelope and pins each []byte in cd.xs.
//
// Decision rationale:
//   – Zero-copy path avoids duplicating potentially large blobs.
//   – release() frees only the envelope; Go memory is unpinned automatically.
func (cd *ColumnDataBlob) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	count := len(cd.xs)
	if count == 0 {
		// still return a valid no-op release func
		return nil, func() {}
	}

	// Envelope allocated once in C memory
	arr, err := qdbAllocBuffer[C.qdb_blob_t](h, count)
	if err != nil {
		panic(err)
	}
	slice := unsafe.Slice(arr, count)

	// Point each qdb_blob_t to pinned Go byte slices (zero-copy)
	for i, b := range cd.xs {
		if len(b) > 0 {
			p.Pin(&b[0])                               // keep backing array immovable
			slice[i].content = unsafe.Pointer(&b[0])   // direct pointer into Go memory
			slice[i].content_length = C.qdb_size_t(len(b))
		}
	}

	// Always return a valid release closure (no-op for the blob contents)
	release := func() {
		qdbRelease(h, arr) // free only the envelope allocated above
	}

	return unsafe.Pointer(arr), release
}

// newColumnDataBlob wraps an existing [][]byte in ColumnDataBlob.
func newColumnDataBlob(xs [][]byte) ColumnDataBlob { return ColumnDataBlob{xs: xs} }
func newColumnDataBlobFromNative(
	name string, xs C.qdb_exp_batch_push_column_t, n int,
) (ColumnDataBlob, error) {
	if xs.data_type != C.qdb_ts_column_blob {
		return ColumnDataBlob{}, fmt.Errorf("expected blob, got %v", xs.data_type)
	}
	if n <= 0 {
		return ColumnDataBlob{}, fmt.Errorf("invalid length %d", n)
	}
	raw := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if raw == nil {
		return ColumnDataBlob{}, fmt.Errorf("nil data ptr for %q", name)
	}
	blobs, err := cPointerArrayToSlice[C.qdb_blob_t, C.qdb_blob_t](raw, int64(n))
	if err != nil {
		return ColumnDataBlob{}, err
	}
	out := make([][]byte, n)
	for i, b := range blobs {
		out[i] = C.GoBytes(unsafe.Pointer(b.content), C.int(b.content_length))
	}
	return newColumnDataBlob(out), nil
}
func GetColumnDataBlob(cd ColumnData) ([][]byte, error) {
	v, ok := cd.(*ColumnDataBlob)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataBlob: expected ColumnDataBlob, got %T", cd)
	}
	return v.xs, nil
}
func GetColumnDataBlobUnsafe(cd ColumnData) [][]byte {
	return (*ColumnDataBlob)(ifaceDataPtr(cd)).xs
}

// --- String -------------------------------------------------------

type ColumnDataString struct{ xs []string }

// Length reports the number of string values held in the column.
func (cd *ColumnDataString) Length() int {
	return len(cd.xs)
}

// ValueType implements ColumnData.ValueType.
func (cd *ColumnDataString) ValueType() TsValueType {
	return TsValueString
}

// EnsureCapacity pre-allocates capacity n to reduce reallocations.
func (cd *ColumnDataString) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets the slice to length 0 while keeping capacity.
func (cd *ColumnDataString) Clear() {
	cd.xs = make([]string, 0)
}

// appendData concatenates another ColumnDataString after type checking.
func (cd *ColumnDataString) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataString)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataString, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe performs the same concatenation without RTTI checks.
func (cd *ColumnDataString) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataString)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC builds a C envelope and pins each string in cd.xs.
//
// Decision rationale:
//   – Zero-copy path avoids duplicating potentially large strings.
//   – qdb_string_t uses explicit length, so NUL-termination is not required.
func (cd *ColumnDataString) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
    count := len(cd.xs)
    if count == 0 {
        // still return a valid no-op release func
        return nil, func() {}
    }

    // Allocate the C envelope once
    arr, err := qdbAllocBuffer[C.qdb_string_t](h, count)
    if err != nil {
        panic(err)
    }
    slice := unsafe.Slice(arr, count)

    // Fill the envelope with zero-copy, pinned Go strings
    for i := 0; i < count; i++ {
        s := &cd.xs[i]
        if len(*s) == 0 {
            continue // leave data/length = 0
        }

        // qdb_string_t already carries an explicit length, therefore no
        // NUL-terminator is required.  We simply obtain the address of the
        // existing Go string bytes, pin that memory so the GC can’t move it,
        // and store the pointer/length in the envelope.
        dataPtr := unsafe.StringData(*s)      // pointer to first byte
        p.Pin(dataPtr)                        // keep the bytes immovable

        slice[i].data   = (*C.char)(unsafe.Pointer(dataPtr))
        slice[i].length = C.qdb_size_t(len(*s))
    }

    // envelope itself must be freed after the C call
    release := func() { qdbRelease(h, arr) }

    return unsafe.Pointer(arr), release
}

// newColumnDataString wraps an existing []string in ColumnDataString.
func newColumnDataString(xs []string) ColumnDataString { return ColumnDataString{xs: xs} }
func newColumnDataStringFromNative(
	name string, xs C.qdb_exp_batch_push_column_t, n int,
) (ColumnDataString, error) {
	if xs.data_type != C.qdb_ts_column_string {
		return ColumnDataString{}, fmt.Errorf("expected string, got %v", xs.data_type)
	}
	if n <= 0 {
		return ColumnDataString{}, fmt.Errorf("invalid length %d", n)
	}
	raw := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if raw == nil {
		return ColumnDataString{}, fmt.Errorf("nil data ptr for %q", name)
	}
	strs, err := cPointerArrayToSlice[C.qdb_string_t, C.qdb_string_t](raw, int64(n))
	if err != nil {
		return ColumnDataString{}, err
	}
	out := make([]string, n)
	for i, v := range strs {
		out[i] = C.GoStringN(v.data, C.int(v.length))
	}
	return newColumnDataString(out), nil
}
func GetColumnDataString(cd ColumnData) ([]string, error) {
	v, ok := cd.(*ColumnDataString)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataString: expected ColumnDataString, got %T", cd)
	}
	return v.xs, nil
}
func GetColumnDataStringUnsafe(cd ColumnData) []string {
	return (*ColumnDataString)(ifaceDataPtr(cd)).xs
}
