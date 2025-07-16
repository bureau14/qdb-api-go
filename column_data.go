// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: QuasarDB Go client API
// Types: Reader, Writer, ColumnData, HandleType
// Ex: h.NewReader(opts).FetchAll() → batch
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"time"
	"unsafe"
)

// ColumnData represents column data for reading/writing.
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

// ColumnDataInt64 stores int64 column data.
type ColumnDataInt64 struct {
	xs []int64
}

// Length returns the number of values.
func (cd *ColumnDataInt64) Length() int {
	return len(cd.xs)
}

// ValueType returns TsValueInt64.
func (cd *ColumnDataInt64) ValueType() TsValueType {
	return TsValueInt64
}

// EnsureCapacity pre-allocates space for n values.
func (cd *ColumnDataInt64) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets to empty.
func (cd *ColumnDataInt64) Clear() {
	cd.xs = make([]int64, 0)
}

// PinToC pins slice for C access.
func (cd *ColumnDataInt64) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	if len(cd.xs) == 0 {
		return nil, func() {}
	}
	base := &cd.xs[0]
	p.Pin(base)
	return unsafe.Pointer(base), func() {}
}

// appendData appends another ColumnDataInt64.
func (cd *ColumnDataInt64) appendData(data ColumnData) error {
	other, ok := data.(*ColumnDataInt64)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ColumnDataInt64, got %T", data)
	}

	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe appends without type check.
func (cd *ColumnDataInt64) appendDataUnsafe(data ColumnData) {
	other := (*ColumnDataInt64)(ifaceDataPtr(data))
	cd.xs = append(cd.xs, other.xs...)
}

// NewColumnDataInt64 creates int64 column data.
func NewColumnDataInt64(xs []int64) ColumnDataInt64 {
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
	// Using NewColumnDataInt64 centralizes ColumnDataInt64 construction logic.
	return NewColumnDataInt64(goSlice), nil
}

// GetColumnDataInt64 extracts []int64 from ColumnData.
func GetColumnDataInt64(cd ColumnData) ([]int64, error) {
	v, ok := cd.(*ColumnDataInt64)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataInt64: type mismatch, expected ColumnDataInt64, got %T", cd)
	}
	return v.xs, nil
}

// GetColumnDataInt64Unsafe extracts []int64 without type check.
func GetColumnDataInt64Unsafe(cd ColumnData) []int64 {
	return (*ColumnDataInt64)(ifaceDataPtr(cd)).xs
}

// --- Double -------------------------------------------------------

// ColumnDataDouble stores float64 column data.
type ColumnDataDouble struct{ xs []float64 }

// Length returns the number of values.
func (cd *ColumnDataDouble) Length() int {
	return len(cd.xs)
}

// ValueType returns TsValueDouble.
func (cd *ColumnDataDouble) ValueType() TsValueType {
	return TsValueDouble
}

// EnsureCapacity pre-allocates space for n values.
func (cd *ColumnDataDouble) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets to empty.
func (cd *ColumnDataDouble) Clear() {
	cd.xs = make([]float64, 0)
}

// appendData appends another ColumnDataDouble.
func (cd *ColumnDataDouble) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataDouble)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataDouble, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe appends without type check.
func (cd *ColumnDataDouble) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataDouble)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC pins slice for C access.
func (cd *ColumnDataDouble) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	if len(cd.xs) == 0 {
		return nil, func() {}
	}
	base := &cd.xs[0]
	p.Pin(base)
	return unsafe.Pointer(base), func() {}
}

// NewColumnDataDouble creates float64 column data.
func NewColumnDataDouble(xs []float64) ColumnDataDouble { return ColumnDataDouble{xs: xs} }

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
	return NewColumnDataDouble(goSlice), nil
}

// GetColumnDataDouble extracts []float64 from ColumnData.
func GetColumnDataDouble(cd ColumnData) ([]float64, error) {
	v, ok := cd.(*ColumnDataDouble)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataDouble: expected ColumnDataDouble, got %T", cd)
	}
	return v.xs, nil
}

// GetColumnDataDoubleUnsafe extracts []float64 without type check.
func GetColumnDataDoubleUnsafe(cd ColumnData) []float64 {
	return (*ColumnDataDouble)(ifaceDataPtr(cd)).xs
}

// --- Timestamp ----------------------------------------------------

// ColumnDataTimestamp stores timestamp column data.
type ColumnDataTimestamp struct{ xs []C.qdb_timespec_t }

// Length returns the number of values.
func (cd *ColumnDataTimestamp) Length() int {
	return len(cd.xs)
}

// ValueType returns TsValueTimestamp.
func (cd *ColumnDataTimestamp) ValueType() TsValueType {
	return TsValueTimestamp
}

// EnsureCapacity pre-allocates space for n values.
func (cd *ColumnDataTimestamp) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets to empty.
func (cd *ColumnDataTimestamp) Clear() {
	cd.xs = make([]C.qdb_timespec_t, 0)
}

// appendData appends another ColumnDataTimestamp.
func (cd *ColumnDataTimestamp) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataTimestamp)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataTimestamp, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe appends without type check.
func (cd *ColumnDataTimestamp) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataTimestamp)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC pins slice for C access.
func (cd *ColumnDataTimestamp) PinToC(p *runtime.Pinner, h HandleType) (unsafe.Pointer, func()) {
	if len(cd.xs) == 0 {
		return nil, func() {}
	}
	base := &cd.xs[0]
	p.Pin(base)
	return unsafe.Pointer(base), func() {}
}

// NewColumnDataTimestamp creates timestamp column data.
func NewColumnDataTimestamp(ts []time.Time) ColumnDataTimestamp {
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

// GetColumnDataTimestamp extracts []time.Time from ColumnData.
func GetColumnDataTimestamp(cd ColumnData) ([]time.Time, error) {
	xs, err := GetColumnDataTimestampNative(cd)
	if err != nil {
		return nil, err
	}

	return QdbTimespecSliceToTime(xs), nil
}

// GetColumnDataTimestampNative extracts []C.qdb_timespec_t from ColumnData.
func GetColumnDataTimestampNative(cd ColumnData) ([]C.qdb_timespec_t, error) {
	v, ok := cd.(*ColumnDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataTimestamp: expected ColumnDataTimestamp, got %T", cd)
	}
	return v.xs, nil
}

// GetColumnDataTimestampUnsafe extracts []time.Time without type check.
func GetColumnDataTimestampUnsafe(cd ColumnData) []time.Time {
	return QdbTimespecSliceToTime(GetColumnDataTimestampNativeUnsafe(cd))
}

// GetColumnDataTimestampNativeUnsafe extracts []C.qdb_timespec_t without type check.
func GetColumnDataTimestampNativeUnsafe(cd ColumnData) []C.qdb_timespec_t {
	return (*ColumnDataTimestamp)(ifaceDataPtr(cd)).xs
}

// --- Blob ---------------------------------------------------------

// ColumnDataBlob stores binary column data.
type ColumnDataBlob struct{ xs [][]byte }

// Length returns the number of values.
func (cd *ColumnDataBlob) Length() int {
	return len(cd.xs)
}

// ValueType returns TsValueBlob.
func (cd *ColumnDataBlob) ValueType() TsValueType {
	return TsValueBlob
}

// EnsureCapacity pre-allocates space for n values.
func (cd *ColumnDataBlob) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets to empty.
func (cd *ColumnDataBlob) Clear() {
	cd.xs = make([][]byte, 0)
}

// appendData appends another ColumnDataBlob.
func (cd *ColumnDataBlob) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataBlob)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataBlob, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe appends without type check.
func (cd *ColumnDataBlob) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataBlob)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC builds a C envelope and pins each []byte in cd.xs.
//
// Decision rationale:
//
//	– Zero-copy path avoids duplicating potentially large blobs.
//	– release() frees only the envelope; Go memory is unpinned automatically.
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
			p.Pin(&b[0])                             // keep backing array immovable
			slice[i].content = unsafe.Pointer(&b[0]) // direct pointer into Go memory
			slice[i].content_length = C.qdb_size_t(len(b))
		}
	}

	// Always return a valid release closure (no-op for the blob contents)
	release := func() {
		qdbRelease(h, arr) // free only the envelope allocated above
	}

	return unsafe.Pointer(arr), release
}

// NewColumnDataBlob creates binary column data.
func NewColumnDataBlob(xs [][]byte) ColumnDataBlob { return ColumnDataBlob{xs: xs} }

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
	return NewColumnDataBlob(out), nil
}

// GetColumnDataBlob extracts [][]byte from ColumnData.
func GetColumnDataBlob(cd ColumnData) ([][]byte, error) {
	v, ok := cd.(*ColumnDataBlob)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataBlob: expected ColumnDataBlob, got %T", cd)
	}
	return v.xs, nil
}

// GetColumnDataBlobUnsafe extracts [][]byte without type check.
func GetColumnDataBlobUnsafe(cd ColumnData) [][]byte {
	return (*ColumnDataBlob)(ifaceDataPtr(cd)).xs
}

// --- String -------------------------------------------------------

// ColumnDataString stores text column data.
type ColumnDataString struct{ xs []string }

// Length returns the number of values.
func (cd *ColumnDataString) Length() int {
	return len(cd.xs)
}

// ValueType returns TsValueString.
func (cd *ColumnDataString) ValueType() TsValueType {
	return TsValueString
}

// EnsureCapacity pre-allocates space for n values.
func (cd *ColumnDataString) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

// Clear resets to empty.
func (cd *ColumnDataString) Clear() {
	cd.xs = make([]string, 0)
}

// appendData appends another ColumnDataString.
func (cd *ColumnDataString) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataString)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataString, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}

// appendDataUnsafe appends without type check.
func (cd *ColumnDataString) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataString)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}

// PinToC builds a C envelope and pins each string in cd.xs.
//
// Decision rationale:
//
//	– Zero-copy path avoids duplicating potentially large strings.
//	– qdb_string_t uses explicit length, so NUL-termination is not required.
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
		dataPtr := unsafe.StringData(*s) // pointer to first byte
		p.Pin(dataPtr)                   // keep the bytes immovable

		slice[i].data = (*C.char)(unsafe.Pointer(dataPtr))
		slice[i].length = C.qdb_size_t(len(*s))
	}

	// envelope itself must be freed after the C call
	release := func() { qdbRelease(h, arr) }

	return unsafe.Pointer(arr), release
}

// NewColumnDataString creates string column data.
func NewColumnDataString(xs []string) ColumnDataString { return ColumnDataString{xs: xs} }

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
	return NewColumnDataString(out), nil
}

// GetColumnDataString extracts []string from ColumnData.
func GetColumnDataString(cd ColumnData) ([]string, error) {
	v, ok := cd.(*ColumnDataString)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataString: expected ColumnDataString, got %T", cd)
	}
	return v.xs, nil
}

// GetColumnDataStringUnsafe extracts []string without type check.
func GetColumnDataStringUnsafe(cd ColumnData) []string {
	return (*ColumnDataString)(ifaceDataPtr(cd)).xs
}
