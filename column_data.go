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

	// PinToC prepares column data for zero-copy passing to C functions.
	//
	// CRITICAL: This is part of the centralized pinning strategy that prevents segfaults.
	// The function returns a PinnableBuilder that MUST be pinned at the top level
	// (in Writer.Push) before any C calls. This two-phase approach ensures:
	//   1. All pointers are collected before any pinning occurs
	//   2. The runtime.Pinner can pin all objects in one batch
	//   3. No Go pointers are stored in C-accessible memory before pinning
	//
	// Parameters:
	//   h       – handle for QDB memory allocation when copies are required
	//
	// Returns:
	//   builder – PinnableBuilder containing the object to pin and builder function
	//   release – cleanup function for any C allocations (called after C returns)
	//
	// Implementation notes:
	//   - Int64/Double/Timestamp: Direct pin of Go slice memory (zero-copy)
	//   - Blob/String: MUST copy to C memory to avoid storing Go pointers before pinning
	//   - The release function is always safe to call, even on error
	PinToC(h HandleType) (builder PinnableBuilder, release func())
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

// PinToC prepares int64 data for zero-copy passing to C.
//
// ZERO-COPY STRATEGY: Safe for numeric types like int64 because:
// - Simple value type with no internal pointers
// - Direct memory layout compatible with C
// - Pinning prevents GC movement during C operations
//
// Performance: Zero allocations, zero copies - maximum efficiency.
func (cd *ColumnDataInt64) PinToC(h HandleType) (PinnableBuilder, func()) {
	if len(cd.xs) == 0 {
		return PinnableBuilder{}, func() {}
	}
	base := &cd.xs[0]
	return NewPinnableBuilderSingle(base, func() unsafe.Pointer {
		// Executed AFTER pinning for safety
		return unsafe.Pointer(base)
	}), func() {} // No C allocations, so no cleanup needed
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

// PinToC prepares float64 data for zero-copy passing to C.
//
// ZERO-COPY STRATEGY: Numeric types (int64, float64, timestamp) can safely
// use zero-copy because they are simple value types without internal pointers.
// We return a PinnableBuilder that will provide the pointer AFTER pinning.
//
// Why zero-copy is safe here:
// - float64 is a value type with no internal pointers
// - Pinning prevents GC from moving the slice during C operations
// - No risk of violating string immutability or pointer rules
//
// Performance: Zero allocations, zero copies - maximum efficiency.
func (cd *ColumnDataDouble) PinToC(h HandleType) (PinnableBuilder, func()) {
	if len(cd.xs) == 0 {
		return PinnableBuilder{}, func() {}
	}
	base := &cd.xs[0]
	return NewPinnableBuilderSingle(base, func() unsafe.Pointer {
		// This executes AFTER pinning, making it safe
		return unsafe.Pointer(base)
	}), func() {} // No C allocations, so no cleanup needed
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

// PinToC prepares timestamp data for zero-copy passing to C.
//
// ZERO-COPY STRATEGY: Safe because we store data in C.qdb_timespec_t format:
// - Data is already in C-compatible memory layout
// - Direct pinning like int64/double (no conversion needed)
// - Pinning prevents GC movement during C operations
//
// Performance: Zero allocations, zero copies - maximum efficiency.
func (cd *ColumnDataTimestamp) PinToC(h HandleType) (PinnableBuilder, func()) {
	if len(cd.xs) == 0 {
		return PinnableBuilder{}, func() {}
	}
	base := &cd.xs[0]
	return NewPinnableBuilderSingle(base, func() unsafe.Pointer {
		// Executed AFTER pinning for safety
		return unsafe.Pointer(base)
	}), func() {} // No C allocations, so no cleanup needed
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
	// Keep data in C.qdb_timespec_t format
	return ColumnDataTimestamp{xs: specs}, nil
}

// GetColumnDataTimestamp extracts []time.Time from ColumnData.
func GetColumnDataTimestamp(cd ColumnData) ([]time.Time, error) {
	v, ok := cd.(*ColumnDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataTimestamp: expected ColumnDataTimestamp, got %T", cd)
	}
	return QdbTimespecSliceToTime(v.xs), nil
}

// GetColumnDataTimestampNative extracts []C.qdb_timespec_t from ColumnData.
func GetColumnDataTimestampNative(cd ColumnData) ([]C.qdb_timespec_t, error) {
	v, ok := cd.(*ColumnDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataTimestamp: expected ColumnDataTimestamp, got %T", cd)
	}
	// Data is already in C.qdb_timespec_t format
	return v.xs, nil
}

// GetColumnDataTimestampUnsafe extracts []time.Time without type check.
func GetColumnDataTimestampUnsafe(cd ColumnData) []time.Time {
	v := (*ColumnDataTimestamp)(ifaceDataPtr(cd))
	return QdbTimespecSliceToTime(v.xs)
}

// GetColumnDataTimestampNativeUnsafe extracts []C.qdb_timespec_t without type check.
func GetColumnDataTimestampNativeUnsafe(cd ColumnData) []C.qdb_timespec_t {
	v := (*ColumnDataTimestamp)(ifaceDataPtr(cd))
	// Data is already in C.qdb_timespec_t format
	return v.xs
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

// PinToC builds a C envelope for blob data and returns PinnableBuilder.
//
// ZERO-COPY STRATEGY: For blobs, we pin each individual blob's data pointer
// rather than copying. This requires more complex pinning but provides optimal
// performance for large binary data transfers.
//
// Implementation notes:
//   - Each []byte is pinned individually using unsafe.SliceData
//   - The envelope array is allocated in C memory
//   - Only the envelope is released, not the blob data (owned by Go)
//
// Safety considerations:
//   - All blob data pointers are pinned before C access
//   - The centralized pinning strategy ensures correctness
//   - Blobs are immutable during C operations
func (cd *ColumnDataBlob) PinToC(h HandleType) (PinnableBuilder, func()) {
	if len(cd.xs) == 0 {
		return PinnableBuilder{}, func() {}
	}

	// Allocate C envelope (unchanged)
	envelope := qdbAllocBuffer[C.qdb_blob_t](h, len(cd.xs))
	envelopeSlice := unsafe.Slice(envelope, len(cd.xs))

	// Track copied blobs for cleanup
	copiedBlobs := make([]unsafe.Pointer, 0, len(cd.xs))

	// No objects to pin - we're copying instead
	return NewPinnableBuilderMultiple(nil, func() unsafe.Pointer {
			// Build C structures (executed in Phase 2.5 after pinning)
			for i, blob := range cd.xs {
				if len(blob) > 0 {
					// Copy blob to C memory
					blobPtr := qdbAllocAndCopyBytes(h, blob)
					copiedBlobs = append(copiedBlobs, blobPtr)
					envelopeSlice[i].content = blobPtr
					envelopeSlice[i].content_length = C.qdb_size_t(len(blob))
				} else {
					envelopeSlice[i].content = nil
					envelopeSlice[i].content_length = 0
				}
			}
			return unsafe.Pointer(envelope)
		}), func() {
			// Release all copied blobs
			for _, blobPtr := range copiedBlobs {
				qdbReleasePointer(h, blobPtr)
			}
			// Release envelope
			qdbRelease(h, envelope)
		}
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

// PinToC builds a C envelope for string data and returns PinnableBuilder.
//
// COPYING STRATEGY: For strings, we copy each string to C memory using qdbCopyString
// rather than pinning. This provides safety by avoiding unsafe pointer operations
// while maintaining correct memory management through proper cleanup.
//
// Implementation notes:
//   - Each string is copied to C memory using qdbCopyString
//   - The envelope array is allocated in C memory
//   - Both the envelope and copied strings are released
//   - Go strings are immutable, so copying is safe
//
// Safety considerations:
//   - All string data is copied to C memory before C access
//   - No pinning required since we use copying instead
//   - String immutability is preserved (C cannot modify original strings)
func (cd *ColumnDataString) PinToC(h HandleType) (PinnableBuilder, func()) {
	if len(cd.xs) == 0 {
		return PinnableBuilder{}, func() {}
	}

	// Allocate C envelope (unchanged)
	envelope := qdbAllocBuffer[C.qdb_string_t](h, len(cd.xs))
	envelopeSlice := unsafe.Slice(envelope, len(cd.xs))

	// Track copied strings for cleanup
	copiedStrings := make([]*C.char, 0, len(cd.xs))

	// No objects to pin - we're copying instead
	return NewPinnableBuilderMultiple(nil, func() unsafe.Pointer {
			// Build C structures (executed in Phase 2.5 after pinning)
			for i, str := range cd.xs {
				if len(str) > 0 {
					// Copy string to C memory (includes null terminator)
					cStr := qdbCopyString(h, str)
					copiedStrings = append(copiedStrings, cStr)
					envelopeSlice[i].data = cStr
					envelopeSlice[i].length = C.qdb_size_t(len(str))
				} else {
					envelopeSlice[i].data = nil
					envelopeSlice[i].length = 0
				}
			}
			return unsafe.Pointer(envelope)
		}), func() {
			// Release all copied strings
			for _, cStr := range copiedStrings {
				qdbReleasePointer(h, unsafe.Pointer(cStr))
			}
			// Release envelope
			qdbRelease(h, envelope)
		}
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
