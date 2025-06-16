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

	// CopyToC copies the internal data slice into a C-allocated buffer and returns
	// a pointer to that buffer. Caller is responsible for releasing it.
	CopyToC(h HandleType) unsafe.Pointer
}

// Int64
type ColumnDataInt64 struct {
	xs []int64
}

func (cd *ColumnDataInt64) Data() []int64 {
	return cd.xs
}

func (cd *ColumnDataInt64) Length() int {
	return len(cd.xs)
}

func (cd *ColumnDataInt64) ValueType() TsValueType {
	return TsValueInt64
}

func (cd *ColumnDataInt64) EnsureCapacity(n int) {
	cd.xs = sliceEnsureCapacity(cd.xs, n)
}

func (cd *ColumnDataInt64) Clear() {
	cd.xs = make([]int64, 0)
}

func (cd *ColumnDataInt64) CopyToC(h HandleType) unsafe.Pointer {
	ptr, err := qdbAllocAndCopyBuffer[int64, C.qdb_int_t](h, cd.xs)
	if err != nil {
		panic(err)
	}

	return unsafe.Pointer(ptr)
}

func (cd *ColumnDataInt64) appendData(data ColumnData) error {
	other, ok := data.(*ColumnDataInt64)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ColumnDataInt64, got %T", data)
	}

	cd.xs = append(cd.xs, other.xs...)
	return nil
}

func (cd *ColumnDataInt64) appendDataUnsafe(data ColumnData) {
	other := (*ColumnDataInt64)(ifaceDataPtr(data))
	cd.xs = append(cd.xs, other.xs...)
}

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

func (cd *ColumnDataDouble) Data() []float64        { return cd.xs }
func (cd *ColumnDataDouble) Length() int            { return len(cd.xs) }
func (cd *ColumnDataDouble) ValueType() TsValueType { return TsValueDouble }
func (cd *ColumnDataDouble) EnsureCapacity(n int)   { cd.xs = sliceEnsureCapacity(cd.xs, n) }
func (cd *ColumnDataDouble) Clear()                 { cd.xs = make([]float64, 0) }
func (cd *ColumnDataDouble) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataDouble)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataDouble, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}
func (cd *ColumnDataDouble) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataDouble)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}
func (cd *ColumnDataDouble) CopyToC(h HandleType) unsafe.Pointer {
	ptr, err := qdbAllocAndCopyBuffer[float64, C.double](h, cd.xs)
	if err != nil {
		panic(err)
	}
	return unsafe.Pointer(ptr)
}
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

type ColumnDataTimestamp struct{ xs []time.Time }

func (cd *ColumnDataTimestamp) Data() []time.Time      { return cd.xs }
func (cd *ColumnDataTimestamp) Length() int            { return len(cd.xs) }
func (cd *ColumnDataTimestamp) ValueType() TsValueType { return TsValueTimestamp }
func (cd *ColumnDataTimestamp) EnsureCapacity(n int)   { cd.xs = sliceEnsureCapacity(cd.xs, n) }
func (cd *ColumnDataTimestamp) Clear()                 { cd.xs = make([]time.Time, 0) }
func (cd *ColumnDataTimestamp) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataTimestamp)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataTimestamp, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}
func (cd *ColumnDataTimestamp) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataTimestamp)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}
func (cd *ColumnDataTimestamp) CopyToC(h HandleType) unsafe.Pointer {
	tsSlice := TimeSliceToQdbTimespec(cd.xs)
	ptr, err := qdbAllocAndCopyBuffer[C.qdb_timespec_t, C.qdb_timespec_t](h, tsSlice)
	if err != nil {
		panic(err)
	}
	return unsafe.Pointer(ptr)
}
func newColumnDataTimestamp(xs []time.Time) ColumnDataTimestamp { return ColumnDataTimestamp{xs: xs} }
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
	specSlice, err := cPointerArrayToSlice[C.qdb_timespec_t, C.qdb_timespec_t](raw, int64(n))
	if err != nil {
		return ColumnDataTimestamp{}, err
	}
	goTimes := QdbTimespecSliceToTime(specSlice)
	return newColumnDataTimestamp(goTimes), nil
}
func GetColumnDataTimestamp(cd ColumnData) ([]time.Time, error) {
	v, ok := cd.(*ColumnDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetColumnDataTimestamp: expected ColumnDataTimestamp, got %T", cd)
	}
	return v.xs, nil
}
func GetColumnDataTimestampUnsafe(cd ColumnData) []time.Time {
	return (*ColumnDataTimestamp)(ifaceDataPtr(cd)).xs
}

// --- Blob ---------------------------------------------------------

type ColumnDataBlob struct{ xs [][]byte }

func (cd *ColumnDataBlob) Data() [][]byte         { return cd.xs }
func (cd *ColumnDataBlob) Length() int            { return len(cd.xs) }
func (cd *ColumnDataBlob) ValueType() TsValueType { return TsValueBlob }
func (cd *ColumnDataBlob) EnsureCapacity(n int)   { cd.xs = sliceEnsureCapacity(cd.xs, n) }
func (cd *ColumnDataBlob) Clear()                 { cd.xs = make([][]byte, 0) }
func (cd *ColumnDataBlob) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataBlob)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataBlob, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}
func (cd *ColumnDataBlob) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataBlob)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}
func (cd *ColumnDataBlob) CopyToC(h HandleType) unsafe.Pointer {
	count := len(cd.xs)
	arr, err := qdbAllocBuffer[C.qdb_blob_t](h, count)
	if err != nil {
		panic(err)
	}
	slice := unsafe.Slice(arr, count)
	for i, b := range cd.xs {
		if len(b) > 0 {
			ptr, err := qdbAllocAndCopyBytes(h, b)
			if err != nil {
				panic(err)
			}
			slice[i].content = ptr
			slice[i].content_length = C.qdb_size_t(len(b))
		}
	}
	return unsafe.Pointer(arr)
}
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

func (cd *ColumnDataString) Data() []string         { return cd.xs }
func (cd *ColumnDataString) Length() int            { return len(cd.xs) }
func (cd *ColumnDataString) ValueType() TsValueType { return TsValueString }
func (cd *ColumnDataString) EnsureCapacity(n int)   { cd.xs = sliceEnsureCapacity(cd.xs, n) }
func (cd *ColumnDataString) Clear()                 { cd.xs = make([]string, 0) }
func (cd *ColumnDataString) appendData(d ColumnData) error {
	other, ok := d.(*ColumnDataString)
	if !ok {
		return fmt.Errorf("appendData: expected ColumnDataString, got %T", d)
	}
	cd.xs = append(cd.xs, other.xs...)
	return nil
}
func (cd *ColumnDataString) appendDataUnsafe(d ColumnData) {
	other := (*ColumnDataString)(ifaceDataPtr(d))
	cd.xs = append(cd.xs, other.xs...)
}
func (cd *ColumnDataString) CopyToC(h HandleType) unsafe.Pointer {
	count := len(cd.xs)
	arr, err := qdbAllocBuffer[C.qdb_string_t](h, count)
	if err != nil {
		panic(err)
	}
	slice := unsafe.Slice(arr, count)
	for i, s := range cd.xs {
		if len(s) > 0 {
			cstr, err := qdbCopyString(h, s)
			if err != nil {
				panic(err)
			}
			slice[i].data = cstr
			slice[i].length = C.qdb_size_t(len(s))
		}
	}
	return unsafe.Pointer(arr)
}
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
