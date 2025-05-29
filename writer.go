// Package qdb provides an api to a quasardb server
package qdb

/*
        #include <string.h> // for memcpy
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"
)

type writerError struct {
	s string
}

func (e *writerError) Error() string {
	return e.s
}

func New(text string) error {
	return &writerError{text}
}

type WriterData interface {
	// Possibly some methods, but often empty
	valueType() TsValueType

	// Returns opaque pointer to internal data array, intended to be set on
	// the `qdb_exp_batch_push_column_t` `data` field.
	ptr() unsafe.Pointer

	// Release any C allocated buffers managed
	releaseNative(h HandleType) error
}

// Int64
type WriterDataInt64 struct {
	xs *C.qdb_int_t
}

// NewWriterDataInt64 constructs a WriterDataInt64 instance, copying the input slice
// into memory allocated by the QDB C API. The allocated memory must be released with C.qdb_release().
func NewWriterDataInt64(h HandleType, xs []int64) (WriterData, error) {
	ptr, err := qdbAllocAndCopy[int64, C.qdb_int_t](h, xs)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate and copy int64 data: %w", err)
	}

	return &WriterDataInt64{
		xs: ptr,
	}, nil
}

func (wd *WriterDataInt64) valueType() TsValueType {
	return TsValueInt64
}

func (wd *WriterDataInt64) ptr() unsafe.Pointer {
	return unsafe.Pointer(wd.xs)
}

func (wd *WriterDataInt64) releaseNative(h HandleType) error {
	// no C-managed memory allocated
	if wd.xs == nil {
		return fmt.Errorf("Internal error: no value array for WriterDataInt64")
	}

	qdbRelease(h, wd.xs)
	wd.xs = nil

	return nil
}

// Double
type WriterDataDouble struct {
	xs *C.double
}

// NewWriterDataDouble constructs a WriterDataDouble instance, copying the input slice
// into memory allocated by the QDB C API. The allocated memory must be released with C.qdb_release().
func NewWriterDataDouble(h HandleType, xs []float64) (WriterData, error) {
	ptr, err := qdbAllocAndCopy[float64, C.double](h, xs)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate and copy double data: %w", err)
	}

	return &WriterDataDouble{
		xs: ptr,
	}, nil
}

func (wd WriterDataDouble) valueType() TsValueType {
	return TsValueDouble
}

func (wd *WriterDataDouble) ptr() unsafe.Pointer {
	return unsafe.Pointer(wd.xs)
}

// Releases all C-allocated memory, which has been allocated by the QDB C API.
func (wd *WriterDataDouble) releaseNative(h HandleType) error {
	if wd.xs == nil {
		return fmt.Errorf("Internal error: no value array for WriterDataDouble")
	}

	qdbRelease(h, wd.xs)
	wd.xs = nil

	return nil
}

// Timestamp
type WriterDataTimestamp struct {
	xs *C.qdb_timespec_t
}

// NewWriterDataTimestampFromTimespec constructs WriterDataTimestamp from a slice of C.qdb_timespec_t,
// copying the data into a newly allocated buffer via qdbAllocAndCopy.
func NewWriterDataTimestampFromTimespec(h HandleType, xs []C.qdb_timespec_t) (WriterData, error) {
	ptr, err := qdbAllocAndCopy[C.qdb_timespec_t, C.qdb_timespec_t](h, xs)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate and copy timestamp data: %w", err)
	}

	return &WriterDataTimestamp{
		xs: ptr,
	}, nil
}

// Constructor for timestamp data array
func NewWriterDataTimestamp(h HandleType, xs []time.Time) (WriterData, error) {
	return NewWriterDataTimestampFromTimespec(h, TimeSliceToQdbTimespec(xs))
}

func (_ WriterDataTimestamp) valueType() TsValueType {
	return TsValueTimestamp
}

func (wd *WriterDataTimestamp) ptr() unsafe.Pointer {
	return unsafe.Pointer(wd.xs)
}

func (wd *WriterDataTimestamp) releaseNative(h HandleType) error {
	if wd.xs == nil {
		return fmt.Errorf("Internal error: no value array for WriterDataTimestamp")
	}

	qdbRelease(h, wd.xs)

	wd.xs = nil

	return nil
}

// Blob
type WriterDataBlob struct {
	xs      *C.qdb_blob_t // array of blob structs
	content *byte         // content array for *all* blobs, allocated as one large slice
}

// Constructor for blob data array
func NewWriterDataBlob(h HandleType, xs [][]byte) (WriterData, error) {
	// NewWriterDataBlob allocates a single large contiguous memory region for all blob contents
	// to reduce memory fragmentation and improve allocation performance.
	//
	// Each blob occupies a fixed-length segment equal to the largest blob size.
	// Smaller blobs do not fully utilize their allocated segment.
	//
	// Both the blob descriptor structs and contents are explicitly allocated
	// and must be explicitly released using C.qdb_release() after usage.

	count := len(xs)
	if count == 0 {
		return nil, fmt.Errorf("no blobs provided")
	}

	// Step 1: allocate qdb_blob_t array
	blobArrayPtr, err := qdbAllocBuffer[C.qdb_blob_t](h, count)
	if err != nil {
		return nil, fmt.Errorf("blob struct array allocation failed: %v", err)
	}

	// Step 2: find largest blob size for uniform allocation
	maxBlobSize := 0
	for _, v := range xs {
		if len(v) > maxBlobSize {
			maxBlobSize = len(v)
		}
	}

	// Step 3: allocate single slab of memory to hold all blobs
	var contentPtr *byte
	var rawPtr unsafe.Pointer
	if maxBlobSize > 0 {
		totalContentSize := maxBlobSize * count
		rawPtr, err = qdbAllocBytes(h, totalContentSize)
		if err != nil {
			// Cleanup previous allocation before returning
			qdbRelease(h, blobArrayPtr)
			return nil, fmt.Errorf("blob content buffer allocation failed: %v", err)
		}
		contentPtr = (*byte)(rawPtr)
	}

	// Step 4: copy blob contents into single contiguous allocation
	blobSlice := unsafe.Slice(blobArrayPtr, count)
	for i, v := range xs {
		var destPtr unsafe.Pointer

		if len(v) > 0 {
			// Calculate the correct offset within the large contiguous slab
			destPtr = unsafe.Pointer(uintptr(rawPtr) + uintptr(i*maxBlobSize))
			C.memcpy(destPtr, unsafe.Pointer(&v[0]), C.size_t(len(v)))
			blobSlice[i].content = destPtr
			blobSlice[i].content_length = C.qdb_size_t(len(v))
		} else {
			// Explicitly handle empty blobs
			blobSlice[i].content = nil
			blobSlice[i].content_length = 0
		}
	}

	return &WriterDataBlob{
		xs:      blobArrayPtr,
		content: contentPtr,
	}, nil
}

func (cd WriterDataBlob) valueType() TsValueType {
	return TsValueBlob
}

func (wd *WriterDataBlob) ptr() unsafe.Pointer {
	return unsafe.Pointer(wd.xs)
}

func (wd *WriterDataBlob) releaseNative(h HandleType) error {
	if wd.xs == nil {
		return fmt.Errorf("Internal error: no value array for WriterDataBlob")
	}

	qdbRelease(h, wd.xs)
	if wd.content != nil {
		qdbRelease(h, wd.content)
	}

	wd.xs = nil
	wd.content = nil

	return nil
}

// String
type WriterDataString struct {
	xs      *C.qdb_string_t // array of string structs
	content *C.char         // content array for *all* strings, allocated as one large slice
}

// Constructor for string data array
func NewWriterDataString(h HandleType, xs []string) (WriterData, error) {
	// NewWriterDataBlob allocates a single large contiguous memory region for all blob contents
	// to reduce memory fragmentation and improve allocation performance.
	//
	// Each blob occupies a fixed-length segment equal to the largest blob size.
	// Smaller blobs do not fully utilize their allocated segment.
	//
	// Both the blob descriptor structs and contents are explicitly allocated
	// and must be explicitly released using C.qdb_release() after usage.

	count := len(xs)
	if count == 0 {
		return nil, fmt.Errorf("no blobs provided")
	}

	// Step 1: allocate qdb_string_t array
	stringArrayPtr, err := qdbAllocBuffer[C.qdb_string_t](h, count)
	if err != nil {
		return nil, fmt.Errorf("blob struct array allocation failed: %v", err)
	}

	// Step 2: find largest blob size for uniform allocation
	maxStringSize := 0
	for _, v := range xs {
		if len(v) > maxStringSize {
			// Max size we need to allocate is the length of the string + 1, for
			// the null terminator.
			//
			// QuasarDB C API doesn't strictly require it, but it is better to be
			// safe.
			maxStringSize = len(v) + 1
		}
	}

	// Step 3: allocate single slab of memory to hold all strings
	var contentPtr *C.char
	var rawPtr unsafe.Pointer
	if maxStringSize > 0 {
		totalContentSize := maxStringSize * count
		rawPtr, err = qdbAllocBytes(h, totalContentSize)
		if err != nil {
			// Cleanup previous allocation before returning
			qdbRelease(h, stringArrayPtr)
			return nil, fmt.Errorf("blob content buffer allocation failed: %v", err)
		}
		contentPtr = (*C.char)(rawPtr)
	}

	// Step 4: copy blob contents into single contiguous allocation
	stringSlice := unsafe.Slice(stringArrayPtr, count)
	for i, v := range xs {
		var destPtr unsafe.Pointer

		if len(v) > 0 {
			// Calculate the correct offset within the large contiguous slab
			destPtr = unsafe.Pointer(uintptr(rawPtr) + uintptr(i*maxStringSize))
			C.memcpy(destPtr, unsafe.Pointer(unsafe.StringData(v)), C.size_t(len(v)))

			// Don't forget to write the \0 null terminator.
			*(*byte)(unsafe.Pointer(uintptr(destPtr) + uintptr(len(v)))) = 0

			stringSlice[i].data = (*C.char)(destPtr)
			stringSlice[i].length = C.qdb_size_t(len(v))
		} else {
			// Explicitly handle empty strings
			stringSlice[i].data = nil
			stringSlice[i].length = 0
		}
	}

	return &WriterDataString{
		xs:      stringArrayPtr,
		content: contentPtr,
	}, nil
}

func (cd WriterDataString) valueType() TsValueType {
	return TsValueString
}

func (wd *WriterDataString) ptr() unsafe.Pointer {
	return unsafe.Pointer(wd.xs)
}

func (wd *WriterDataString) releaseNative(h HandleType) error {
	if wd.xs == nil {
		return fmt.Errorf("Internal error: no value array for WriterDataString")
	}

	qdbRelease(h, wd.xs)
	if wd.content != nil {
		qdbRelease(h, wd.content)
	}

	wd.xs = nil
	wd.content = nil

	return nil
}

// Metadata we need to represent a single column.
type WriterColumn struct {
	ColumnName string
	ColumnType TsColumnType
}

// Single table to be provided to the batch writer.
type WriterTable struct {
	TableName *C.char

	// All arrays are guaranteed to be of lenght `rowCount`. This means specifically
	// the `idx` parameter and all Writerdata value arrays within `data`.
	rowCount int

	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []WriterColumn

	// An index that enables looking up of a column's offset within the table by its name.
	columnOffsetByName map[string]int

	// The index, can not contain null values
	idx *C.qdb_timespec_t

	// Value arrays to write for each column.
	data []WriterData
}

type WriterPushMode C.qdb_exp_batch_push_mode_t

const (
	WriterPushModeTransactional WriterPushMode = C.qdb_exp_batch_push_transactional
	WriterPushModeFast          WriterPushMode = C.qdb_exp_batch_push_fast
	WriterPushModeAsync         WriterPushMode = C.qdb_exp_batch_push_async
)

type WriterDeduplicationMode C.qdb_exp_batch_deduplication_mode_t

const (
	WriterDeduplicationModeDisabled WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_disabled
	WriterDeduplicationModeDrop     WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_drop
	WriterDeduplicationModeUpsert   WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_upsert
)

type WriterOptions struct {
	pushMode             WriterPushMode
	dropDuplicates       bool
	dropDuplicateColumns []string
	dedupMode            WriterDeduplicationMode
}

type Writer struct {
	options WriterOptions
	tables  map[string]WriterTable
}

func ifaceDataPtr(i interface{}) unsafe.Pointer {
	// internal helper in your package (private, defined once)
	type iface struct {
		tab  unsafe.Pointer
		data unsafe.Pointer
	}

	return (*iface)(unsafe.Pointer(&i)).data
}

// GetInt64Array safely converts WriterData to *WriterDataInt64.
//
// Returns an error if data is not of type Int64.
func GetInt64Array(x WriterData) (*WriterDataInt64, error) {
	v, ok := x.(*WriterDataInt64)
	if !ok {
		return nil, fmt.Errorf("GetInt64Array: type mismatch, expected WriterDataInt64, got %T", x)
	}
	return v, nil
}

// GetInt64ArrayUnsafe is an unsafe version of GetInt64Array. Undefined behavior occurs when
// invoked on the incorrect type.
func GetInt64ArrayUnsafe(x WriterData) *WriterDataInt64 {
	return (*WriterDataInt64)(ifaceDataPtr(x))
}

// GetDoubleArray safely converts WriterData to *WriterDataDouble.
//
// Returns an error if data is not of type Double.
func GetDoubleArray(x WriterData) (*WriterDataDouble, error) {
	v, ok := x.(*WriterDataDouble)
	if !ok {
		return nil, fmt.Errorf("GetDoubleArray: type mismatch, expected WriterDataDouble, got %T", x)
	}
	return v, nil
}

// GetDoubleArrayUnsafe is an unsafe version of GetDoubleArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetDoubleArrayUnsafe(x WriterData) *WriterDataDouble {
	return (*WriterDataDouble)(ifaceDataPtr(x))
}

// GetTimestampArray safely converts WriterData to *WriterDataTimestamp.
//
// Returns an error if data is not of type Timestamp.
func GetTimestampArray(x WriterData) (*WriterDataTimestamp, error) {
	v, ok := x.(*WriterDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetTimestampArray: type mismatch, expected WriterDataTimestamp, got %T", x)
	}
	return v, nil
}

// GetTimestampArrayUnsafe is an unsafe version of GetTimestampArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetTimestampArrayUnsafe(x WriterData) *WriterDataTimestamp {
	return (*WriterDataTimestamp)(ifaceDataPtr(x))
}

// GetStringArray safely converts WriterData to *WriterDataString.
//
// Returns an error if data is not of type String.
func GetStringArray(x WriterData) (*WriterDataString, error) {
	v, ok := x.(*WriterDataString)
	if !ok {
		return nil, fmt.Errorf("GetStringArray: type mismatch, expected WriterDataString, got %T", x)
	}
	return v, nil
}

// GetStringArrayUnsafe is an unsafe version of GetStringArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetStringArrayUnsafe(x WriterData) *WriterDataString {
	return (*WriterDataString)(ifaceDataPtr(x))
}

// GetBlobArray safely converts WriterData to *WriterDataBlob.
//
// Returns an error if data is not of type Blob.
func GetBlobArray(x WriterData) (*WriterDataBlob, error) {
	v, ok := x.(*WriterDataBlob)
	if !ok {
		return nil, fmt.Errorf("GetBlobArray: type mismatch, expected WriterDataBlob, got %T", x)
	}
	return v, nil
}

// GetBlobArrayUnsafe is an unsafe version of GetBlobArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetBlobArrayUnsafe(x WriterData) *WriterDataBlob {
	return (*WriterDataBlob)(ifaceDataPtr(x))
}

func NewWriterTable(h HandleType, t string, cols []WriterColumn) (WriterTable, error) {
	// Pre-allocate our data array, which has exactly 1 entry for every column we intend to write.
	data := make([]WriterData, len(cols))

	// Build indexes
	columnInfoByOffset := make([]WriterColumn, len(cols))
	columnOffsetByName := make(map[string]int)

	for i, col := range cols {
		columnInfoByOffset[i] = col
		columnOffsetByName[col.ColumnName] = i
	}

	tableName, err := qdbCopyString(h, t)
	if err != nil {
		return WriterTable{}, err
	}

	// An index of column offset to name
	return WriterTable{tableName, 0, columnInfoByOffset, columnOffsetByName, nil, data}, nil
}

func (t *WriterTable) GetNameNative() *C.char {
	return t.TableName
}

func (t *WriterTable) GetName() string {
	return C.GoString(t.GetNameNative())
}

// Sets the index into the table
func (t *WriterTable) SetIndexFromNativeUnsafe(idx *C.qdb_timespec_t, rowCount int) error {
	if t.idx != nil {
		return fmt.Errorf("Index already set, cannot set twice")
	}

	if t.rowCount != 0 {
		return fmt.Errorf("Internal error: index not set, but rowCount is non-zero")
	}

	t.idx = idx
	t.rowCount = rowCount

	return nil
}

func (t *WriterTable) SetIndexFromNative(h HandleType, idx []C.qdb_timespec_t) error {
	ptr, err := qdbAllocAndCopy[C.qdb_timespec_t, C.qdb_timespec_t](h, idx)
	if err != nil {
		return fmt.Errorf("failed to allocate and copy index data: %w", err)
	}

	return t.SetIndexFromNativeUnsafe(ptr, len(idx))
}
func (t *WriterTable) SetIndex(h HandleType, idx []time.Time) error {
	return t.SetIndexFromNative(h, TimeSliceToQdbTimespec(idx))
}

func (t *WriterTable) GetIndexAsNative() []C.qdb_timespec_t {
	if t.idx == nil || t.rowCount <= 0 {
		return nil
	}

	// Create a slice backed by the C array
	return unsafe.Slice(t.idx, t.rowCount)
}

func (t *WriterTable) GetIndex() []time.Time {
	return QdbTimespecSliceToTime(t.GetIndexAsNative())
}

// toNativeTableData converts the "table data" part of the WriterTable to native C type,
// i.e., it fills the C struct `qdb_exp_batch_push_table_data_t` with the data from the WriterTable.
func (t *WriterTable) toNativeTableData(h HandleType, out *C.qdb_exp_batch_push_table_data_t) error {
	// Set row and column counts directly.
	out.row_count = C.qdb_size_t(t.rowCount)
	out.column_count = C.qdb_size_t(len(t.data))

	// Index ("timestamps") slice: directly reference underlying Go slice memory.
	if t.idx == nil {
		return fmt.Errorf("Index is not set")
	}

	if t.rowCount <= 0 {
		return fmt.Errorf("Index provided, but number of rows is 0")
	}

	if len(t.data) == 0 {
		return fmt.Errorf("Index provided, but no column data provided")
	}

	out.timestamps = t.idx

	// Allocate native columns array using the QuasarDB allocator so the
	// memory remains valid after this function returns.
	columnCount := len(t.data)
	elemSize := unsafe.Sizeof(C.qdb_exp_batch_push_column_t{})
	total := uintptr(columnCount) * elemSize

	basePtr, err := qdbAllocBytes(h, int(total))
	if err != nil {
		return err
	}

	// Convert each WriterData to its native counterpart.
	for i, column := range t.columnInfoByOffset {

		// This is the equivalent to C code:
		//
		//   elem = basePtr[i * elemSize]
		//
		// plus a whole bunch of casts necessary for Go interaction.
		elem := (*C.qdb_exp_batch_push_column_t)(unsafe.Pointer(
			uintptr(basePtr) + uintptr(i)*uintptr(elemSize)))

		// Allocate and copy column name using the QDB allocator.
		name, err := qdbCopyString(h, column.ColumnName)
		if err != nil {
			return fmt.Errorf("toNative: failed to copy column name: %w", err)
		}

		// Initialize struct fields individually to avoid incorrect
		// pointer conversions with the union field on different
		// architectures.
		elem.name = name
		elem.data_type = C.qdb_ts_column_type_t(column.ColumnType)

		// Store the pointer to the value array in the union field.
		// The `data` field is represented as a byte array by cgo.
		// Writing an unsafe.Pointer fits both 32‑bit and 64‑bit
		// systems, as the size of unsafe.Pointer matches the
		// architecture's pointer width.
		*(*unsafe.Pointer)(unsafe.Pointer(&elem.data[0])) = t.data[i].ptr()
	}

	// Store the pointer to the first element.
	out.columns = (*C.qdb_exp_batch_push_column_t)(basePtr)

	return nil
}

// toNative converts WriterTable to native C type and avoids copies where possible.
// It is the caller's responsibility to ensure that the WriterTable lives at least
// as long as the native C structure.
func (t *WriterTable) toNative(h HandleType, opts WriterOptions, out *C.qdb_exp_batch_push_table_t) error {

	out.name = t.TableName

	err := t.toNativeTableData(h, &out.data)
	if err != nil {
		return err
	}

	// Zero-initialize the rest of the struct. This should already be the case,
	// but just in case, we are very explicit about all the default values we
	// use.
	//
	// Insert truncate -- not supported yet
	out.truncate_ranges = nil
	out.truncate_range_count = 0

	// Deduplication parameters
	out.deduplication_mode = C.qdb_exp_batch_deduplication_mode_t(opts.dedupMode)

	// Upsert mode requires explicit columns so QuasarDB knows which columns
	// are compared for duplicates.
	if opts.dedupMode == WriterDeduplicationModeUpsert && len(opts.dropDuplicateColumns) == 0 {
		return fmt.Errorf("upsert deduplication mode requires drop duplicate columns to be set")
	}

	if len(opts.dropDuplicateColumns) > 0 {
		count := len(opts.dropDuplicateColumns)
		elemSize := C.qdb_size_t(unsafe.Sizeof((*C.char)(nil)))
		total := C.qdb_size_t(count) * elemSize

		var basePtr unsafe.Pointer
		errCode := C.qdb_alloc_buffer(h.handle, total, &basePtr)
		if err := makeErrorOrNil(errCode); err != nil {
			return err
		}

		slice := (*[1 << 30]*C.char)(basePtr)[:count:count]
		for i, c := range opts.dropDuplicateColumns {
			slice[i] = (*C.char)(unsafe.Pointer(unsafe.StringData(c)))
		}

		out.where_duplicate = (**C.char)(basePtr)
		out.where_duplicate_count = C.qdb_size_t(count)
	} else {
		out.where_duplicate = nil
		out.where_duplicate_count = 0
	}

	// Never automatically create tables
	out.creation = C.qdb_exp_batch_creation_mode_t(C.qdb_exp_batch_dont_create)

	return nil
}

func (t *WriterTable) releaseNative(h HandleType, tbl *C.qdb_exp_batch_push_table_t) error {
	if tbl == nil {
		panic("WriterTable.releaseNative: nil table pointer")
	}

	columnCount := len(t.data)
	if columnCount == 0 || tbl.data.columns == nil || tbl.name == nil {
		panic("WriterTable.releaseNative: inconsistent state")
	}

	qdbRelease(h, t.TableName)
	t.TableName = nil

	if t.idx != nil {
		qdbRelease(h, t.idx)
		tbl.data.timestamps = nil
		t.idx = nil
	}

	for _, v := range t.data {
		v.releaseNative(h)
	}

	if tbl.data.columns != nil {
		// Release any column names we allocated during toNativeTableData
		columnSlice := unsafe.Slice(tbl.data.columns, columnCount)
		for i := range columnSlice {
			if columnSlice[i].name != nil {
				qdbRelease(h, columnSlice[i].name)
				columnSlice[i].name = nil
			}
		}

		qdbRelease(h, tbl.data.columns)
		tbl.data.columns = nil
	}

	if tbl.where_duplicate != nil {
		qdbRelease(h, tbl.where_duplicate)
		tbl.where_duplicate = nil
	}

	return nil
}

// Sets data for a single column
func (t *WriterTable) SetData(offset int, xs WriterData) error {
	if len(t.columnInfoByOffset) <= offset {
		return fmt.Errorf("Column offset out of range: %v", offset)
	}

	col := t.columnInfoByOffset[offset]
	if col.ColumnType.AsValueType() != xs.valueType() {
		return fmt.Errorf("Column's expected value type does not match provided value type: column type (%v)'s value type %v != %v", col.ColumnType, col.ColumnType.AsValueType(), xs.valueType())
	}

	t.data[offset] = xs

	return nil
}

// Sets all all column data for a single table into the writer, assumes offsets of provided
// data are aligned with the table.
func (t *WriterTable) SetDatas(xs []WriterData) error {
	for i, x := range xs {
		err := t.SetData(i, x)

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *WriterTable) GetData(offset int) (WriterData, error) {
	if offset >= len(t.data) {
		return nil, fmt.Errorf("Column offset out of range: %v", offset)
	}

	return t.data[offset], nil
}

// Returns new WriterOptions struct with default options set.
func NewWriterOptions() WriterOptions {
	return WriterOptions{
		pushMode:             WriterPushModeTransactional,
		dropDuplicates:       false,
		dropDuplicateColumns: []string{},
		dedupMode:            WriterDeduplicationModeDisabled,
	}
}

// Returns the currently set push mode
func (options WriterOptions) GetPushMode() WriterPushMode {
	return options.pushMode
}

// Returns the deduplication mode currently set.
func (options WriterOptions) GetDeduplicationMode() WriterDeduplicationMode {
	return options.dedupMode
}

// Returns true if deduplication is enabled
func (options WriterOptions) IsDropDuplicatesEnabled() bool {
	return options.dropDuplicates
}

// Enables deduplication based on all columns
func (options WriterOptions) EnableDropDuplicates() WriterOptions {
	options.dropDuplicates = true
	if options.dedupMode == WriterDeduplicationModeDisabled {
		// Dropping duplicates causes the least overhead when enabled.
		options.dedupMode = WriterDeduplicationModeDrop
	}
	return options
}

// Enables deduplicates based on the provided columns
func (options WriterOptions) EnableDropDuplicatesOn(columns []string) WriterOptions {
	options.dropDuplicates = true
	options.dropDuplicateColumns = columns
	if options.dedupMode == WriterDeduplicationModeDisabled {
		// Default deduplication mode when enabling is to drop duplicates.
		options.dedupMode = WriterDeduplicationModeDrop
	}
	return options
}

// Returns the columns to be deduplicated on. If empty, deduplicates based on
// equality of all columns.
func (options WriterOptions) GetDropDuplicateColumns() []string {
	return options.dropDuplicateColumns
}

// Sets the push mode to the desired push mode
func (options WriterOptions) WithPushMode(mode WriterPushMode) WriterOptions {
	options.pushMode = mode
	return options
}

// WithDeduplicationMode selects the deduplication behaviour. Upsert mode only
// becomes valid when specific columns are supplied; this is validated during
// native conversion.
func (options WriterOptions) WithDeduplicationMode(mode WriterDeduplicationMode) WriterOptions {
	options.dedupMode = mode
	options.dropDuplicates = mode != WriterDeduplicationModeDisabled
	return options
}

// Shortcut for `WithPushMode(WriterPushModeAsync)`
func (options WriterOptions) WithAsyncPush() WriterOptions {
	return options.WithPushMode(WriterPushModeAsync)
}

// Shortcut for `WithPushMode(WriterPushModeAsync)`
func (options WriterOptions) WithFastPush() WriterOptions {
	return options.WithPushMode(WriterPushModeFast)
}

// Shortcut for `WithPushMode(WriterPushModeAsync)`
func (options WriterOptions) WithTransactionalPush() WriterOptions {
	return options.WithPushMode(WriterPushModeTransactional)
}

// Creates a new Writer with the provided options
func NewWriter(options WriterOptions) Writer {
	return Writer{options: options, tables: make(map[string]WriterTable)}
}

// Creates a new Writer with default options
func NewWriterWithDefaultOptions() Writer {
	return NewWriter(NewWriterOptions())
}

// Returns the writer's options
func (w *Writer) GetOptions() WriterOptions {
	return w.options
}

// Sets the data of a table. Returns error if table already exists.
func (w *Writer) SetTable(t WriterTable) error {
	tableName := t.GetName()

	// Check if the table already exists
	_, exists := w.tables[tableName]
	if exists {
		return fmt.Errorf("table %q already exists", tableName)
	}

	// Ensure schema consistency with previously added tables by comparing to
	// the first table. If a=b and a=c, then b=c.
	for _, existing := range w.tables {
		if !writerTableSchemasEqual(existing, t) {
			return fmt.Errorf("table %q schema differs from existing table %q", t.GetName(), existing.GetName())
		}
		break
	}

	w.tables[tableName] = t

	return nil
}

// Returns the table with the provided name
func (w *Writer) GetTable(name string) (WriterTable, error) {
	t, ok := w.tables[name]
	if !ok {
		return WriterTable{}, fmt.Errorf("Table not found: %s", name)
	}

	return t, nil
}

// Returns the number of tables the writer currently holds.
func (w *Writer) Length() int {
	return len(w.tables)
}

// Pushes all tables to the server according to PushOptions.
func (w *Writer) Push(h HandleType) error {
	if w.Length() == 0 {
		return fmt.Errorf("No tables to push")
	}

	tbls, err := qdbAllocBuffer[C.qdb_exp_batch_push_table_t](h, w.Length())
	if err != nil {
		return fmt.Errorf("Push failed: %v", err)
	}

	defer qdbRelease(h, tbls)
	i := 0

	// Convert the raw pointer to a Go slice explicitly for easier access.
	tblSlice := unsafe.Slice(tbls, w.Length())

	for _, v := range w.tables {
		err := v.toNative(h, w.options, &tblSlice[i])
		if err != nil {
			// Potential memory leak as we don't clear up all our qdbAllocBuffer data, but
			// extemely unlikely to occur.
			return fmt.Errorf("Failed to convert table %q to native: %v", v.GetName(), err)
		}
		defer v.releaseNative(h, &tblSlice[i])
		i++
	}

	var tableSchemas = (**C.qdb_exp_batch_push_table_schema_t)(nil)

	errCode := C.qdb_exp_batch_push(
		h.handle,
		C.qdb_exp_batch_push_mode_t(w.options.pushMode),
		(*C.qdb_exp_batch_push_table_t)(unsafe.Pointer(tbls)),
		tableSchemas,
		C.qdb_size_t(w.Length()),
	)
	return makeErrorOrNil(C.qdb_error_t(errCode))
}

// writerTableSchemasEqual returns true when both tables have the same column
// names and types in identical order.
func writerTableSchemasEqual(a, b WriterTable) bool {
	if len(a.columnInfoByOffset) != len(b.columnInfoByOffset) {
		return false
	}
	for i := range a.columnInfoByOffset {
		if a.columnInfoByOffset[i] != b.columnInfoByOffset[i] {
			return false
		}
	}
	return true
}
