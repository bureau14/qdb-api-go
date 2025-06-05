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
	// Returns the type of value of this class
	valueType() TsValueType

	// Returns opaque pointer to internal data array, intended to be set on
	// the `qdb_exp_batch_push_column_t` `data` field.
	toNative(h HandleType) (unsafe.Pointer, error)
}

// Int64
type WriterDataInt64 struct {
	xs []int64
}

// NewWriterDataInt64 constructs a WriterDataInt64 instance, copying the input slice
// into memory allocated by the QDB C API. The allocated memory must be released with C.qdb_release().
func NewWriterDataInt64(xs []int64) WriterData {
	return &WriterDataInt64{xs: xs}
}

func (wd *WriterDataInt64) valueType() TsValueType {
	return TsValueInt64
}

func (wd *WriterDataInt64) toNative(h HandleType) (unsafe.Pointer, error) {
	ptr, err := qdbAllocAndCopyBuffer[int64, C.qdb_int_t](h, wd.xs)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate and copy int64 data: %w", err)
	}

	return unsafe.Pointer(ptr), nil
}

// Double
type WriterDataDouble struct {
	xs []float64
}

// NewWriterDataDouble constructs a WriterDataDouble instance, copying the input slice
// into memory allocated by the QDB C API. The allocated memory must be released with C.qdb_release().
func NewWriterDataDouble(xs []float64) WriterData {
	return &WriterDataDouble{xs}
}

func (wd WriterDataDouble) valueType() TsValueType {
	return TsValueDouble
}

func (wd *WriterDataDouble) toNative(h HandleType) (unsafe.Pointer, error) {
	ptr, err := qdbAllocAndCopyBuffer[float64, C.double](h, wd.xs)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate and copy double data: %w", err)
	}

	return unsafe.Pointer(ptr), nil
}

// Timestamp
type WriterDataTimestamp struct {
	xs []C.qdb_timespec_t
}

// NewWriterDataTimestampFromTimespec constructs WriterDataTimestamp from a slice of C.qdb_timespec_t,
// copying the data into a newly allocated buffer via qdbAllocAndCopy.
func NewWriterDataTimestampFromTimespec(xs []C.qdb_timespec_t) WriterData {
	return &WriterDataTimestamp{xs: xs}
}

// Constructor for timestamp data array
func NewWriterDataTimestamp(xs []time.Time) WriterData {
	return NewWriterDataTimestampFromTimespec(TimeSliceToQdbTimespec(xs))
}

func (_ WriterDataTimestamp) valueType() TsValueType {
	return TsValueTimestamp
}

func (wd *WriterDataTimestamp) toNative(h HandleType) (unsafe.Pointer, error) {
	ptr, err := qdbAllocAndCopyBuffer[C.qdb_timespec_t, C.qdb_timespec_t](h, wd.xs)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate and copy timestamp data: %w", err)
	}

	return unsafe.Pointer(ptr), nil
}

// Blob
type WriterDataBlob struct {
	xs [][]byte
}

// Constructor for blob data array
func NewWriterDataBlob(xs [][]byte) WriterData {
	return &WriterDataBlob{xs}
}

func (cd WriterDataBlob) valueType() TsValueType {
	return TsValueBlob
}

func (wd *WriterDataBlob) toNative(h HandleType) (unsafe.Pointer, error) {
	count := len(wd.xs)
	if count == 0 {
		return nil, fmt.Errorf("no blobs provided")
	}

	// Step 1: allocate qdb_blob_t array
	blobArrayPtr, err := qdbAllocBuffer[C.qdb_blob_t](h, count)
	if err != nil {
		return nil, fmt.Errorf("blob struct array allocation failed: %v", err)
	}

	// Step 2: copy blob contents
	blobSlice := unsafe.Slice(blobArrayPtr, count)
	for i, v := range wd.xs {
		var destPtr unsafe.Pointer

		if len(v) > 0 {
			// Calculate the correct offset within the large contiguous slab
			destPtr, err = qdbAllocAndCopyBytes(h, v)
			if err != nil {
				return nil, err
			}
			blobSlice[i].content = destPtr
			blobSlice[i].content_length = C.qdb_size_t(len(v))
		} else {
			// Explicitly handle empty blobs
			blobSlice[i].content = nil
			blobSlice[i].content_length = 0
		}
	}

	return unsafe.Pointer(blobArrayPtr), nil
}

// String
type WriterDataString struct {
	xs []string
}

// Constructor for string data array
func NewWriterDataString(xs []string) WriterData {
	return &WriterDataString{xs}
}

func (cd WriterDataString) valueType() TsValueType {
	return TsValueString
}

func (wd *WriterDataString) toNative(h HandleType) (unsafe.Pointer, error) {
	count := len(wd.xs)
	if count == 0 {
		return nil, fmt.Errorf("no strings provided")
	}

	// Step 1: allocate qdb_string_t array
	retPtr, err := qdbAllocBuffer[C.qdb_string_t](h, count)
	if err != nil {
		return nil, err
	}

	retSlice := unsafe.Slice(retPtr, count)

	// Step 2: copy string contents
	for i, v := range wd.xs {
		var destPtr *C.char

		if len(v) > 0 {
			// Calculate the correct offset within the large contiguous slab
			destPtr, err = qdbCopyString(h, v)
			if err != nil {
				return nil, err
			}
			retSlice[i].data = destPtr
			retSlice[i].length = C.qdb_size_t(len(v))
		} else {
			// Explicitly handle empty blobs
			retSlice[i].data = nil
			retSlice[i].length = 0
		}
	}

	return unsafe.Pointer(retPtr), nil
}

// Metadata we need to represent a single column.
type WriterColumn struct {
	ColumnName string
	ColumnType TsColumnType
}

// Single table to be provided to the batch writer.
type WriterTable struct {
	TableName string

	// All arrays are guaranteed to be of lenght `rowCount`. This means specifically
	// the `idx` parameter and all Writerdata value arrays within `data`.
	rowCount int

	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []WriterColumn

	// An index that enables looking up of a column's offset within the table by its name.
	columnOffsetByName map[string]int

	// The index, can not contain null values
	idx []C.qdb_timespec_t

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

// GetWriterDataInt64 safely converts WriterData to *WriterDataInt64.
//
// Returns an error if data is not of type Int64.
func GetWriterDataInt64(x WriterData) (*WriterDataInt64, error) {
	v, ok := x.(*WriterDataInt64)
	if !ok {
		return nil, fmt.Errorf("GetInt64Array: type mismatch, expected WriterDataInt64, got %T", x)
	}
	return v, nil
}

// GetWriterDataInt64Unsafe is an unsafe version of GetWriterDataInt64. Undefined behavior occurs when
// invoked on the incorrect type.
func GetWriterDataInt64Unsafe(x WriterData) *WriterDataInt64 {
	return (*WriterDataInt64)(ifaceDataPtr(x))
}

// GetWriterDataDouble safely converts WriterData to *WriterDataDouble.
//
// Returns an error if data is not of type Double.
func GetWriterDataDouble(x WriterData) (*WriterDataDouble, error) {
	v, ok := x.(*WriterDataDouble)
	if !ok {
		return nil, fmt.Errorf("GetDoubleArray: type mismatch, expected WriterDataDouble, got %T", x)
	}
	return v, nil
}

// GetWriterDataDoubleUnsafe is an unsafe version of GetWriterDataDouble. Undefined behavior occurs when
// invoked on the incorrect type.
func GetWriterDataDoubleUnsafe(x WriterData) *WriterDataDouble {
	return (*WriterDataDouble)(ifaceDataPtr(x))
}

// GetWriterDataTimestamp safely converts WriterData to *WriterDataTimestamp.
//
// Returns an error if data is not of type Timestamp.
func GetWriterDataTimestamp(x WriterData) (*WriterDataTimestamp, error) {
	v, ok := x.(*WriterDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetTimestampArray: type mismatch, expected WriterDataTimestamp, got %T", x)
	}
	return v, nil
}

// GetWriterDataTimestampUnsafe is an unsafe version of GetWriterDataTimestamp. Undefined behavior occurs when
// invoked on the incorrect type.
func GetWriterDataTimestampUnsafe(x WriterData) *WriterDataTimestamp {
	return (*WriterDataTimestamp)(ifaceDataPtr(x))
}

// GetWriterDataString safely converts WriterData to *WriterDataString.
//
// Returns an error if data is not of type String.
func GetWriterDataString(x WriterData) (*WriterDataString, error) {
	v, ok := x.(*WriterDataString)
	if !ok {
		return nil, fmt.Errorf("GetStringArray: type mismatch, expected WriterDataString, got %T", x)
	}
	return v, nil
}

// GetWriterDataStringUnsafe is an unsafe version of GetWriterDataString. Undefined behavior occurs when
// invoked on the incorrect type.
func GetWriterDataStringUnsafe(x WriterData) *WriterDataString {
	return (*WriterDataString)(ifaceDataPtr(x))
}

// GetWriterDataBlob safely converts WriterData to *WriterDataBlob.
//
// Returns an error if data is not of type Blob.
func GetWriterDataBlob(x WriterData) (*WriterDataBlob, error) {
	v, ok := x.(*WriterDataBlob)
	if !ok {
		return nil, fmt.Errorf("GetBlobArray: type mismatch, expected WriterDataBlob, got %T", x)
	}
	return v, nil
}

// GetWriterDataBlobUnsafe is an unsafe version of GetWriterDataBlob. Undefined behavior occurs when
// invoked on the incorrect type.
func GetWriterDataBlobUnsafe(x WriterData) *WriterDataBlob {
	return (*WriterDataBlob)(ifaceDataPtr(x))
}

func NewWriterTable(t string, cols []WriterColumn) (WriterTable, error) {
	// Pre-allocate our data array, which has exactly 1 entry for every column we intend to write.
	data := make([]WriterData, len(cols))

	// Build indexes
	columnInfoByOffset := make([]WriterColumn, len(cols))
	columnOffsetByName := make(map[string]int)

	for i, col := range cols {
		columnInfoByOffset[i] = col
		columnOffsetByName[col.ColumnName] = i
	}

	// An index of column offset to name
	return WriterTable{t, 0, columnInfoByOffset, columnOffsetByName, nil, data}, nil
}

func (t *WriterTable) GetName() string {
	return t.TableName
}

func (t *WriterTable) RowCount() int {
	return t.rowCount
}

func (t *WriterTable) SetIndexFromNative(idx []C.qdb_timespec_t) {
	t.idx = idx
	t.rowCount = len(idx)
}

func (t *WriterTable) SetIndex(idx []time.Time) {
	t.SetIndexFromNative(TimeSliceToQdbTimespec(idx))
}

func (t *WriterTable) GetIndexAsNative() []C.qdb_timespec_t {
	return t.idx
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

	timestampPtr, err := qdbAllocAndCopyBuffer[C.qdb_timespec_t, C.qdb_timespec_t](h, t.idx)
	if err != nil {
		return fmt.Errorf("Unable to copy timestamps: %v", err)
	}
	out.timestamps = timestampPtr

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

		ptr, err := t.data[i].toNative(h)
		if err != nil {
			return err
		}

		// Store the pointer to the value array in the union field.
		// The `data` field is represented as a byte array by cgo.
		// Writing an unsafe.Pointer fits both 32‑bit and 64‑bit
		// systems, as the size of unsafe.Pointer matches the
		// architecture's pointer width.
		//
		// `ptr` should be released using qdbRelease() once done
		*(*unsafe.Pointer)(unsafe.Pointer(&elem.data[0])) = ptr
	}

	// Store the pointer to the first element.
	out.columns = (*C.qdb_exp_batch_push_column_t)(basePtr)

	return nil
}

// toNative converts WriterTable to native C type and avoids copies where possible.
// It is the caller's responsibility to ensure that the WriterTable lives at least
// as long as the native C structure.
func (t *WriterTable) toNative(h HandleType, opts WriterOptions, out *C.qdb_exp_batch_push_table_t) error {

	tableName, err := qdbCopyString(h, t.TableName)
	if err != nil {
		return err
	}

	out.name = tableName

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

	err = t.toNativeTableData(h, &out.data)
	if err != nil {
		return err
	}

	return nil
}

func releaseBatchPushBlobColumns(h HandleType, xs []C.qdb_blob_t) {
	for _, x := range xs {
		if x.content != nil {
			C.qdb_release(h.handle, x.content)
		}
	}
}

func releaseBatchPushStringColumns(h HandleType, xs []C.qdb_string_t) {
	for _, x := range xs {
		if x.data != nil {
			qdbRelease(h, x.data)
		}
	}
}

// Invokes qdb_release() in all data stored inside a push column, which we previously manually
// allocated using the qdbAlloc.. set of functions.
func releaseBatchPushColumn(h HandleType, x C.qdb_exp_batch_push_column_t, rowCount int) error {
	if x.name != nil {
		qdbRelease(h, x.name)
	}

	// Extract the pointer stored in the union field. We must read the pointer
	// value instead of taking the address of the union field, otherwise we pass
	// a pointer to Go stack memory to C which triggers the cgo "Go pointer to
	// unpinned Go pointer" check.
	dataPtr := *(*unsafe.Pointer)(unsafe.Pointer(&x.data[0]))
	if dataPtr != nil {

		// For blobs and strings, we need to go through the extra effort of releasing
		// their internally allocated data.
		switch x.data_type {
		case C.qdb_ts_column_blob:
			xs := unsafe.Slice((*C.qdb_blob_t)(dataPtr), rowCount)
			releaseBatchPushBlobColumns(h, xs)
		case C.qdb_ts_column_string:
			xs := unsafe.Slice((*C.qdb_string_t)(dataPtr), rowCount)
			releaseBatchPushStringColumns(h, xs)
		}

		qdbReleasePointer(h, dataPtr)
	}

	return nil
}

func releaseBatchPushColumns(h HandleType, xs []C.qdb_exp_batch_push_column_t, rowCount int) error {
	for _, x := range xs {
		err := releaseBatchPushColumn(h, x, rowCount)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *WriterTable) releaseNative(h HandleType, tbl *C.qdb_exp_batch_push_table_t) error {
	if tbl == nil {
		return fmt.Errorf("WriterTable.releaseNative: nil table pointer")
	}

	columnCount := len(t.data)
	if columnCount == 0 || tbl.data.columns == nil || tbl.name == nil {
		return fmt.Errorf("WriterTable.releaseNative: inconsistent state")
	}

	if tbl.name != nil {
		qdbRelease(h, tbl.name)
		tbl.name = nil
	}

	if t.idx != nil {
		qdbRelease(h, tbl.data.timestamps)
		tbl.data.timestamps = nil
	}

	if tbl.data.columns != nil {
		// Release any column names we allocated during toNativeTableData
		columnSlice := unsafe.Slice(tbl.data.columns, columnCount)
		err := releaseBatchPushColumns(h, columnSlice, int(tbl.data.row_count))
		if err != nil {
			return err
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

type WriterPushFlag C.qdb_exp_batch_push_flags_t

const (
	WriterPushFlagNone            WriterPushFlag = C.qdb_exp_batch_push_flag_none
	WriterPushFlagWriteThrough    WriterPushFlag = C.qdb_exp_batch_push_flag_write_through
	WriterPushFlagAsyncClientPush WriterPushFlag = C.qdb_exp_batch_push_flag_asynchronous_client_push
)

type WriterOptions struct {
	pushMode             WriterPushMode
	dropDuplicates       bool
	dropDuplicateColumns []string
	dedupMode            WriterDeduplicationMode
	pushFlags            WriterPushFlag
}

// NewWriterOptions returns a WriterOptions struct initialized with safe, default settings.
//
// Defaults:
// - Push mode: Transactional (strongest consistency, lowest performance)
// - Deduplication: Disabled (fastest ingestion, risk of duplicate data)
// - Write-Through: Enabled (don't pollute the query cache with newly written data)
func NewWriterOptions() WriterOptions {
	return WriterOptions{
		pushMode:             WriterPushModeTransactional,
		dropDuplicates:       false,
		dropDuplicateColumns: []string{},
		dedupMode:            WriterDeduplicationModeDisabled,
		pushFlags:            WriterPushFlagWriteThrough,
	}
}

// GetPushMode returns the current WriterPushMode configured in WriterOptions.
func (options WriterOptions) GetPushMode() WriterPushMode {
	return options.pushMode
}

// GetDeduplicationMode returns the current deduplication strategy.
func (options WriterOptions) GetDeduplicationMode() WriterDeduplicationMode {
	return options.dedupMode
}

// IsDropDuplicatesEnabled indicates whether deduplication is currently enabled.
func (options WriterOptions) IsDropDuplicatesEnabled() bool {
	return options.dropDuplicates
}

// EnableWriteThrough ensures each push bypasses the server-side cache.
//
// Decision rationale:
//   - Write-through avoids stale reads when concurrent clients rely on cache coherency.
//
// Key assumptions:
//   - Other push flags remain intact; we simply set the bit via OR.
//
// Performance trade-offs:
//   - Slightly higher latency as new values are immediately committed.
//
// Usage example:
//
//	opt := NewWriterOptions().EnableWriteThrough()
func (options WriterOptions) EnableWriteThrough() WriterOptions {
	options.pushFlags |= WriterPushFlagWriteThrough
	return options
}

// DisableWriteThrough allows caching of newly pushed data on the server.
//
// Decision rationale:
//   - Useful when write-heavy workloads benefit from reduced commit overhead.
//
// Key assumptions:
//   - Caller intentionally accepts potential cache incoherency during writes.
//
// Performance trade-offs:
//   - May serve slightly stale data if cache eviction lags behind writes.
//
// Usage example:
//
//	opt := NewWriterOptions().DisableWriteThrough()
func (options WriterOptions) DisableWriteThrough() WriterOptions {
	options.pushFlags &^= WriterPushFlagWriteThrough
	return options
}

// IsWriteThroughEnabled reports whether push operations bypass the cache.
//
// Decision rationale:
//   - Helps unit tests verify flag manipulation logic.
func (options WriterOptions) IsWriteThroughEnabled() bool {
	return options.pushFlags&WriterPushFlagWriteThrough != 0
}

// EnableAsyncClientPush makes Push return before the server writes data.
//
// Decision rationale:
//   - Enables batching from the client without blocking on disk I/O.
//
// Key assumptions:
//   - Caller handles potential failures asynchronously.
//
// Performance trade-offs:
//   - Higher throughput at the cost of durability on client failure.
//
// Usage example:
//
//	opt := NewWriterOptions().EnableAsyncClientPush()
func (options WriterOptions) EnableAsyncClientPush() WriterOptions {
	options.pushFlags |= WriterPushFlagAsyncClientPush
	return options
}

// DisableAsyncClientPush waits for server acknowledgement before returning.
//
// Decision rationale:
//   - Provides stronger durability guarantees for each push.
//
// Key assumptions:
//   - Caller desires synchronous semantics and accepts reduced throughput.
//
// Performance trade-offs:
//   - Slower ingestion but easier error handling.
//
// Usage example:
//
//	opt := NewWriterOptions().DisableAsyncClientPush()
func (options WriterOptions) DisableAsyncClientPush() WriterOptions {
	options.pushFlags &^= WriterPushFlagAsyncClientPush
	return options
}

// IsAsyncClientPushEnabled reports whether pushes return before data is persisted.
//
// Decision rationale:
//   - Mirrors Enable/Disable helpers for symmetric API design.
func (options WriterOptions) IsAsyncClientPushEnabled() bool {
	return options.pushFlags&WriterPushFlagAsyncClientPush != 0
}

// EnableDropDuplicates activates deduplication across all columns.
//
// Trade-off:
//   - Increases CPU and memory overhead slightly due to additional hashing/comparison,
//     but ensures data uniqueness across the full row.
func (options WriterOptions) EnableDropDuplicates() WriterOptions {
	options.dropDuplicates = true
	if options.dedupMode == WriterDeduplicationModeDisabled {
		options.dedupMode = WriterDeduplicationModeDrop // set least-expensive deduplication strategy
	}
	return options
}

// EnableDropDuplicatesOn activates deduplication limited to specific columns.
//
// Assumption:
// - Caller specifies at least one valid column; validation is deferred.
//
// Performance implication:
// - Reduces overhead compared to full-row deduplication, useful for large, wide tables.
func (options WriterOptions) EnableDropDuplicatesOn(columns []string) WriterOptions {
	options.dropDuplicates = true
	options.dropDuplicateColumns = columns
	if options.dedupMode == WriterDeduplicationModeDisabled {
		options.dedupMode = WriterDeduplicationModeDrop // use least-expensive strategy by default
	}
	return options
}

// GetDropDuplicateColumns returns a slice of columns targeted for deduplication.
//
// Behavior:
// - Empty slice implies deduplication is performed across all columns.
func (options WriterOptions) GetDropDuplicateColumns() []string {
	return options.dropDuplicateColumns
}

// IsValid validates the combination of WriterOptions fields.
//
// Decision rationale:
//   - Centralized sanity checks simplify downstream validation and generators.
//   - Mirrors checks performed during conversion in WriterTable.toNative.
func (options WriterOptions) IsValid() bool {
	switch options.pushMode {
	case WriterPushModeTransactional, WriterPushModeFast, WriterPushModeAsync:
	default:
		return false
	}

	switch options.dedupMode {
	case WriterDeduplicationModeDisabled, WriterDeduplicationModeDrop, WriterDeduplicationModeUpsert:
	default:
		return false
	}

	allowedFlags := WriterPushFlagWriteThrough | WriterPushFlagAsyncClientPush | WriterPushFlagNone
	if options.pushFlags&^allowedFlags != 0 {
		return false
	}

	if options.dedupMode == WriterDeduplicationModeUpsert && len(options.dropDuplicateColumns) == 0 {
		return false
	}

	if options.dedupMode == WriterDeduplicationModeDisabled && options.dropDuplicates {
		return false
	}

	if options.dedupMode != WriterDeduplicationModeDisabled && !options.dropDuplicates {
		return false
	}

	return true
}

// WithPushMode sets the desired push mode and returns an updated copy of WriterOptions.
//
// Trade-offs:
// - Transactional: High consistency guarantees, slower performance
// - Fast/Async: Lower consistency, higher throughput
func (options WriterOptions) WithPushMode(mode WriterPushMode) WriterOptions {
	options.pushMode = mode
	return options
}

// WithDeduplicationMode explicitly sets deduplication behavior.
//
// Assumption:
// - Upsert mode validity depends on provided columns; caller must ensure correct usage.
//
// Implications:
// - Enabling any deduplication mode activates dropDuplicates automatically.
func (options WriterOptions) WithDeduplicationMode(mode WriterDeduplicationMode) WriterOptions {
	options.dedupMode = mode
	options.dropDuplicates = mode != WriterDeduplicationModeDisabled
	return options
}

// WithAsyncPush is a convenience method equivalent to WithPushMode(WriterPushModeAsync).
func (options WriterOptions) WithAsyncPush() WriterOptions {
	return options.WithPushMode(WriterPushModeAsync)
}

// WithFastPush is a convenience method equivalent to WithPushMode(WriterPushModeFast).
func (options WriterOptions) WithFastPush() WriterOptions {
	return options.WithPushMode(WriterPushModeFast)
}

// WithTransactionalPush is a convenience method equivalent to WithPushMode(WriterPushModeTransactional).
func (options WriterOptions) WithTransactionalPush() WriterOptions {
	return options.WithPushMode(WriterPushModeTransactional)
}

// setNative transfers specific fields from WriterOptions into a native C struct qdb_exp_batch_options_t.
//
// Important:
//   - This method intentionally mutates only a subset of the fields in the native struct.
//   - Fields not explicitly set here must be configured separately.
//
// Rationale:
//   - Direct mapping from WriterOptions to qdb_exp_batch_options_t is not 1:1; some options are
//     managed elsewhere due to structural differences between Go and native representations.
func (options WriterOptions) setNative(opts C.qdb_exp_batch_options_t) C.qdb_exp_batch_options_t {
	opts.mode = C.qdb_exp_batch_push_mode_t(options.pushMode)
	return opts

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
			// Potential memory leak occurs here, but if we cannot do this conversion,
			// it means something is very wrong and the user should close the handle
			// anyway (and all memory is allocated+tracked using qdbAlloc anyway)
			return fmt.Errorf("Failed to convert table %q to native: %v", v.GetName(), err)
		}
		defer v.releaseNative(h, &tblSlice[i])
		i++
	}

	var tableSchemas = (**C.qdb_exp_batch_push_table_schema_t)(nil)

	var options C.qdb_exp_batch_options_t
	options = w.options.setNative(options)

	errCode := C.qdb_exp_batch_push_with_options(
		h.handle,
		&options,
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
