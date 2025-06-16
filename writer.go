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
	data []ColumnData
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

// NewWriterTable constructs an empty table definition using the provided columns.
//
// Decision rationale:
//   - Precomputes both column name→offset and offset→column mappings for
//     efficient validation during SetData and Push.
//
// Key assumptions:
//   - cols contains at least one column and no duplicate names.
//
// Performance trade-offs:
//   - Linear initialization to build the lookup maps; negligible for typical
//     column counts.
func NewWriterTable(t string, cols []WriterColumn) (WriterTable, error) {
	data := make([]ColumnData, len(cols))

	columnInfoByOffset := make([]WriterColumn, len(cols))
	columnOffsetByName := make(map[string]int)

	for i, col := range cols {
		columnInfoByOffset[i] = col
		columnOffsetByName[col.ColumnName] = i
	}

	return WriterTable{t, 0, columnInfoByOffset, columnOffsetByName, nil, data}, nil
}

// GetName returns the table identifier used when pushing data.
func (t *WriterTable) GetName() string {
	return t.TableName
}

// RowCount reports the number of rows currently assigned to the table.
func (t *WriterTable) RowCount() int {
	return t.rowCount
}

// SetIndexFromNative sets the timestamp index using a C-compatible slice.
//
// Decision rationale:
//   - Avoids repeated conversions when the caller already holds native timespec
//     values (e.g., from another API call).
//
// Key assumptions:
//   - idx represents the exact row count for subsequent column data.
func (t *WriterTable) SetIndexFromNative(idx []C.qdb_timespec_t) {
	t.idx = idx
	t.rowCount = len(idx)
}

// SetIndex converts times to qdb_timespec_t and stores them as the index.
// This helper is convenient for typical Go callers.
func (t *WriterTable) SetIndex(idx []time.Time) {
	t.SetIndexFromNative(TimeSliceToQdbTimespec(idx))
}

// GetIndexAsNative exposes the internal index slice in C form.
// The caller must treat the slice as read-only.
func (t *WriterTable) GetIndexAsNative() []C.qdb_timespec_t {
	return t.idx
}

// GetIndex returns the index converted back to time.Time values.
func (t *WriterTable) GetIndex() []time.Time {
	return QdbTimespecSliceToTime(t.GetIndexAsNative())
}

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
	cols, err := qdbAllocBuffer[C.qdb_exp_batch_push_column_t](h, columnCount)
	if err != nil {
		return err
	}
	colSlice := unsafe.Slice(cols, columnCount)

	// Convert each ColumnData to its native counterpart.
	for i, column := range t.columnInfoByOffset {

		elem := &colSlice[i]

		// Allocate and copy column name using the QDB allocator.
		name, err := qdbCopyString(h, column.ColumnName)
		if err != nil {
			return fmt.Errorf("toNative: failed to copy column name: %w", err)
		}

		elem.name = name
		elem.data_type = C.qdb_ts_column_type_t(column.ColumnType)

		ptr := t.data[i].CopyToC(h)
		*(*unsafe.Pointer)(unsafe.Pointer(&elem.data[0])) = ptr
	}

	// Store the pointer to the first element.
	out.columns = cols

	return nil
}

// toNative converts WriterTable to native C type and avoids copies where possible.
// It is the caller's responsibility to ensure that the WriterTable lives at least
// as long as the native C structure.
func (t *WriterTable) toNative(pinner *runtime.Pinner, h HandleType, opts WriterOptions, out *C.qdb_exp_batch_push_table_t) error {
	var err error

	// Zero-copy: use the Go string bytes directly and pin them so the GC keeps
	// the backing array alive for the entire push.
	out.name = pinStringBytes(pinner, &t.TableName)

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
		ptr, err := qdbAllocBuffer[*C.char](h, count)
		if err != nil {
			return err
		}
		dupSlice := unsafe.Slice(ptr, count)
		for i := range opts.dropDuplicateColumns {
			dupSlice[i] = pinStringBytes(pinner, &opts.dropDuplicateColumns[i])
		}

		out.where_duplicate = ptr
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

// releaseBatchPushBlobColumns releases the memory of a slice of qdb_blob_t.
// Each blob may own individually allocated content buffers.
func releaseBatchPushBlobColumns(h HandleType, xs []C.qdb_blob_t) {
	for _, x := range xs {
		if x.content != nil {
			C.qdb_release(h.handle, x.content)
		}
	}
}

// releaseBatchPushStringColumns releases memory owned by qdb_string_t elements.
func releaseBatchPushStringColumns(h HandleType, xs []C.qdb_string_t) {
	for _, x := range xs {
		if x.data != nil {
			qdbRelease(h, x.data)
		}
	}
}

// releaseBatchPushColumn frees all allocations associated with a single push column.
//
// Decision rationale:
//   - Consolidates cleanup logic for WriterTable.releaseNative.
//   - Handles type-specific allocations for strings and blobs.
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

// releaseBatchPushColumns iterates releaseBatchPushColumn over the provided slice.
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
		// Name points to pinned Go memory – just nil it out.
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

func (t *WriterTable) SetData(offset int, xs ColumnData) error {
	if len(t.columnInfoByOffset) <= offset {
		return fmt.Errorf("Column offset out of range: %v", offset)
	}

	col := t.columnInfoByOffset[offset]
	if col.ColumnType.AsValueType() != xs.ValueType() {
		return fmt.Errorf("Column's expected value type does not match provided value type: column type (%v)'s value type %v != %v", col.ColumnType, col.ColumnType.AsValueType(), xs.ValueType())
	}

	t.data[offset] = xs

	return nil
}

func (t *WriterTable) SetDatas(xs []ColumnData) error {
	for i, x := range xs {
		err := t.SetData(i, x)

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *WriterTable) GetData(offset int) (ColumnData, error) {
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
	var pinner runtime.Pinner
	defer pinner.Unpin()

	if w.Length() == 0 {
		return fmt.Errorf("No tables to push")
	}

	tblSlice := make([]C.qdb_exp_batch_push_table_t, w.Length())
	i := 0

	for _, v := range w.tables {
		err := v.toNative(&pinner, h, w.options, &tblSlice[i])
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
		&tblSlice[0],
		tableSchemas,
		C.qdb_size_t(len(tblSlice)),
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
