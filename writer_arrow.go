// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: QuasarDB Go client API
// Arrow input for the batch writer.
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>

	#cgo noescape qdb_exp_batch_push_arrow_with_options

	// No nocallback: the C API drains each ArrowArrayStream inside this call,
	// and the stream exported by arrow-go answers get_schema, get_next and
	// release with Go functions. Those run as C-to-Go callbacks during the
	// call.
*/
import "C"

import (
	"runtime"
	"slices"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

const arrowTableColumnName = "$table"

// arrowSupportedTypes are the Arrow types the C API accepts as data columns.
var arrowSupportedTypes = []arrow.Type{arrow.INT64, arrow.FLOAT64, arrow.TIMESTAMP, arrow.DATE64, arrow.STRING, arrow.BINARY}

// arrowTypeSupported reports whether the C API accepts this Arrow type as a
// data column. The set mirrors make_arrow_holder in the C client: anything
// else is refused there with qdb_e_not_implemented.
func arrowTypeSupported(dt arrow.DataType) bool {
	return slices.Contains(arrowSupportedTypes, dt.ID())
}

// arrowTimestampTypeSupported reports whether the C API accepts this Arrow
// type as the "$timestamp" index: TIMESTAMP in any unit, or DATE64.
func arrowTimestampTypeSupported(dt arrow.DataType) bool {
	return dt.ID() == arrow.TIMESTAMP || dt.ID() == arrow.DATE64
}

// findTimestampField returns the index of the "$timestamp" field, or -1.
func findTimestampField(schema *arrow.Schema) int {
	idx := schema.FieldIndices(tsTimestampColumnName)
	if len(idx) == 0 {
		return -1
	}

	return idx[0]
}

// validateArrowSchema checks a table schema against what the C API accepts.
func validateArrowSchema(table string, schema *arrow.Schema) error {
	// The C side finds the index by name and sends every other field as a
	// data column, so the checks are on names and types only:
	//  1. "$timestamp" present and of a timestamp type;
	//  2. no "$table": the reader emits it, the C side would send it as data;
	//  3. every other field of a type the C side can hold.
	if ts := findTimestampField(schema); ts < 0 {
		return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "table", table, "reason", "missing $timestamp column")
	} else if !arrowTimestampTypeSupported(schema.Field(ts).Type) {
		return wrapError(C.qdb_e_incompatible_type, "arrow_writer_set_table", "table", table, "column", tsTimestampColumnName, "type", schema.Field(ts).Type.String())
	}

	if len(schema.FieldIndices(arrowTableColumnName)) > 0 {
		return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "table", table, "reason", "$table column not allowed")
	}

	for _, f := range schema.Fields() {
		if f.Name == tsTimestampColumnName || arrowTypeSupported(f.Type) {
			continue
		}

		return wrapError(C.qdb_e_incompatible_type, "arrow_writer_set_table", "table", table, "column", f.Name, "type", f.Type.String())
	}

	return nil
}

// validateArrowBatches checks that every batch shares the first schema and
// that no "$timestamp" slot is null. A null index slot becomes qdb_min_time
// on the C side and is refused there; refusing it here names the table.
func validateArrowBatches(table string, batches []arrow.RecordBatch) error {
	if len(batches) == 0 {
		return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "table", table, "reason", "no batches")
	}

	schema := batches[0].Schema()
	err := validateArrowSchema(table, schema)
	if err != nil {
		return err
	}
	ts := findTimestampField(schema)

	for i, rec := range batches {
		if !rec.Schema().Equal(schema) {
			return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "table", table, "batch", i, "reason", "schema differs from first batch")
		}
		if rec.Column(ts).NullN() > 0 {
			return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "table", table, "batch", i, "reason", "null $timestamp")
		}
	}

	return nil
}

// pinArrayData pins the backing array of every buffer reachable from d.
func pinArrayData(pinner *runtime.Pinner, d arrow.ArrayData) {
	// arrow-go's export stores each buffer's address in C memory. The cgo
	// rules allow that only for pinned memory, and cgocheck2 enforces it.
	// Pin is a no-op for buffers that already live in C memory, such as
	// batches that came out of Reader.Arrow or FetchArrow.
	for _, buf := range d.Buffers() {
		if buf != nil && buf.Len() > 0 {
			pinner.Pin(&buf.Bytes()[0])
		}
	}

	// Validation limits columns to flat types, so these are empty today;
	// walking them keeps the pin complete if that ever changes.
	for _, child := range d.Children() {
		pinArrayData(pinner, child)
	}
	// Dictionary returns a typed nil for every other type, so check the type.
	if d.DataType().ID() == arrow.DICTIONARY {
		pinArrayData(pinner, d.Dictionary())
	}
}

// pinRecordBuffers pins every buffer of every column of rec.
func pinRecordBuffers(pinner *runtime.Pinner, rec arrow.RecordBatch) {
	for _, col := range rec.Columns() {
		pinArrayData(pinner, col.Data())
	}
}

// exportArrowStream fills the zeroed stream slot with a stream over batches.
func exportArrowStream(batches []arrow.RecordBatch, out *C.struct_ArrowArrayStream) error {
	// cdata requires zeroed destination memory; the table array is allocated
	// zeroed. The record reader retains the batches; the exported stream then
	// retains the reader, so our reference can go at once. The stream is
	// released by whoever consumes it: the C side on success, releaseIfLive
	// otherwise.
	rr, err := array.NewRecordReader(batches[0].Schema(), batches)
	if err != nil {
		return wrapError(C.qdb_e_invalid_argument, "arrow_writer_push", errorDetailKey, err.Error())
	}
	defer rr.Release()

	cdata.ExportRecordReader(rr, (*cdata.CArrowArrayStream)(unsafe.Pointer(out)))

	return nil
}

// releaseIfLive releases a stream the C side did not consume.
func releaseIfLive(stream *C.struct_ArrowArrayStream) {
	// arrow::ImportRecordBatchReader moves the stream and NULLs the source
	// release; an error before that point (bad mode, bad flags) leaves it
	// set, and then the Go side must release it.
	if stream.release != nil {
		cdata.ReleaseCArrowArrayStream((*cdata.CArrowArrayStream)(unsafe.Pointer(stream)))
	}
}

// arrowWriterTable is one staged table: its name and the batches to send.
type arrowWriterTable struct {
	name    string
	batches []arrow.RecordBatch
}

// rowCount sums the rows over all batches.
func (t arrowWriterTable) rowCount() int64 {
	var n int64
	for _, rec := range t.batches {
		n += rec.NumRows()
	}

	return n
}

// retainAndPin retains every batch and pins its buffers. The returned
// closure releases the retained references.
func retainAndPin(pinner *runtime.Pinner, tables []arrowWriterTable) func() {
	var retained []arrow.RecordBatch
	for _, t := range tables {
		for _, rec := range t.batches {
			rec.Retain()
			retained = append(retained, rec)
			pinRecordBuffers(pinner, rec)
		}
	}

	return func() {
		for _, rec := range retained {
			rec.Release()
		}
	}
}

// ArrowWriter stages arrow.RecordBatch tables and pushes them to the server
// in one qdb_exp_batch_push_arrow_with_options call.
//
// Batches are borrowed: the caller keeps ownership, must not Release a batch
// while Push is running, and releases it afterwards as usual.
//
// Nulls follow the Arrow validity bitmap; no sentinel value is needed. The
// server itself stores a zero-length string or blob as null, whichever
// writer sent it, so an empty value with the validity bit set reads back as
// null.
type ArrowWriter struct {
	options WriterOptions
	tables  []arrowWriterTable // insertion order, names unique
}

// NewArrowWriter creates an Arrow writer with the given push options.
func NewArrowWriter(options WriterOptions) ArrowWriter {
	return ArrowWriter{options: options}
}

// NewArrowWriterWithDefaultOptions creates an Arrow writer with default options.
func NewArrowWriterWithDefaultOptions() ArrowWriter {
	return NewArrowWriter(NewWriterOptions())
}

// GetOptions returns the writer's push configuration.
func (w *ArrowWriter) GetOptions() WriterOptions {
	return w.options
}

// Length returns the number of staged tables.
func (w *ArrowWriter) Length() int {
	return len(w.tables)
}

// SetTable stages one table from one or more batches sharing a schema.
//
// The schema needs a "$timestamp" field of Arrow type timestamp (any unit)
// or date64 without nulls, no "$table" field, and data fields of type int64,
// float64, timestamp, date64, utf8 or binary. Zero-row batches are accepted.
//
// Returns qdb_e_invalid_argument for a bad name, batch set or "$timestamp"
// column and qdb_e_incompatible_type for an unsupported field type. Nothing
// is staged when an error is returned.
func (w *ArrowWriter) SetTable(name string, batches ...arrow.RecordBatch) error {
	if name == "" {
		return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "reason", "empty table name")
	}
	for _, t := range w.tables {
		if t.name == name {
			return wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table", "table", name, "reason", "already exists")
		}
	}
	err := validateArrowBatches(name, batches)
	if err != nil {
		return err
	}

	w.tables = append(w.tables, arrowWriterTable{name: name, batches: batches})

	return nil
}

// Push writes every staged table in one C call.
//
// Tables with zero rows in total are skipped; when nothing remains Push
// returns nil without calling the C API. On return, successful or not, the
// caller still owns every batch it passed to SetTable.
func (w *ArrowWriter) Push(h HandleType) error {
	// The C API drains every Arrow stream inside the call and reads our
	// buffers until it returns, so the body is one pin-export-call-unpin
	// sequence. Tables with zero rows are dropped first; when none remain
	// there is nothing to push and the C API is not called.
	//
	//  1. Retain and pin every buffer of every column of every batch.
	//     arrow-go's export stores the buffer addresses in C memory, which
	//     the cgo rules allow only for pinned memory; retaining also keeps
	//     the buffers alive if the caller releases a batch early.
	//  2. Allocate the zeroed C table array and fill name and dedup fields
	//     with qdb-allocated strings, as WriterTable.toNative does.
	//  3. Export one ArrowArrayStream per table into its zeroed slot.
	//  4. Convert options and make the single C call, during which arrow-go's
	//     stream callbacks run as C-to-Go callbacks.
	//  5. Release any stream the C side left unconsumed, then keep the Go
	//     side alive until here so step 1's pins cover the whole call.
	live := w.liveTables()
	if len(live) == 0 {
		return nil
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()
	var releases []func()
	defer func() {
		for _, f := range releases {
			f()
		}
	}()

	// 1. retain and pin buffers
	releases = append(releases, retainAndPin(&pinner, live))

	// 2. C table array
	tblPtr := qdbAllocBufferZeroed[C.qdb_exp_batch_push_arrow_t](h, len(live))
	releases = append(releases, releaseCPtr(h, unsafe.Pointer(tblPtr)))
	tbl := unsafe.Slice(tblPtr, len(live))
	for i := range live {
		rel, err := w.fillArrowTableOptions(h, live[i].name, &tbl[i])
		releases = append(releases, rel)
		if err != nil {
			return err
		}
	}

	// 3. export streams
	for i := range live {
		err := exportArrowStream(live[i].batches, &tbl[i].stream)
		if err != nil {
			return err
		}
	}

	// 4. the C call
	err := w.callPushArrow(h, tbl)

	// 5. release leftovers, keep alive
	for i := range tbl {
		releaseIfLive(&tbl[i].stream)
	}
	runtime.KeepAlive(live)

	return err
}

// liveTables returns the staged tables that have at least one row.
func (w *ArrowWriter) liveTables() []arrowWriterTable {
	live := make([]arrowWriterTable, 0, len(w.tables))
	for _, t := range w.tables {
		if t.rowCount() > 0 {
			live = append(live, t)
		}
	}

	return live
}

// fillArrowTableOptions sets name and dedup fields on one C table entry.
// The returned closure frees the strings it allocated.
func (w *ArrowWriter) fillArrowTableOptions(h HandleType, name string, out *C.qdb_exp_batch_push_arrow_t) (func(), error) {
	var releases []func()
	release := func() {
		for _, f := range releases {
			f()
		}
	}

	cName := qdbCopyString(h, name)
	releases = append(releases, releaseCPtr(h, unsafe.Pointer(cName)))
	setCPtr(unsafe.Pointer(&out.name), unsafe.Pointer(cName))

	// Truncate is not supported, as in WriterTable.toNative.
	setCPtr(unsafe.Pointer(&out.truncate_ranges), nil)
	out.truncate_range_count = 0

	out.deduplication_mode = C.qdb_exp_batch_deduplication_mode_t(w.options.dedupMode)
	if w.options.dedupMode == WriterDeduplicationModeUpsert && len(w.options.dropDuplicateColumns) == 0 {
		return release, wrapError(C.qdb_e_invalid_argument, "arrow_writer_push", "dedup_mode", "upsert", "reason", "missing drop duplicate columns")
	}

	count := len(w.options.dropDuplicateColumns)
	if count == 0 {
		setCPtr(unsafe.Pointer(&out.where_duplicate), nil)
		out.where_duplicate_count = 0

		return release, nil
	}

	ptr := qdbAllocBuffer[*C.char](h, count)
	releases = append(releases, releaseCPtr(h, unsafe.Pointer(ptr)))
	cols := unsafe.Slice(ptr, count)
	for i, col := range w.options.dropDuplicateColumns {
		cCol := qdbCopyString(h, col)
		releases = append(releases, releaseCPtr(h, unsafe.Pointer(cCol)))
		setCPtr(unsafe.Pointer(&cols[i]), unsafe.Pointer(cCol))
	}
	setCPtr(unsafe.Pointer(&out.where_duplicate), unsafe.Pointer(ptr))
	out.where_duplicate_count = C.qdb_size_t(count)

	return release, nil
}

// callPushArrow converts the options and makes the single C call.
func (w *ArrowWriter) callPushArrow(h HandleType, tbl []C.qdb_exp_batch_push_arrow_t) error {
	var options C.qdb_exp_batch_options_t
	options = w.options.setNative(options)

	var totalRows int64
	for _, t := range w.tables {
		totalRows += t.rowCount()
	}

	start := time.Now()
	errCode := C.qdb_exp_batch_push_arrow_with_options(
		h.handle,
		&options,
		&tbl[0],
		nil,
		C.qdb_size_t(len(tbl)),
	)
	if errCode == 0 {
		L().Info("wrote rows", "count", totalRows, "duration", time.Since(start))
	}

	return wrapError(errCode, "arrow_writer_push", "tables", len(tbl))
}
