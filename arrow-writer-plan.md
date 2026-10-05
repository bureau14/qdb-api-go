# Plan: Arrow batch writer (`ArrowWriter`)

Status: ready to implement
Date: 2026-10-05
Owner: Leon. Branch `sc-19837/arrow-batch-writer`, based on `master` at
`abadf4e` (QDB-19821). Ticket QDB-19837, epic 19491 (3.14.4 Release).

## Goal

Add a writer next to `Writer` that pushes `arrow.RecordBatch` tables
through the C API's `qdb_exp_batch_push_arrow_with_options`, zero-copy
over the caller's Arrow buffers, in one C call per `Push`.

```go
w := qdb.NewArrowWriter(qdb.NewWriterOptions().WithFastPush())

// One table, one or more batches sharing a schema. Batches are borrowed.
if err := w.SetTable("trades", rec1, rec2); err != nil { ... }
if err := w.SetTable("quotes", rec3); err != nil { ... }

// One C call. After it returns the caller still owns rec1..rec3.
if err := w.Push(handle); err != nil { ... }
```

`Writer`, `WriterTable`, `ColumnData` and `WriterOptions` stay as they
are. `WriterOptions` is reused unchanged as the options type.

Consumer: the qdb-api-rest rewrite (branch `sc-19567/rest-rewrite`),
which decodes CSV, NDJSON and Arrow IPC bodies into one
`arrow.RecordBatch` per table and wants multi-table pushes, borrow
ownership, validity nulls and the existing `WriterOptions` mapping. Both
REST sessions reviewed the surface below on 2026-09-24 and accepted it.

## Context a cold session needs

Verified 2026-09-24 and re-checked 2026-10-05. The vendored C library in
`qdb/lib/libqdb_api.dylib` exports `qdb_exp_batch_push_arrow` and
`qdb_exp_batch_push_arrow_with_options`. The quasardb checkout at
`~/git/quasardb` is on `3.14.2-test-runner`; every C-side claim below was
read from `origin/master` with `git show origin/master:<path>`.

### The C API surface (`qdb/include/qdb/ts.h`)

- `ts.h:1450` `qdb_exp_batch_push_arrow_with_options(handle, const
qdb_exp_batch_options_t *options, qdb_exp_batch_push_arrow_t *tables,
const qdb_exp_batch_push_table_schema_t **table_schemas, qdb_size_t
table_count)`. `tables` is non-const and documented as "Move" logic: the
  stream is consumed inside the call.
- `ts.h:609` `qdb_exp_batch_push_arrow_t` is `{ const char *name; struct
ArrowArrayStream stream; const qdb_ts_range_t *truncate_ranges;
qdb_size_t truncate_range_count; qdb_exp_batch_deduplication_mode_t
deduplication_mode; const char **where_duplicate; qdb_size_t
where_duplicate_count; }`. Same dedup and truncate fields as
  `qdb_exp_batch_push_table_t`, minus `creation`.
- `ts.h:585` `qdb_exp_batch_options_t` is `{ mode; push_flags }`, shared
  with the columnar push.
- `arrow_abi.h` vendors the Arrow C Data Interface structs. The stream is
  the standard `ArrowArrayStream` with `get_schema`, `get_next`,
  `get_last_error`, `release`, `private_data`.

### What the C side does with the stream

`qdb/client/batch_table_push.cpp:577` `ts_batch_arrow_stream_push`:

1. Per table: `arrow::ImportRecordBatchReader(&tab.stream)`, then
   `ToTable()`, then `CombineChunks()`. The stream is drained completely
   here, on the calling thread, before anything is sent. Several batches
   per table are concatenated.
2. `extract_arrow_columns` walks the combined table's fields. The field
   named `$timestamp` (by name, any position) becomes the row index; every
   other field becomes a `qdb_exp_batch_push_column_t`. `$table` is not
   special: it would be sent as a data column and fail server-side.
3. Each field goes through `arrow::ImportArray` into a holder
   (`qdb/client/arrow_data_holder.hpp:316`), switching on the Arrow type:

   | Arrow type                   | qdb column       | Null handling                                                             |
   | ---------------------------- | ---------------- | ------------------------------------------------------------------------- |
   | INT64                        | int64            | zero-copy span if `null_count == 0`, else copy with none sentinel         |
   | DOUBLE                       | double           | same as int64                                                             |
   | TIMESTAMP (s, ms, us, ns)    | timestamp        | converted to `qdb_timespec_t`, null -> `qdb_min_timespec`                 |
   | DATE64                       | timestamp        | milliseconds, same conversion                                             |
   | STRING (utf8, int32 offsets) | string or symbol | `qdb_string_t` pointing into the Arrow data buffer; null -> none sentinel |
   | BINARY (int32 offsets)       | blob             | same as string                                                            |
   | anything else                | error            | `qdb_e_not_implemented`, "Unsupported arrow::Array type"                  |

   Timezone metadata on TIMESTAMP is ignored. LARGE_STRING, LARGE_BINARY,
   STRING_VIEW, dictionary and nested types are all rejected.

4. The result is handed to the columnar `ts_batch_table_push`, so
   validation, dedup, sorting and sending are identical to `Writer.Push`.
   `verify_timestamps` rejects negative `tv_sec`, so a null `$timestamp`
   (converted to min time) fails with `qdb_e_invalid_argument`. No silent
   row drop.
5. Holders are destroyed when the function returns, which calls the Arrow
   `release` callbacks on every imported array and the stream. Nothing of
   the caller's is referenced after return.

Consequences for Go: all Go memory behind the batch must stay valid and
pinned for the duration of the single C call, and no longer. Multiple
batches per table are fine. Column sets may differ per table; the
same-schema rule in `Writer.SetTable` (`writer.go:76`) is a Go-side
restriction and does not apply here.

### Zero rows

Not yet verified against the server. `ts_batch_table_push` skips tables
with `row_count == 0` in the null-timestamp check, and `verify_table`
accepts zero rows. The REST consumer asked that zero-row tables be
skipped, not refused. Decision: `SetTable` accepts zero-row batches;
`Push` drops tables with zero rows in total before building the C call,
and returns nil without calling C when nothing remains. This differs from
`Writer.Push`, which refuses an empty push (`writer.go:144`). A test pins
the behaviour either way.

### arrow-go and the cgo pointer rules

This is the one non-obvious part. arrow-go v18.8.0 (the newest v18
release, already in `go.mod`) provides `cdata.ExportRecordReader`, which
fills an `ArrowArrayStream` whose callbacks export batches on demand. The
array export, `arrow/cdata/cdata_exports.go:388`, stores the address of
each Go buffer into a `calloc`'d C array:

```go
cBufs[i] = (*C.void)(unsafe.Pointer(&buf.Bytes()[0]))
```

without pinning. The cgo rules (`go doc cmd/cgo`, "Passing pointers")
forbid storing an unpinned Go pointer in C memory. arrow-go keeps the
buffers alive through `ArrayData.Retain()` plus a `cgo.Handle` in
`private_data`, and relies on the Go heap being non-moving, so it works
under the default `GODEBUG=cgocheck=1`, which only inspects call
arguments. Under `GOEXPERIMENT=cgocheck2`, which instruments pointer
stores, it aborts:

```
write of unpinned Go pointer 0x... to non-Go memory 0x...
fatal error: unpinned Go pointer stored into non-Go memory
  arrow/cdata/cdata_exports.go:388 exportArray
```

Upstream: apache/arrow-go issue #70, "C Data Interface implementation
violates cgo rules by default", closed as not planned.

The runtime check (`runtime/cgocheck.go`, `cgoCheckPtrWrite`) returns
early when `isPinned(src)`. So the fix is on our side and small: pin the
backing array of every buffer of every column before calling
`cdata.ExportRecordReader`, keep the pinner alive across the C call,
unpin after. Verified 2026-10-05 in a scratch module: the same export
passes cgocheck2 once the buffers are pinned, with the default Go
allocator. `Pinner.Pin` on a pointer outside the Go heap (buffers that
came from `Reader.Arrow()` or `FetchArrow`, or from `mallocator`) is a
silent no-op (`runtime/pinner.go`, `setPinned` with `span == nil`), so
one code path covers every allocator. Callers need no special allocator.

This is the same five-phase pattern `Writer.Push` already uses for
int64, double and timestamp columns; only the party writing the pointers
into C memory differs.

### Callbacks

Because the C side drains the stream inside the call, arrow-go's
`streamGetSchema`, `streamGetNext`, `releaseExportedArray` and
`streamRelease` run as C-to-Go callbacks during
`qdb_exp_batch_push_arrow_with_options`. Therefore:

- `#cgo noescape qdb_exp_batch_push_arrow_with_options` yes (nothing is
  retained after return).
- `#cgo nocallback` must NOT be set for this function.
- CLAUDE.md's list of functions that call back into Go (`qdb_query_continuous`,
  `qdb_log_add_callback`) grows by one. Update it in the same PR.

### Ownership

Borrow. The caller keeps its batches and releases them after `Push`
returns. `Push` retains each batch for the duration of the call and
releases its own reference afterwards, so a caller that releases early
by mistake does not free buffers under C. The exported stream, schema
strings and C arrays are allocated and freed by arrow-go through the
`release` callbacks the C side invokes. The `qdb_exp_batch_push_arrow_t`
array, table names and `where_duplicate` strings are qdb-allocated and
released by `Push` with the existing `releaseCPtr` closures.

If the C call fails before draining a stream, its `release` is still
called by `ImportRecordBatchReader`'s error path or by the holder
destructors; if `ImportRecordBatchReader` itself fails the stream is left
intact. `Push` therefore calls `cdata.ReleaseCArrowArrayStream` on every
stream whose `release` is still non-NULL after the call, before freeing
the table array.

### Null semantics compared with `Writer`

The columnar writer encodes null as a sentinel (`MinInt64`, `NaN`, empty
string, nil blob, `NullTime`). The Arrow path uses the validity bitmap,
so an empty string or empty blob with the validity bit set is a value,
not a null. State this in the `ArrowWriter` doc comment.

### Pre-existing gap, out of scope

`WriterOptions.setNative` (`writer_options.go:206`) writes only `mode`
into `qdb_exp_batch_options_t`; `push_flags` is never copied, so
`EnableWriteThrough` and `EnableAsyncClientPush` are no-ops for `Writer`
today. Fixing it changes `Writer` behaviour (write-through is the
default). Separate story; `ArrowWriter` calls the same `setNative` so it
inherits whichever behaviour is current.

## Design

### Public surface (new file `writer_arrow.go`)

```go
// ArrowWriter stages arrow.RecordBatch tables and pushes them to the
// server in one qdb_exp_batch_push_arrow_with_options call.
//
// Batches are borrowed: the caller keeps ownership, must not Release a
// batch while Push is running, and releases it afterwards as usual.
//
// Nulls follow the Arrow validity bitmap. Unlike Writer, which encodes
// null as a per-type sentinel, an empty string or empty blob with the
// validity bit set is a value.
type ArrowWriter struct {
    options WriterOptions
    tables  []arrowWriterTable   // insertion order, names unique
}

type arrowWriterTable struct {
    name    string
    batches []arrow.RecordBatch
}

func NewArrowWriter(options WriterOptions) ArrowWriter
func NewArrowWriterWithDefaultOptions() ArrowWriter
func (w *ArrowWriter) GetOptions() WriterOptions
func (w *ArrowWriter) Length() int

// SetTable stages one table from one or more batches sharing a schema.
//
// The schema needs a "$timestamp" field of Arrow type timestamp (any
// unit) or date64 without nulls, no "$table" field, and data fields of
// type int64, float64, timestamp, date64, utf8 or binary. Zero-row
// batches are accepted.
//
// Returns qdb_e_invalid_argument for a bad name, batch set or
// "$timestamp" column and qdb_e_incompatible_type for an unsupported
// field type. Nothing is staged when an error is returned.
func (w *ArrowWriter) SetTable(name string, batches ...arrow.RecordBatch) error

// Push writes every staged table in one C call.
//
// Tables with zero rows in total are skipped; when nothing remains Push
// returns nil without calling the C API. On return, successful or not,
// the caller still owns every batch it passed to SetTable.
func (w *ArrowWriter) Push(h HandleType) error
```

A separate type rather than new methods on `Writer`: one push is one C
call and the two column formats cannot share a transaction, and keeping
`writer.go` untouched keeps backwards compatibility trivial.

### Documentation shape

Follow the documentation discipline used in qdb-api-rest
(`.claude/skills/doc-discipline`): the doc comment above a function is
the contract only; a function that encodes a rule, an algorithm or an
invariant gets an overview comment at the top of its body and short
step comments carrying the overview's numbers; everything else stays
bare. Applied to this file:

| function                                                                         | shape    | the body documents                                                                                                                |
| -------------------------------------------------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `ArrowWriter` type                                                               | contract | borrow rule, one-C-call guarantee, validity-null difference from `Writer` (above)                                                 |
| `NewArrowWriter*`, `GetOptions`, `Length`, `liveTables`, `fillArrowTableOptions` | bare     | nothing; name and body say everything                                                                                             |
| `SetTable`                                                                       | narrated | validation order as a numbered overview, since the order decides which error a caller sees                                        |
| `validateArrowSchema`, `findTimestampField`, `arrowTypeSupported`                | narrated | the `$timestamp` and `$table` rules and the accepted type set, each pointing at the C-side source (`arrow_data_holder.hpp:316`)   |
| `Push`                                                                           | narrated | the full process overview below, then one-line step labels                                                                        |
| `pinRecordBuffers`                                                               | narrated | why pinning makes arrow-go's store legal (`runtime/cgocheck.go` `cgoCheckPtrWrite`, `isPinned`), why `Pin` on C memory is a no-op |
| `exportArrowTable`                                                               | narrated | the zeroed-memory requirement of `cdata`, who owns the stream after export, why `rr.Release()` directly after export is safe      |
| `releaseIfLive`                                                                  | narrated | which C error paths leave a stream unconsumed and why `release` must then run on the Go side                                      |
| cgo preamble                                                                     | comment  | why `nocallback` is absent: the stream callbacks run during the call                                                              |

Every "why" in those comments has its evidence in this plan (the C
source lines and the cgocheck2 experiment under "Context"); the comment
states the reason, not the evidence.

### Validation in `SetTable` (before any C call)

All with `wrapError(C.qdb_e_invalid_argument, "arrow_writer_set_table",
"table", name, "reason", ...)` unless noted:

- name non-empty and not already staged
- at least one batch
- every batch's schema `Equal` to the first
- a field named `$timestamp` of type TIMESTAMP (any unit) or DATE64
- no field named `$table`
- every other field of type INT64, FLOAT64, TIMESTAMP, DATE64, STRING or
  BINARY; otherwise `C.qdb_e_incompatible_type` with the field name and
  Arrow type string
- `$timestamp` column `NullN() == 0` in every batch

Validation lives in small pure functions (`validateArrowSchema`,
`arrowTypeSupported`, `findTimestampField`) so tests can hit them
without a server.

### `Push` phases (mirrors `Writer.Push`)

```go
func (w *ArrowWriter) Push(h HandleType) error {
    // The C API drains every Arrow stream inside the call and reads our
    // buffers until it returns, so the body is one pin-export-call-unpin
    // sequence. Tables with zero rows are dropped first; when none remain
    // there is nothing to push and the C API is not called.
    //
    //  1. Retain and pin every buffer of every column of every batch.
    //     arrow-go's export stores the buffer addresses in C memory, which
    //     the cgo rules allow only for pinned memory; pinning also keeps
    //     the buffers alive even if the caller releases a batch early.
    //     Pin is a no-op for buffers already in C memory.
    //  2. Allocate the zeroed C table array and fill name and dedup fields
    //     with qdb-allocated strings, as WriterTable.toNative does.
    //  3. Export one ArrowArrayStream per table into the zeroed stream
    //     slot, which cdata requires. The stream holds its own reference
    //     to the record reader, so ours is released at once.
    //  4. Convert options and make the single C call. arrow-go's stream
    //     callbacks run as C-to-Go callbacks during this call, which is
    //     why the function has no nocallback directive.
    //  5. Release any stream the C side left unconsumed (an error before
    //     ImportRecordBatchReader leaves it intact), then keep the Go
    //     side alive until here so step 1's pins cover the whole call.
    live := w.liveTables()
    if len(live) == 0 { return nil }

    var pinner runtime.Pinner
    defer pinner.Unpin()
    var releases []func()
    defer runReleases(&releases)

    // 1. retain and pin buffers
    for _, t := range live { retainAndPin(&pinner, &releases, t.batches) }

    // 2. C table array
    tbl := qdbAllocBufferZeroed[C.qdb_exp_batch_push_arrow_t](h, len(live))
    releases = append(releases, releaseCPtr(h, unsafe.Pointer(tbl)))
    ...

    // 3. export streams
    rr, _ := array.NewRecordReader(schema, t.batches)
    cdata.ExportRecordReader(rr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&tbl[i].stream)))
    rr.Release()

    // 4. the C call
    opts := w.options.setNative(C.qdb_exp_batch_options_t{})
    errCode := C.qdb_exp_batch_push_arrow_with_options(h.handle, &opts, &tbl[0], nil, C.qdb_size_t(len(live)))

    // 5. release leftovers, keep alive
    for i := range tbl { releaseIfLive(&tbl[i].stream) }
    runtime.KeepAlive(live)
    return wrapError(errCode, "arrow_writer_push", "tables", len(live))
}
```

Functions stay under 40 lines by splitting: `liveTables`, `retainAndPin`,
`pinRecordBuffers`, `exportArrowTable`, `fillArrowTableOptions`,
`releaseIfLive`. Dedup columns are copied with `qdbCopyString` into a
qdb-allocated `const char **`, exactly as `WriterTable.toNative` does
(`writer_table.go:354`).

`pinRecordBuffers` walks `col.Data().Buffers()` and also children and
dictionary data for completeness, even though validation already limits
columns to flat types. Pins `&buf.Bytes()[0]` for every non-nil,
non-empty buffer.

### cgo preamble

```go
/*
    #include <qdb/client.h>
    #include <qdb/ts.h>

    #cgo noescape qdb_exp_batch_push_arrow_with_options
*/
import "C"
```

No `nocallback`, with a comment stating why and pointing at the stream
callbacks.

### Logging and errors

`Push` logs at Info like `Writer.Push` ("wrote rows", total rows,
duration). Every error goes through `wrapError`, so `errors.Is(err,
ErrAliasNotFound)` holds for an unknown table and the three predicates
behave as for `Writer`.

## Tests (`writer_arrow_test.go`)

Follow `reader_arrow_test.go` structure: `rapid.Check` with
`newTestHandle`, `WithGCAndHandle` around the body, cleanup via
`t.Cleanup`. Generators: `genArrowSchema(rt)` draws a column set over the
six types, `genArrowBatches(rt, schema, rows, parts)` draws values and
validity and splits them into batches, `readBackArrow(t, h, table)`
drains `Reader.Arrow()`. Three tests, the first two generative.

1. `TestArrowWriterRoundTrip`. Per run draw: number of tables (1-3), a
   column set per table (sets may differ across tables), a row count per
   table including zero, a batch count (1-4) the rows are split into,
   per-cell validity including empty strings and empty blobs with the
   bit set, a `$timestamp` unit from {s, ms, us, ns, date64}, a push mode
   from {transactional, fast, async}, and a dedup setting from {off, drop,
   upsert} with explicit columns. Push, release the batches, read back
   through `Reader.Arrow()` and through `FetchArrow`, and compare cell by
   cell against the drawn data with the `assertArrowCellEquals...`
   helpers. Properties checked in the same run: zero-row tables are
   absent from the read-back and do not fail the push; a push with only
   zero-row tables returns nil; multiple batches concatenate; pushing the
   same data a second time under drop or upsert leaves the drawn row
   count. Finally re-push a batch obtained from `Reader.Arrow()`
   (C-allocated buffers) into a fresh table and read it back, which
   covers the no-op `Pin` path.
2. `TestArrowWriterRejectsInvalidInput`, no server. Draw a valid schema
   and batch, then one mutation from: drop `$timestamp`, retype it, inject
   a null into it, add `$table`, retype a data column to large_utf8,
   dictionary or list, make two batches' schemas differ, reuse a staged
   table name, use an empty name. Assert `SetTable` returns the expected
   `ErrorType` and that `Length()` is unchanged.
3. `TestArrowWriterUnknownTableIsAliasNotFound`: one fixed case,
   `errors.Is(err, ErrAliasNotFound)`.

The `Arrow` tests additionally run under `direnv exec . env
GOEXPERIMENT=cgocheck2 go test -run Arrow ./...` and under
`GODEBUG=invalidptr=1,cgocheck=1`; that is a build step (step 6 below),
not a test.

## Steps

1. `writer_arrow.go`: types, constructors, `SetTable` with validation.
2. `writer_arrow_test.go`: rejection tests (no server), green.
3. `Push`: phases above, one table, int64 only; round-trip test green.
4. All types, nulls, units, multi-batch, multi-table, zero rows.
5. Dedup and mode tests; error mapping test.
6. cgocheck2 run; lint with `GOTOOLCHAIN=go1.26.2 golangci-lint run
--timeout=5m ./*.go`, zero new issues on touched files.
7. CLAUDE.md: add `qdb_exp_batch_push_arrow_with_options` to the callback
   list; note the null-semantics difference in the `ArrowWriter` doc.
8. Remove this plan file in a final commit before the PR is marked ready,
   as was done for `arrow-reader-plan.md`.
9. PR `QDB-19837 - Arrow batch writer`, reviewer igorniebylski, Buildkite
   pipeline `qdb-api-go`; reprioritize scheduled jobs after it starts.
10. After squash-merge: cherry-pick to `3.14.x`; tell the REST session the
    commit SHA to vendor.

## Open decisions for Leon

- Zero-row tables skipped silently (proposed) versus a sentinel error.
- Whether to fix `setNative` push_flags in its own story before or after
  this one.
- Whether to file the pinning fix upstream on apache/arrow-go issue #70.
