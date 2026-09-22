# Plan: Arrow bulk reader (`Reader.Chunks` and `Reader.Arrow`)

Status: approved, ready to implement
Date: 2026-09-22
Owner: Leon. Branch `sc-19821/arrow-bulk-reader`, based on `master` at
`6f2a7ab` (QDB-19814). Story QDB-19821, epic 19491 (3.14.4 Release).

This file is temporary. It is removed in the last commit of the branch.

## Goal

Add two iterator methods to `Reader`. Both walk the same C bulk reader
cursor; they differ only in the shape of each batch.

```go
rd, err := qdb.NewReader(h, qdb.NewReaderOptions().WithTables([]string{"trades"}))
if err != nil {
    return err
}
defer rd.Close()

for rec, err := range rd.Arrow() {
    if err != nil {
        return err
    }
    err = ipc.NewWriter(w, ipc.WithSchema(rec.Schema())).Write(rec)
    rec.Release()
    if err != nil {
        return err
    }
}
```

`Reader.Arrow` is the bulk counterpart of `Query.FetchArrow`: same
`arrow-go` types, same ownership rule (caller owns the batch, releases it
once, batch outlives reader and handle). `Reader.Chunks` is the same
iterator over today's `ReaderChunk`.

`NewReader`, `ReaderOptions`, `Next`, `Batch`, `Err`, `FetchAll` and
`Close` stay. Whether the `Next`/`Batch`/`Err` family is removed later is a
separate story; this branch does not touch its behaviour.

## Decisions taken

- One `Reader` type, two iterator methods. No `ArrowReader` type, no
  `Reader[T]`. Chosen over the alternatives on 2026-09-22 because
  range-over-func binds the batch shape to a method, not to a type, and
  scopes the Arrow stream lifetime to the iteration.
- `iter.Seq2[T, error]`, error in-band. The last yielded pair carries the
  error and the sequence ends. No `Err()` needed for the new methods.
- One sequence per `Reader`. Starting a second sequence, or calling `Next`
  after a sequence started, yields `ErrInvalidIterator` from operation
  `reader_iterate`. The C cursor is single-pass; this makes it explicit.
- No `Schema()` accessor. Each batch carries its schema. An empty result
  yields nothing; callers needing a schema for zero rows use `ColumnsInfo`.
- `FetchAll` stays chunk-only. Merging Arrow batches into one copies; no
  consumer wants it.

## Context a cold session needs

Verified 2026-09-22 against the vendored C API in `./qdb` (3.15.0.dev0,
2026-09-09 nightly) and the quasardb source at `~/git/quasardb`, which sits
on a 3.14.2 branch: read C-side references with
`git show origin/master:<path>`.

### The C API surface

- `qdb/include/qdb/ts.h:1576`: `qdb_bulk_reader_fetch` opens the cursor.
  Already wrapped by `NewReader` (reader.go:346). Shared by both getters.
- `qdb/include/qdb/ts.h:1631`: `qdb_bulk_reader_get_data_arrow(reader,
struct ArrowArrayStream **data, qdb_size_t rows_to_get)`. Same
  `rows_to_get` semantics as `qdb_bulk_reader_get_data`; `0` reads all.
  Returns `qdb_e_iterator_end` with `*data == NULL` at end of data.
- Implementation: `qdb/client/bulk_reader_arrow.cpp:634` builds an
  `arrow::Table`, wraps it in `arrow::TableBatchReader`, exports it with
  `arrow::ExportRecordBatchReader` into a heap `ArrowArrayStream`, and
  tracks the tuple (stream struct, table, reader) on the client heap.
  Consequences:
  - One stream per C call, N record batches per stream. Today N is 1
    (builders finish single-chunk arrays); the contract is N.
  - The stream struct is freed with `qdb_release(handle, stream)`. The C++
    tests (`tests/integration/api/ts/bulk_reader_arrow.cpp:52`) call
    `stream->release(stream)` first, then `qdb_release`.
  - Exported arrays hold their own `shared_ptr<ArrayData>`. A batch stays
    valid after the stream, the tuple and the reader handle are released.
  - Empty result: `qdb_e_iterator_end` on the first call, no stream, so
    no schema.
- Layout (`ts.h:1550`): without requested columns the schema is `$table`,
  `$timestamp`, then every data column. With requested columns the order is
  exact and specials appear only when named. The regular getter's legacy
  layout prepends only `$table`; `$timestamp` is in `timestamps` there.
- Types (`bulk_reader_arrow.cpp:57` and `:465`): int64, float64,
  `timestamp[ns]` without zone, string and symbol as utf8, blob as binary.
  Data columns nullable with validity bitmaps. `$table` and `$timestamp`
  non-nullable. No `max_width` metadata. Both differ from
  `qdb_query_arrow`; consumers document the difference, no C change.

### The arrow-go side

`github.com/apache/arrow-go/v18` v18.8.0, already a dependency
(`query_arrow.go`).

- `cdata.ImportCRecordReader(stream, nil)` moves the C stream into a
  malloc'd copy owned by the returned reader (`ArrowArrayStreamMove`). The
  qdb struct is left released but must stay allocated until the reader is
  released: the tracked tuple owns the Arrow table, and `TableBatchReader`
  reads it by reference. Freeing early yields zero rows (verified
  2026-09-22 with a probe).
- The returned reader's `Next` releases the previous record before reading
  the next one (`cdata.go:1264`). A record handed to the caller must be
  `Retain`ed first.
- The returned reader must be `Release`d once; that frees the moved stream
  and its own copy of the struct.

### Existing Go code to reuse

- `reader.go:346` `NewReader`: validation and marshalling of tables,
  columns and range, then `qdb_bulk_reader_fetch`. Extracted to a helper
  in commit 2, unchanged in behaviour.
- `reader.go:540` `fetchBatch`: the regular getter. `Chunks` is built on
  it.
- `query_arrow.go`: cgo directive placement, `wrapError` operation naming,
  `setCPtr` for barrier-free C pointer stores, `releaseCPtr` for deferred
  frees, doc comment shape for the public Arrow entry point.
- `test_utils.go:558` `genPopulatedTables`, `:327` `pushWriterTables`,
  `:89` `newTestHandle`: the fixtures `reader_test.go` already uses.

## Interface specification

### `Reader.Chunks`

```go
// Chunks returns the remaining rows as batches of at most batchSize rows.
//
// Returns:
//
//	iter.Seq2[ReaderChunk, error]: one chunk per step; the last step
//	carries a non-nil error when the read failed
//
// One sequence per Reader. A second Chunks, Arrow or Next call after the
// first step yields ErrInvalidIterator. Breaking out of the loop is safe;
// the reader is still closed by Close.
func (r *Reader) Chunks() iter.Seq2[ReaderChunk, error]
```

Semantics:

- Each step is one `qdb_bulk_reader_get_data` call with
  `options.batchSize`. Chunks are Go-owned copies, as today.
- Ends when the C API returns `qdb_e_iterator_end`. That step is not
  yielded. An empty table yields nothing.
- On any other error the sequence yields `(ReaderChunk{}, err)` once and
  ends. Chunks already yielded are unaffected.
- `yield` returning false ends the sequence without further C calls.
- After the sequence ends, for any reason, `r.done` is true.

### `Reader.Arrow`

```go
// Arrow returns the remaining rows as Arrow record batches.
//
// Returns:
//
//	iter.Seq2[arrow.RecordBatch, error]: one batch per step; the last step
//	carries a non-nil error when the read or the Arrow import failed
//
// The caller owns every yielded batch and must Release it exactly once. A
// batch holds no reference to the reader or the handle and stays valid
// after the next step, after Close and after the handle is closed. On an
// error step the batch is nil.
//
// Schema: without WithColumns the fields are "$table" (utf8), "$timestamp"
// (timestamp[ns], no zone) and then every data column; with WithColumns
// the requested order, specials only when named. Data columns are
// nullable. Timestamps carry no zone, as in Query.FetchArrow.
//
// One sequence per Reader. A second Chunks, Arrow or Next call after the
// first step yields ErrInvalidIterator. Breaking out of the loop is safe;
// the reader is still closed by Close.
//
// Example:
//
//	for rec, err := range rd.Arrow() {
//	    if err != nil {
//	        return err
//	    }
//	    process(rec)
//	    rec.Release()
//	}
func (r *Reader) Arrow() iter.Seq2[arrow.RecordBatch, error]
```

Semantics:

- Each C call (`qdb_bulk_reader_get_data_arrow` with `options.batchSize`)
  yields one or more steps: every record batch in the returned stream, in
  order. Batches are never merged or split.
- Ends on `qdb_e_iterator_end`. Not yielded. An empty table yields nothing.
- Error in the C call, in the stream import, or from the stream's own
  `get_next`: the sequence yields `(nil, err)` once and ends. Nothing
  partially imported is handed out; it is released inside the sequence.
- `yield` returning false ends the sequence. The current stream reader is
  released before the sequence function returns, whether the loop finished,
  broke or returned.
- Row count over all batches equals the row count over `Chunks` on the
  same options.

### Internal functions (unexported, `reader_arrow.go`)

```go
// arrowStream is one C result: the record reader owning the moved stream
// and the release of the C allocation it came from.
type arrowStream struct {
    reader array.RecordReader
    free   func()
}

// fetchArrowStream runs one qdb_bulk_reader_get_data_arrow call and moves
// the result into an arrow-go record reader. Returns ErrIteratorEnd at end
// of data. The caller drains the stream with drain, which releases it.
func (r *Reader) fetchArrowStream() (arrowStream, error)

// drain yields every record, retaining each before handing it out.
// Returns false when yield asked to stop or an error step was yielded.
// Releases the reader, then frees the C allocation, before returning.
func (s arrowStream) drain(yield func(arrow.RecordBatch, error) bool) bool
```

Ownership at the CGO boundary, documented in code as in `query_arrow.go`:

1. `qdb_bulk_reader_get_data_arrow` allocates the stream struct on the
   client heap, tracked together with the Arrow table it reads.
2. `cdata.ImportCRecordReader` moves it; the source is now released but
   still allocated.
3. Each record from the arrow-go reader is `Retain`ed, then yielded. The
   caller's `Release` is the one that frees the buffers.
4. When the stream is drained or the loop stops: the arrow-go reader is
   `Release`d, then `qdbReleasePointer(handle, stream)` frees the struct
   and the tracked tuple. Records already yielded own their buffers and
   are unaffected.

### Errors

Only `wrapError`; no `fmt.Errorf` in new code. Operation names:

| Operation             | Where                                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| `reader_iterate`      | second sequence, code `ErrInvalidIterator`                                                              |
| `reader_fetch_arrow`  | the C getter                                                                                            |
| `reader_arrow_import` | `ImportCRecordReader` and stream `get_next`, code `ErrIncompatibleType`, detail from the arrow-go error |

`NewReader` on a missing table already fails with an error matching
`errors.Is(err, ErrAliasNotFound)`; a test pins it.

### cgo directives

```go
// #cgo noescape qdb_bulk_reader_get_data_arrow
// #cgo nocallback qdb_bulk_reader_get_data_arrow
```

The stream callbacks are C-to-C; `nocallback` holds. Add the same pair for
`qdb_bulk_reader_get_data` if missing (check in commit 2).

## Commit plan

Each commit builds, passes lint on touched files and passes the tests it
adds. Messages are one line, `type(scope): subject`, no trailers, no body.
Push after every commit; the PR webhook builds each push.

1. `docs(reader): add arrow reader plan` (this file).
2. `refactor(reader): extract cursor open from NewReader`. Move the
   validation and marshalling in `NewReader` into an unexported helper
   returning `C.qdb_reader_handle_t`. No behaviour change. Add the
   `iterating` guard field and the cgo directive pair for the regular
   getter if absent. Existing tests pass unchanged.
3. `feat(reader): add Chunks iterator`. `Reader.Chunks` over `fetchBatch`,
   the single-sequence guard applied to `Chunks` and `Next`. Tests: same
   rows as `FetchAll`; empty table yields nothing; early break then Close
   is clean; second sequence yields `ErrInvalidIterator`.
4. `feat(reader): add arrow stream fetch`. New `reader_arrow.go` with the
   cgo block, `fetchArrowStream`, `yieldArrowStream`. Unit-level tests
   through a temporary unexported call path: one stream imported and
   released, batch valid after release, `ErrIteratorEnd` on empty.
5. `feat(reader): add Arrow iterator`. `Reader.Arrow` and its doc comment.
   Tests: layout without and with `WithColumns` (specials named, shuffled
   order); per-type values and nulls against `genPopulatedTables`, symbol
   as utf8, blob as binary, validity bitmaps; small `WithBatchSize`
   producing several steps with total rows equal to `Chunks`; batch valid
   after next step, after Close, after handle Close; Release exactly once;
   missing table pins `ErrAliasNotFound`.
6. `test(reader): run arrow reader under cgocheck2`. Confirm
   `GOEXPERIMENT=cgocheck2` is clean for `TestReaderArrow*`; fix anything
   it finds in the same commit.
7. `docs(reader): document iterators`. README reader section gains the two
   loops; `CLAUDE.md` key components line for `reader.go` mentions
   `reader_arrow.go`. Run `npx prettier --write` on every touched
   Markdown file.
8. `chore(reader): remove arrow reader plan`. Delete this file.

## Documentation discipline

- Every exported identifier has a doc comment starting with its name.
  Public entry points use the `query_arrow.go` shape: one-sentence summary,
  `Returns:` block, ownership paragraph, `Example:` block.
- Comments state what the code does and what the reader must know:
  lifetimes, ownership, layout, error codes. No history, no rationale
  narration, no restating the code.
- Every C pointer crossing has a comment naming who allocates, who
  releases, and when it becomes invalid.
- Unsafe or barrier-sensitive stores use the existing helpers (`setCPtr`,
  `releaseCPtr`) and reference them by name in the comment, as
  `reader.go:410` does.
- Function length under 40 lines. New code uses `wrapError` only.

## Verification per commit

```bash
direnv exec . go build ./...
direnv exec . env GOTOOLCHAIN=go1.26.2 golangci-lint run --timeout=5m ./*.go
direnv exec . go test -run 'TestReader' ./...
direnv exec . env GOEXPERIMENT=cgocheck2 go test -run 'TestReaderArrow' ./...
```

Lint: judge by touched files only; master carries pre-existing issues.
Never run `--fix` over `./*.go`, it rewrites baseline files. Tests need the
daemons from `bash scripts/tests/setup/start-services.sh` (insecure on
`127.0.0.1:2836`); if a secure cluster is already running from another
checkout, copy its keys, do not restart it.

## GitHub and Buildkite protocol

1. Open the PR after commit 2 is pushed, so every later push builds:

   ```bash
   gh pr create -B master -t "QDB-19821 - Arrow bulk reader" -b "" -r igorniebylski
   ```

   Title format is fixed; body empty; reviewer Igor.

2. The PR webhook starts a build on pipeline `qdb-api-go` (org
   `quasar-1`) for each push. No manual `bk build create`; if one is ever
   needed it takes `-i` and the full commit SHA.
3. Monitor:

   ```bash
   bk build watch -p qdb-api-go -b sc-19821/arrow-bulk-reader
   bk build view <n> -p qdb-api-go --json
   ```

   Done only when every job is `passed`. If jobs sit queued,
   `bk job reprioritize` bumps them.

4. Before requesting review: commit 8 is pushed, the last build is green,
   lint on touched files is clean, and this file is gone.
5. After squash-merge: cherry-pick the squash commit to `3.14.x`, and send
   the merged master SHA to the `qdb-api-rest` session, which bumps its
   vendored `github.com/bureau14/qdb-api-go/v3`.

## Coordination

- A sibling session adds read accessors to `ReaderChunk` on
  `feat/reader-chunk` (about 20 additive lines after `RowCount`,
  reader.go:109). It merges first; this branch rebases. No overlap with
  commit 2, which touches `NewReader` only.
- `qdb-api-rest` will consume `Reader.Arrow` for a streamed table dump
  endpoint: one batch per step, caller-owned, Arrow IPC via arrow-go. It
  designed against `Next`/`Batch`/`Err`; tell it the final shape is
  `for rec, err := range rd.Arrow()`.
