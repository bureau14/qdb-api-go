# Plan: Arrow <-> QuasarDB column type mapping

Status: ready for review
Date: 2026-10-06
Owner: Leon. Branch `sc-19912/arrow-to-quasardb-column-type-mapping`, based
on `master` at `f221349` (QDB-19837). Ticket QDB-19912.

## Goal

Give callers one canonical, public answer to two questions that today are
answered by private copies in test code and in qdb-api-rest:

1. Which Arrow type does a QuasarDB column type come back as from
   `Reader.Arrow` and `Query.FetchArrow`?
2. Which QuasarDB column type does an Arrow field map to when pushed
   through `ArrowWriter`?

```go
// Forward: total over valid column types, nil for an invalid one.
dt := qdb.TsColumnDouble.ArrowType()            // float64

// Reverse: errors on anything the writer would refuse.
ct, err := qdb.ColumnTypeOfArrow(field.Type)   // TsColumnDouble
```

## Context a cold session needs

### Consumers

- qdb-api-rest (branch `sc-19567/rest-rewrite`, `internal/qdb/read.go`)
  keeps a hand-rolled `map[TsColumnType]arrow.DataType` to predict the
  reader's schema for empty tables and to type ingest bodies. Its test
  fixture builds record batches in the reader's exact Arrow types with no
  cluster at hand. It needs the forward direction only, as a method,
  deterministic, no error path, and compares with `arrow.TypeEqual` and
  `Schema.Equal`, so the answer must be identical to what the C side
  emits.
- This repo's own tests carry the same mapping three times:
  `writer_arrow_test.go:56` (`arrowDataType`), `reader_arrow_test.go:157`
  (`arrowTypeIDOf`), and inline assertions in `query_arrow_test.go`.
- The reverse direction has no consumer today. It is added because it is
  small, and because `validateArrowSchema` in `writer_arrow.go` already
  encodes it as an accept-list of Arrow type IDs; folding that list into
  the mapping removes one place where the two can drift.

### What the C side emits (forward direction, verified)

The reader and query paths import schemas straight from C; there is no
Go-side mapping to keep in sync, only to describe. From
`reader_arrow_test.go:128` and `query_arrow_test.go:38`:

| TsColumnType | Arrow type             |
| ------------ | ---------------------- |
| Int64        | int64                  |
| Double       | float64                |
| Timestamp    | timestamp[ns], no zone |
| Blob         | binary                 |
| String       | utf8                   |
| Symbol       | utf8 (plain, not dict) |

Not part of the mapping, but part of the reader's schema and worth stating
in doc comments: `$table` is utf8 non-nullable, `$timestamp` is
timestamp[ns] non-nullable, data columns are nullable, and utf8/binary
fields carry `max_width` field metadata attached by C.

### What the C side accepts (reverse direction, verified)

`writer_arrow.go:33` lists what `make_arrow_holder` in the C client takes:
int64, float64, timestamp (any unit), date64, utf8, binary. Anything else
is `qdb_e_not_implemented` on the C side and `qdb_e_incompatible_type` in
`validateArrowSchema`. Timestamps with a zone are accepted by the writer
today; validation checks only the type ID. Nothing in the push path reads
the zone, so the value is taken as-is.

## Design decisions

- Forward is a method, `func (v TsColumnType) ArrowType() arrow.DataType`.
  Total over the six valid types, returns nil for `TsColumnUninitialized`
  or any other invalid value. Never panics (project rule), and the REST
  side accepts nil for invalid input.
- The timestamp instance is one package-level `var`, `arrowTimestampNs`,
  so `ArrowType()` answers are pointer-stable and `arrow.TypeEqual`
  trivially holds. This is the same shape arrow-go uses for
  `arrow.FixedWidthTypes.Timestamp_ns`; we do not reuse that one because
  its zone is `"UTC"` and the C side emits none.
- Reverse is a free function, `func ColumnTypeOfArrow(dt arrow.DataType) (TsColumnType, error)`.
  It is partial, so it returns an error, built with `wrapError` on
  `qdb_e_incompatible_type` with the Arrow type string as context, the same
  code `validateArrowSchema` uses. Free function rather than a method
  because `arrow.DataType` is not ours.
- Reverse policy mirrors the writer's accept-list exactly, nothing wider:
  - int64 -> Int64; float64 -> Double; binary -> Blob; utf8 -> String
  - timestamp (any unit, with or without zone) -> Timestamp; date64 ->
    Timestamp. The doc comment states that a zone is dropped and the value
    taken as-is, which is what the push does.
  - Symbol is never inferred; utf8 is String. Doc comment says so.
  - int32, float32, date32, large_utf8, large_binary, dictionary, lists:
    error. No silent widening, matching what the REST ingest wants
    (refuse loudly) and what the C side does.
- `validateArrowSchema` is rewritten on top of `ColumnTypeOfArrow` so the
  accept-list `arrowSupportedTypes` and `arrowTypeSupported` go away.
  `arrowTimestampTypeSupported` stays, since the index column has a
  narrower rule (timestamp or date64 only) than data columns.
- Both functions live in `entry_timeseries_common.go` next to
  `AsValueType` and `asWriterDataType`, after them in book order. That
  file gains an arrow-go import; no new source file.
- Names: `ArrowType` is what the REST side asked for. `ColumnTypeOfArrow`
  reads as "column type of (this) Arrow type" and keeps the `TsColumn`
  vocabulary out of a name that already says column.

## Non-goals

- A whole-table schema builder (`ArrowSchemaOf([]TsColumnInfo)`). The REST
  side wants it only if byte-equal to the reader's output including
  `max_width` metadata, which only C knows. Separate story if wanted.
- Mapping to or from string names ("int64", "double", ...). REST keeps
  its own vocabulary.
- Nullability. Not a property of the type.
- Any change to what the C side emits or accepts.

## Documentation discipline

- Every exported identifier gets a doc comment with one example, in the
  style of `Fetch` in `query_result_convert.go:353`.
- Comments are factual and terse: which type, why that one, what is
  dropped. No history, no "we decided".
- The forward doc comment names the two producers it describes
  (`Reader.Arrow`, `Query.FetchArrow`) and the reader's schema conventions
  listed above so a caller composing a schema has them in one place.

## Commits, in order

Each commit builds, lints clean on touched files (`GOTOOLCHAIN=go1.26.2`,
see memory), and passes `direnv exec . go test -run 'Arrow' ./...` with
`GOEXPERIMENT=cgocheck2`. One line, `type(scope): subject`.

1. `feat(timeseries): map column types to and from arrow types`

   `entry_timeseries_common.go`: add `arrowTimestampNs`, `ArrowType`,
   `ColumnTypeOfArrow`. `exhaustive` lint forces every `TsColumnType` and
   every `arrow.Type` case to be named or defaulted; use a default with
   the error for the Arrow switch.

   New test file `entry_timeseries_arrow_test.go`, cluster-free:

   - forward table: each valid type to its expected Arrow type via
     `arrow.TypeEqual`; `TsColumnUninitialized` to nil.
   - round trip: for every valid type `t`,
     `ColumnTypeOfArrow(t.ArrowType())` is `t`, except Symbol gives String.
   - reverse accept table: int64, float64, every timestamp unit with and
     without zone, date64, utf8, binary.
   - reverse reject table: int32, float32, date32, large_utf8,
     large_binary, dictionary(utf8), list(int64), null; each yields an
     error whose `ErrorType` is `ErrIncompatibleType`.

2. `refactor(writer): validate arrow schema through ColumnTypeOfArrow`

   `writer_arrow.go`: delete `arrowSupportedTypes` and
   `arrowTypeSupported`; step 3 of `validateArrowSchema` calls
   `ColumnTypeOfArrow` and returns its error with the table and column
   added via a second `wrapError`, or keeps building the existing one and
   discards the inner error. Pick whichever keeps the error message the
   existing `TestArrowWriterRejects*` tests assert on; adjust those
   assertions only if they check the text, not the code.

3. `test(arrow): use ArrowType in reader, writer and query tests`

   Replace `arrowTypeIDOf` in `reader_arrow_test.go` with
   `col.ColumnType.ArrowType()` compared via `arrow.TypeEqual` (a stronger
   check than ID equality, which is the point). Replace the non-timestamp
   arms of `arrowDataType` in `writer_arrow_test.go` with `ArrowType()`,
   keeping the `unit` override for the timestamp column since that test
   deliberately pushes every unit. Add one assertion in
   `query_arrow_test.go` that each result field's type equals
   `ArrowType()` of the column it selects, which pins the forward mapping
   to a live cluster, not only to our own expectations.

4. `chore(timeseries): remove arrow type mapping plan`

   Delete this file before the PR is opened for merge, as on sc-19821 and
   sc-19837.

## Verification before the PR

- `direnv exec . go build ./...`
- `direnv exec . env GOEXPERIMENT=cgocheck2 go test -run 'Arrow|ColumnType' ./...`
- `direnv exec . env GOTOOLCHAIN=go1.26.2 golangci-lint run --timeout=5m ./*.go`, zero new issues on touched files.
- Tell the qdb-api-rest session the method name and signature so it can
  drop `arrowTypes` from its read.go once it bumps the vendored copy.

## PR and build

`gh pr create -B master -t "QDB-19912 - Arrow to QuasarDB column type mapping" -b "" -r igorniebylski`.
Pipeline `qdb-api-go` builds on webhook; reprioritize scheduled jobs to 100. After squash-merge, cherry-pick to `3.14.x`.

## Open items for review

- Name of the reverse function: `ColumnTypeOfArrow` vs `TsColumnTypeOf`
  vs `ArrowColumnType`. The plan takes the first.
- Should a timestamp with a non-empty, non-UTC zone be an error in the
  reverse direction rather than accepted and dropped? The writer accepts
  it today, so making it an error in commit 2 would be a behaviour change
  for `ArrowWriter.SetTable`. The plan keeps current behaviour and
  documents it.
