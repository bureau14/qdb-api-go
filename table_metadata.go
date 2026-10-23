// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

/*
	#include <qdb/option.h>
	#include <qdb/ts.h>

	#cgo noescape qdb_ts_get_metadata
	#cgo nocallback qdb_ts_get_metadata
	#cgo noescape qdb_ts_get_table_metadata_cached
	#cgo nocallback qdb_ts_get_table_metadata_cached
	#cgo noescape qdb_ts_set_ttl
	#cgo nocallback qdb_ts_set_ttl
	#cgo noescape qdb_option_set_table_cache_ttl
	#cgo nocallback qdb_option_set_table_cache_ttl
	#cgo noescape qdb_option_get_table_cache_ttl
	#cgo nocallback qdb_option_get_table_cache_ttl
*/
import "C"

import (
	"math"
	"time"
)

// TableMetadata is a snapshot of a table's schema and settings, copied out
// of one qdb_ts_metadata_t block. It holds no C memory: it may be shared
// between goroutines, held across calls, and compared with reflect.DeepEqual.
type TableMetadata struct {
	// Name is the table alias.
	Name string
	// Columns in schema order. The implicit $timestamp column is not
	// listed. Symbol columns carry the name of their symbol table.
	Columns []TsColumnInfo
	// ShardSize is the duration covered by one shard.
	ShardSize time.Duration
	// TTL is the time to live of the data. Zero means disabled.
	TTL time.Duration
}

// Column returns the column named name and whether it exists.
func (m TableMetadata) Column(name string) (TsColumnInfo, bool) {
	for _, c := range m.Columns {
		if c.name == name {
			return c, true
		}
	}

	return TsColumnInfo{}, false
}

// durationFromMilliseconds converts a qdb_duration_t, saturating at the
// largest time.Duration so sentinels such as qdb_d_infinite_shard_size do
// not wrap.
func durationFromMilliseconds(ms C.qdb_duration_t) time.Duration {
	if uint64(ms) >= uint64(math.MaxInt64/int64(time.Millisecond)) {
		return time.Duration(math.MaxInt64)
	}

	return time.Duration(ms) * time.Millisecond
}

// durationToMilliseconds converts to a qdb_duration_t. A negative duration
// is rejected before the unsigned conversion could wrap it.
func durationToMilliseconds(d time.Duration, op, key string) (C.qdb_duration_t, error) {
	if d < 0 {
		return 0, wrapError(C.qdb_e_invalid_argument, op, key, d, "reason", "negative")
	}

	return C.qdb_duration_t(d / time.Millisecond), nil
}

// tableMetadataFromC deep-copies the C block into Go memory. The caller
// still owns and must release the block.
func tableMetadataFromC(alias string, m *C.qdb_ts_metadata_t) TableMetadata {
	return TableMetadata{
		Name:      alias,
		Columns:   columnInfoArrayToGo(m.columns, m.column_count),
		ShardSize: durationFromMilliseconds(m.shard_size),
		TTL:       durationFromMilliseconds(m.ttl),
	}
}

// tsMetadataFetch is the signature shared by the fetching and the cached
// C metadata calls.
type tsMetadataFetch func(C.qdb_handle_t, *C.char, **C.qdb_ts_metadata_t) C.qdb_error_t

// fetchTableMetadata runs one metadata call and copies the result out
// before releasing the C block.
func (entry TimeseriesEntry) fetchTableMetadata(fetch tsMetadataFetch, op string) (TableMetadata, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)

	var metadata *C.qdb_ts_metadata_t
	err := fetch(entry.handle, alias, &metadata)
	if err != C.qdb_e_ok {
		return TableMetadata{}, wrapError(err, op, "alias", entry.alias)
	}
	defer qdbRelease(entry.HandleType, metadata)

	return tableMetadataFromC(entry.alias, metadata), nil
}

// Metadata fetches the table's columns, shard size and TTL from the server
// in one call.
//
// An unknown table yields an error for which errors.Is(err, ErrAliasNotFound)
// holds.
func (entry TimeseriesEntry) Metadata() (TableMetadata, error) {
	return entry.fetchTableMetadata(func(h C.qdb_handle_t, a *C.char, out **C.qdb_ts_metadata_t) C.qdb_error_t {
		return C.qdb_ts_get_metadata(h, a, out)
	}, "ts_get_metadata")
}

// CachedMetadata is Metadata served from the handle's table cache when the
// entry is present and younger than the cache TTL (see
// HandleOptions.WithTableCacheTTL), and fetched from the server otherwise.
// The C API drops a cached entry itself when the handle alters the table.
func (entry TimeseriesEntry) CachedMetadata() (TableMetadata, error) {
	return entry.fetchTableMetadata(func(h C.qdb_handle_t, a *C.char, out **C.qdb_ts_metadata_t) C.qdb_error_t {
		return C.qdb_ts_get_table_metadata_cached(h, a, out)
	}, "ts_get_table_metadata_cached")
}

// SetTTL sets the table's time to live. Zero disables it. The C API keeps
// whole seconds, so a sub-second remainder is dropped.
func (entry TimeseriesEntry) SetTTL(ttl time.Duration) error {
	ms, convErr := durationToMilliseconds(ttl, "ts_set_ttl", "ttl")
	if convErr != nil {
		return convErr
	}
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	err := C.qdb_ts_set_ttl(entry.handle, alias, ms)

	return wrapError(err, "ts_set_ttl", "alias", entry.alias, "ttl", ttl)
}

// SetTableCacheTTL sets how long the handle's table metadata cache keeps an
// entry. Zero disables the cache. The C API default is 60 seconds and it
// keeps whole seconds, so a sub-second remainder is dropped.
func (h HandleType) SetTableCacheTTL(ttl time.Duration) error {
	if ttl < 0 || ttl/time.Millisecond > math.MaxInt32 {
		return wrapError(C.qdb_e_invalid_argument, "handle_set_table_cache_ttl", "ttl", ttl, "reason", "outside C int milliseconds")
	}
	err := C.qdb_option_set_table_cache_ttl(h.handle, C.int(ttl/time.Millisecond))

	return wrapError(err, "handle_set_table_cache_ttl", "ttl_ms", ttl/time.Millisecond)
}

// GetTableCacheTTL returns the handle's table metadata cache TTL.
func (h HandleType) GetTableCacheTTL() (time.Duration, error) {
	var ms C.int
	err := C.qdb_option_get_table_cache_ttl(h.handle, &ms)

	return time.Duration(ms) * time.Millisecond, wrapError(err, "handle_get_table_cache_ttl")
}
