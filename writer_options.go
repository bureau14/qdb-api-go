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

// WriterPushFlag controls batch push behavior.
type WriterPushFlag C.qdb_exp_batch_push_flags_t

const (
	WriterPushFlagNone            WriterPushFlag = C.qdb_exp_batch_push_flag_none
	WriterPushFlagWriteThrough    WriterPushFlag = C.qdb_exp_batch_push_flag_write_through
	WriterPushFlagAsyncClientPush WriterPushFlag = C.qdb_exp_batch_push_flag_asynchronous_client_push
)

// WriterPushMode sets batch push consistency level.
type WriterPushMode C.qdb_exp_batch_push_mode_t

const (
	WriterPushModeTransactional WriterPushMode = C.qdb_exp_batch_push_transactional
	WriterPushModeFast          WriterPushMode = C.qdb_exp_batch_push_fast
	WriterPushModeAsync         WriterPushMode = C.qdb_exp_batch_push_async
)

// WriterDeduplicationMode controls duplicate handling.
type WriterDeduplicationMode C.qdb_exp_batch_deduplication_mode_t

const (
	WriterDeduplicationModeDisabled WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_disabled
	WriterDeduplicationModeDrop     WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_drop
	WriterDeduplicationModeUpsert   WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_upsert
)

// WriterOptions configures batch push behavior.
type WriterOptions struct {
	pushMode             WriterPushMode          // consistency level
	dropDuplicates       bool                    // enable deduplication
	dropDuplicateColumns []string                // columns for dedup
	dedupMode            WriterDeduplicationMode // dedup strategy
	pushFlags            WriterPushFlag          // behavior flags
}

// NewWriterOptions creates writer options with safe defaults.
func NewWriterOptions() WriterOptions {
	return WriterOptions{
		pushMode:             WriterPushModeTransactional,
		dropDuplicates:       false,
		dropDuplicateColumns: []string{},
		dedupMode:            WriterDeduplicationModeDisabled,
		pushFlags:            WriterPushFlagWriteThrough,
	}
}

// GetPushMode returns the configured push mode.
func (options WriterOptions) GetPushMode() WriterPushMode {
	return options.pushMode
}

// GetDeduplicationMode returns the deduplication strategy.
func (options WriterOptions) GetDeduplicationMode() WriterDeduplicationMode {
	return options.dedupMode
}

// IsDropDuplicatesEnabled reports if deduplication is enabled.
func (options WriterOptions) IsDropDuplicatesEnabled() bool {
	return options.dropDuplicates
}

// EnableWriteThrough bypasses server-side cache on push.
func (options WriterOptions) EnableWriteThrough() WriterOptions {
	options.pushFlags |= WriterPushFlagWriteThrough

	return options
}

// DisableWriteThrough allows server to cache pushed data.
func (options WriterOptions) DisableWriteThrough() WriterOptions {
	options.pushFlags &^= WriterPushFlagWriteThrough

	return options
}

// IsWriteThroughEnabled reports if write-through is enabled.
func (options WriterOptions) IsWriteThroughEnabled() bool {
	return options.pushFlags&WriterPushFlagWriteThrough != 0
}

// EnableAsyncClientPush returns before server persistence.
func (options WriterOptions) EnableAsyncClientPush() WriterOptions {
	options.pushFlags |= WriterPushFlagAsyncClientPush

	return options
}

// DisableAsyncClientPush waits for server acknowledgement.
func (options WriterOptions) DisableAsyncClientPush() WriterOptions {
	options.pushFlags &^= WriterPushFlagAsyncClientPush

	return options
}

// IsAsyncClientPushEnabled reports if async push is enabled.
func (options WriterOptions) IsAsyncClientPushEnabled() bool {
	return options.pushFlags&WriterPushFlagAsyncClientPush != 0
}

// EnableDropDuplicates activates deduplication on all columns.
func (options WriterOptions) EnableDropDuplicates() WriterOptions {
	options.dropDuplicates = true
	if options.dedupMode == WriterDeduplicationModeDisabled {
		options.dedupMode = WriterDeduplicationModeDrop // set least-expensive deduplication strategy
	}

	return options
}

// EnableDropDuplicatesOn activates deduplication on specific columns.
func (options WriterOptions) EnableDropDuplicatesOn(columns []string) WriterOptions {
	options.dropDuplicates = true
	options.dropDuplicateColumns = columns
	if options.dedupMode == WriterDeduplicationModeDisabled {
		options.dedupMode = WriterDeduplicationModeDrop // use least-expensive strategy by default
	}

	return options
}

// GetDropDuplicateColumns returns columns used for deduplication.
func (options WriterOptions) GetDropDuplicateColumns() []string {
	return options.dropDuplicateColumns
}

// IsValid checks if the options are correctly configured.
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

// WithPushMode sets the push mode.
func (options WriterOptions) WithPushMode(mode WriterPushMode) WriterOptions {
	options.pushMode = mode

	return options
}

// WithDeduplicationMode sets the deduplication mode.
func (options WriterOptions) WithDeduplicationMode(mode WriterDeduplicationMode) WriterOptions {
	options.dedupMode = mode
	options.dropDuplicates = mode != WriterDeduplicationModeDisabled

	return options
}

// WithAsyncPush enables async push mode.
func (options WriterOptions) WithAsyncPush() WriterOptions {
	return options.WithPushMode(WriterPushModeAsync)
}

// WithFastPush enables fast push mode.
func (options WriterOptions) WithFastPush() WriterOptions {
	return options.WithPushMode(WriterPushModeFast)
}

// WithTransactionalPush enables transactional push mode.
func (options WriterOptions) WithTransactionalPush() WriterOptions {
	return options.WithPushMode(WriterPushModeTransactional)
}

// setNative converts options to C struct.
func (options WriterOptions) setNative(opts C.qdb_exp_batch_options_t) C.qdb_exp_batch_options_t {
	opts.mode = C.qdb_exp_batch_push_mode_t(options.pushMode)

	return opts
}
