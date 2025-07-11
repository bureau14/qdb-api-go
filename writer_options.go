package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

// WriterPushFlag: batch push behavior flags
type WriterPushFlag C.qdb_exp_batch_push_flags_t

const (
	WriterPushFlagNone            WriterPushFlag = C.qdb_exp_batch_push_flag_none
	WriterPushFlagWriteThrough    WriterPushFlag = C.qdb_exp_batch_push_flag_write_through
	WriterPushFlagAsyncClientPush WriterPushFlag = C.qdb_exp_batch_push_flag_asynchronous_client_push
)

// WriterPushMode: batch push consistency mode
type WriterPushMode C.qdb_exp_batch_push_mode_t

const (
	WriterPushModeTransactional WriterPushMode = C.qdb_exp_batch_push_transactional
	WriterPushModeFast          WriterPushMode = C.qdb_exp_batch_push_fast
	WriterPushModeAsync         WriterPushMode = C.qdb_exp_batch_push_async
)

// WriterDeduplicationMode: duplicate handling strategy
type WriterDeduplicationMode C.qdb_exp_batch_deduplication_mode_t

const (
	WriterDeduplicationModeDisabled WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_disabled
	WriterDeduplicationModeDrop     WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_drop
	WriterDeduplicationModeUpsert   WriterDeduplicationMode = C.qdb_exp_batch_deduplication_mode_upsert
)

// WriterOptions: batch push configuration.
// Fields:
//
//	pushMode: consistency level
//	dropDuplicates: enable dedup
//	dropDuplicateColumns: dedup keys
//	dedupMode: dedup strategy
//	pushFlags: behavior flags
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
