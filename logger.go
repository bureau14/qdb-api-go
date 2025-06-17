package qdb

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

// QdbLogTimeKey overrides the Record.Time when present as an attribute.
// The contract: if callers pass `QdbLogTimeKey, time.Time` (or the Attr
// equivalent), the slog adapter will use that value as the record’s
// timestamp and omit the attribute itself.
const QdbLogTimeKey = "qdb_time"

// Logger is the project-wide logging façade.
//
// Decision rationale:
//
//	– Decouples QuasarDB Go API from any concrete logging package.
//	– Keeps the surface small; only the levels currently needed by the
//	  codebase are exposed, yet it is open for future extension.
//
// Key assumptions:
//
//	– Structured logging is preferred; key-value pairs follow slog’s
//	  “attr” convention (`msg, "k1", v1, …`).
//	– All methods are safe for concurrent use by multiple goroutines.
type Logger interface {
	Detailed(msg string, args ...any) // NEW – maps to slog.Debug
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Panic(msg string, args ...any) // NEW – maps to slog.Error

	// With returns a logger that permanently appends the supplied attributes.
	With(args ...any) Logger
}

var globalLogger atomic.Value // stores Logger

// splitTimestamp scans the variadic slog-arg slice, extracts the first
// QdbLogTimeKey attribute whose value is a time.Time, and returns that
// timestamp plus the remaining args.
//
// Decision rationale:
//
//	– Enables record-level timestamp overrides while preserving slog’s
//	  zero-alloc attribute API.
//	– Keeps caller code simple: just append QdbLogTimeKey, t when needed.
//
// Key assumptions:
//
//	– Only the first occurrence wins; later duplicates are ignored.
//	– Accepts both slog.Attr and "key", value pairs.
//	– Non-time.Time values for QdbLogTimeKey are ignored.
//
// Performance trade-offs:
//
//	– Single linear pass; rest slice reuses input capacity, so no
//	  additional allocations unless an attr is removed.
//	– Avoids reflection; uses type switches on concrete types.
//
// Inline usage example:
// // ts, attrs := splitTimestamp([]any{qdb.QdbLogTimeKey, t,
// //                                   "pid", pid, "user", u})
// // if !ts.IsZero() { … build custom Record … }
//
// (function signature remains unchanged)
func splitTimestamp(args []any) (ts time.Time, rest []any) {
	// Pre-size rest so, in the common case (no timestamp found),
	// we can append without triggering a re-allocation.
	rest = make([]any, 0, len(args))

	// When we detect the key in "key,value" form we must also ignore
	// the *value* during the next iteration; `skip` flags that intent.
	skip := false

	for i := range len(args) {
		// Honour skip request coming from the previous iteration.
		if skip {
			skip = false
			continue
		}

		// Two syntaxes are possible in slog:
		//   1. slog.Attr{Key: "k", Value: …}
		//   2. alternating "key", value pairs
		// We must support both so callers remain ergonomic.
		switch v := args[i].(type) {
		case slog.Attr:
			// Attr branch – already a fully-formed slog attribute.
			if v.Key == QdbLogTimeKey {
				// Only treat it as an override when the value is *exactly* time.Time.
				if t, ok := v.Value.Any().(time.Time); ok {
					ts = t
					// Do NOT add this attr to rest; we’re replacing the record’s timestamp
					// and purposely omitting the original attribute to avoid duplication.
					continue
				}
			}

		case string:
			// "key","value" branch – we need one-element look-ahead for the value.
			if v == QdbLogTimeKey && i+1 < len(args) {
				if t, ok := args[i+1].(time.Time); ok {
					ts = t
					// Skip the value on the next loop iteration.
					skip = true
					continue
				}
			}
		}

		// Not a timestamp attribute → copy through unchanged.
		rest = append(rest, args[i])
	}

	// ts will be zero when no override was provided; callers test that.
	return
}

type slogAdapter struct{ l *slog.Logger }

func (s *slogAdapter) log(level slog.Level, msg string, args ...any) {
	ts, rest := splitTimestamp(args)

	// Fast path: no custom timestamp requested.
	if ts.IsZero() {
		s.l.Log(context.Background(), level, msg, rest...)
		return
	}

	// Manual Record build with overridden time stamp.
	rec := slog.NewRecord(ts, level, msg, 0)
	if len(rest) > 0 {
		rec.Add(rest...)
	}
	// Pass straight to the underlying Handler.
	_ = s.l.Handler().Handle(context.Background(), rec)
}

func (s *slogAdapter) Detailed(msg string, args ...any) { s.log(slog.LevelDebug, msg, args...) }
func (s *slogAdapter) Debug(msg string, args ...any)    { s.log(slog.LevelDebug, msg, args...) }
func (s *slogAdapter) Info(msg string, args ...any)     { s.log(slog.LevelInfo, msg, args...) }
func (s *slogAdapter) Warn(msg string, args ...any)     { s.log(slog.LevelWarn, msg, args...) }
func (s *slogAdapter) Error(msg string, args ...any)    { s.log(slog.LevelError, msg, args...) }
func (s *slogAdapter) Panic(msg string, args ...any)    { s.log(slog.LevelError, msg, args...) }
func (s *slogAdapter) With(args ...any) Logger          { return &slogAdapter{l: s.l.With(args...)} }

func init() {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo, // default level; tweak in tests if needed
	})
	globalLogger.Store(Logger(&slogAdapter{l: slog.New(handler)}))
}

// L returns the current package-level logger.
func L() Logger { return globalLogger.Load().(Logger) }

// SetLogger replaces the package-level logger.
//
// Performance trade-offs:
//
//	– Atomic swap is O(1) and contention-free for callers.
//	– Panics on nil to fail fast in mis-configuration scenarios.
func SetLogger(l Logger) {
	if l == nil {
		panic("SetLogger: logger must not be nil")
	}
	globalLogger.Store(l)
}
