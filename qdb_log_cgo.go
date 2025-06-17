package qdb

/*
	#include <qdb/log.h>

	void go_callback_log(qdb_log_level_t log_level, unsigned long * date, unsigned long pid, unsigned long tid, char * message_buffer, size_t message_size);
*/
import "C"
import (
	"log/slog"
	"math"
	"os"
	"time"
	"unsafe"
)

var gCallbackID C.qdb_log_callback_id

func convertDate(d *C.ulong, length int) []C.ulong {
	date := make([]C.ulong, 6)
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	slice := (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.ulong(0))]C.ulong)(unsafe.Pointer(d))[:length:length]
	for i, s := range slice {
		date[i] = s
	}
	return date
}

//export go_callback_log
func go_callback_log(lvl C.qdb_log_level_t, d *C.ulong, pid, tid C.ulong, buf *C.char, sz C.size_t) {
	date := convertDate(d, 6)
	tstamp := time.Date(int(date[0]), time.Month(date[1]), int(date[2]),
		int(date[3]), int(date[4]), int(date[5]), 0, time.UTC)

	msg := C.GoStringN(buf, C.int(sz))
	attrs := []any{QdbLogTimeKey, tstamp, "pid", int(pid), "tid", int(tid)}

	switch lvl {
	case C.qdb_log_detailed:
		L().Detailed(msg, attrs...)
	case C.qdb_log_debug:
		L().Debug(msg, attrs...)
	case C.qdb_log_info:
		L().Info(msg, attrs...)
	case C.qdb_log_warning:
		L().Warn(msg, attrs...)
	case C.qdb_log_error:
		L().Error(msg, attrs...)
	case C.qdb_log_panic:
		L().Panic(msg, attrs...)
	default:
		L().Info(msg, attrs...)
	}
}

func swapCallback() {
	if err := C.qdb_log_remove_callback(gCallbackID); err != 0 && err != C.qdb_e_invalid_argument {
		L().Warn("unable to remove previous log callback",
			"code", err, "msg", C.GoString(C.qdb_error(err)))
	}

	C.qdb_log_option_set_sync(1)

	if err := C.qdb_log_add_callback(C.qdb_log_callback(C.go_callback_log), &gCallbackID); err != 0 {
		L().Warn("unable to add new log callback",
			"code", err, "msg", C.GoString(C.qdb_error(err)))
	}
}

func SetLogFile(path string) {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	if path != "" {
		if f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644); err == nil {
			h = slog.NewTextHandler(f, &slog.HandlerOptions{Level: slog.LevelInfo})
		} else {
			L().Warn("cannot open log file, falling back to stderr", "path", path, "err", err)
		}
	}
	SetLogger(&slogAdapter{l: slog.New(h)})
}
