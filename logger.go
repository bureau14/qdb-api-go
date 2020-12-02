package qdb

/*
	#include <qdb/log.h>

	void go_callback_log(qdb_log_level_t log_level, unsigned long * date, unsigned long pid, unsigned long tid, char * message_buffer, size_t message_size);
*/
import "C"
import (
	"fmt"
	"math"
	"unsafe"
)

var (
	gCallbackID C.qdb_log_callback_id
)

func getLogLevel(log_level C.qdb_log_level_t) string {
	switch log_level {
	case C.qdb_log_detailed:
		return "detail"
	case C.qdb_log_debug:
		return "debug"
	case C.qdb_log_info:
		return "info"
	case C.qdb_log_warning:
		return "warning"
	case C.qdb_log_error:
		return "error"
	case C.qdb_log_panic:
		return "panic"
	}
	return ""
}

func convertDate(d *C.ulong, length int) []C.ulong {
	date := make([]C.ulong, 6)
	var temp C.ulong
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	slice := (*[math.MaxInt32 - 1/unsafe.Sizeof(temp)]C.ulong)(unsafe.Pointer(d))[:length:length]
	for i, s := range slice {
		date[i] = s
	}
	return date
}

//export go_callback_log
func go_callback_log(log_level C.qdb_log_level_t, d *C.ulong, pid C.ulong, tid C.ulong, message_buffer *C.char, message_size C.size_t) {
	date := convertDate(d, 6)
	msg := C.GoStringN(message_buffer, C.int(message_size))
	fmt.Printf("%d-%02d-%02dT%d:%d:%d [%s]\t%d\t%d\t%s\n", date[0], date[1], date[2], date[3], date[4], date[5], getLogLevel(log_level), pid, tid, msg)
}

func swapCallback() {

	err := C.qdb_log_remove_callback(gCallbackID)
	if err != 0 {
	}

	// C.log_add_callback(C.qdb_log_callback(C.go_callback_log))
	err = C.qdb_log_add_callback(C.qdb_log_callback(C.go_callback_log), &gCallbackID)
	if err != 0 {
		fmt.Printf("unable to add new callback: %s (%#x)\n", C.qdb_error(err), err)
	}
}
