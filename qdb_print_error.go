package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
*/
import "C"
import "fmt"

// PrintError : translates an error into an english error message
func PrintError(e ErrorType) {
	errorMessage := C.qdb_error(C.qdb_error_t(e))
	errorString := C.GoString(errorMessage)
	fmt.Printf("%s\n", errorString)
}
