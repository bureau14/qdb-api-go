package qdb

// #cgo CFLAGS: -I${SRCDIR}/thirdparty/quasardb/include
// #cgo LDFLAGS: -L${SRCDIR}/thirdparty/quasardb/bin -L${SRCDIR}/thirdparty/quasardb/lib -lqdb_api
import "C"
