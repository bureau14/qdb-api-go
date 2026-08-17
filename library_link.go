package qdb

// Linux links the self-contained lib/libqdb_api.a that the c-api package
// ships next to libqdb_api.so, plus libstdc++/libgcc statically, so binaries
// carry the C API and need no runtime library or rpath. -Bstatic makes
// -lqdb_api pick the archive over the .so in the same directory; the archive
// needs a libstdc++ from GCC 13 or newer. The other platforms link the
// shared library.

// #cgo CFLAGS: -I${SRCDIR}/qdb/include
// #cgo linux CFLAGS: -DQDB_API_STATIC_LINK
// #cgo linux LDFLAGS: -L${SRCDIR}/qdb/lib -Wl,-Bstatic -lqdb_api -lstdc++ -Wl,-Bdynamic -static-libgcc -lm -ldl -lpthread -lrt
// #cgo !linux LDFLAGS: -L${SRCDIR}/qdb/bin -L${SRCDIR}/qdb/lib -lqdb_api
import "C"
