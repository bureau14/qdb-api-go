package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/client.h>
*/
import "C"
import "unsafe"

// HandleType obfuscating qdb_handle_t
type HandleType struct {
	handle C.qdb_handle_t
	uri    string
}

// Protocol : A network protocol.
type Protocol C.qdb_protocol_t

const (
	// ProtocolTCP : Uses TCP/IP to communicate with the cluster. This is currently the only supported network protocol.
	ProtocolTCP = C.qdb_p_tcp
)

// Compression : compression parameter
type Compression C.qdb_compression_t

const (
	// CompNone : No compression.
	CompNone = C.qdb_comp_none
	// CompFast : Maximum compression speed, potentially minimum compression ratio. This is currently the default.
	CompFast = C.qdb_comp_fast
	// CompBest : Maximum compression ratio, potentially minimum compression speed. This is currently not implemented.
	CompBest = C.qdb_comp_best
)

// APIVersion : Returns a string describing the API version.
func (h HandleType) APIVersion() string {
	version := C.qdb_version()
	defer h.Release(unsafe.Pointer(version))
	return C.GoString(version)
}

// APIBuild : Returns a string describing the exact API build.
func (h HandleType) APIBuild() string {
	build := C.qdb_build()
	defer h.Release(unsafe.Pointer(build))
	return C.GoString(build)
}

// Open : Creates a handle. No connection will be established.
// Not needed if you created your handle with NewHandle.
func (h HandleType) Open(protocol Protocol) error {
	err := C.qdb_open(&h.handle, C.qdb_protocol_t(protocol))
	return makeErrorOrNil(err)
}

// SetTimeout : Sets the timeout of all network operations.
// The lower the timeout, the higher the risk of having timeout errors.
func (h HandleType) SetTimeout(timeout int) error {
	err := C.qdb_option_set_timeout(h.handle, C.int(timeout))
	return makeErrorOrNil(err)
}

// SetMaxCardinality : Sets the maximum allowed cardinality of a quasardb query. The default value is 10,007. The minimum allowed values is 100.
func (h HandleType) SetMaxCardinality(maxCardinality uint) error {
	err := C.qdb_option_set_max_cardinality(h.handle, C.qdb_uint_t(maxCardinality))
	return makeErrorOrNil(err)
}

// SetCompression : Set the compression level for all future messages emitted by the specified handle.
// Regardless of this parameter, the API will be able to read whatever compression the server uses.
func (h HandleType) SetCompression(compressionLevel Compression) error {
	err := C.qdb_option_set_compression(h.handle, C.qdb_compression_t(compressionLevel))
	return makeErrorOrNil(err)
}

// Connect : connect a previously opened handle
func (h HandleType) Connect(clusterURI string) error {
	err := C.qdb_connect(h.handle, C.CString(clusterURI))
	return makeErrorOrNil(err)
}

// Close : open a tcp handle
func (h HandleType) Close() error {
	err := C.qdb_close(h.handle)
	return makeErrorOrNil(err)
}

// Release : release previously allocated qdb resource
func (h HandleType) Release(buffer unsafe.Pointer) {
	C.qdb_release(h.handle, buffer)
}

// NewHandle : Create a new handle, return error if needed
func NewHandle() (HandleType, error) {
	var h HandleType
	err := C.qdb_open((*C.qdb_handle_t)(&h.handle), ProtocolTCP)
	return h, makeErrorOrNil(err)
}

// Entries creators

// Blob : create a blob entry object
func (h HandleType) Blob(alias string) BlobEntry {
	return BlobEntry{entry{h, alias}}
}

// Integer : create an integer entry object
func (h HandleType) Integer(alias string) IntegerEntry {
	return IntegerEntry{entry{h, alias}}
}

// Timeseries : create a timeseries entry object
func (h HandleType) Timeseries(alias string, columns []TsColumnInfo) TimeseriesEntry {
	return TimeseriesEntry{entry{h, alias}, columns}
}
