// Package qdb provides an api to a quasardb server
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/node.h>
*/
import "C"
import "unsafe"

// HandleType : An opaque handle to internal API-allocated structures needed for maintaining connection to a cluster.
type HandleType struct {
	handle C.qdb_handle_t
}

// Protocol : A network protocol.
type Protocol C.qdb_protocol_t

// Protocol values:
//	ProtocolTCP : Uses TCP/IP to communicate with the cluster. This is currently the only supported network protocol.
const (
	ProtocolTCP Protocol = C.qdb_p_tcp
)

// Compression : compression parameter
type Compression C.qdb_compression_t

// Compression values:
//	CompNone : No compression.
//	CompFast : Maximum compression speed, potentially minimum compression ratio. This is currently the default.
//	CompBest : Maximum compression ratio, potentially minimum compression speed. This is currently not implemented.
const (
	CompNone Compression = C.qdb_comp_none
	CompFast Compression = C.qdb_comp_fast
	CompBest Compression = C.qdb_comp_best
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

// Open : Creates a handle.
//	No connection will be established.
//	Not needed if you created your handle with NewHandle.
func (h HandleType) Open(protocol Protocol) error {
	err := C.qdb_open(&h.handle, C.qdb_protocol_t(protocol))
	return makeErrorOrNil(err)
}

// SetTimeout : Sets the timeout of all network operations.
//	The lower the timeout, the higher the risk of having timeout errors.
func (h HandleType) SetTimeout(timeout int) error {
	err := C.qdb_option_set_timeout(h.handle, C.int(timeout))
	return makeErrorOrNil(err)
}

// SetMaxCardinality : Sets the maximum allowed cardinality of a quasardb query.
//	The default value is 10,007. The minimum allowed values is 100.
func (h HandleType) SetMaxCardinality(maxCardinality uint) error {
	err := C.qdb_option_set_max_cardinality(h.handle, C.qdb_uint_t(maxCardinality))
	return makeErrorOrNil(err)
}

// SetCompression : Set the compression level for all future messages emitted by the specified handle.
//	Regardless of this parameter, the API will be able to read whatever compression the server uses.
func (h HandleType) SetCompression(compressionLevel Compression) error {
	err := C.qdb_option_set_compression(h.handle, C.qdb_compression_t(compressionLevel))
	return makeErrorOrNil(err)
}

// Connect : connect a previously opened handle
//	Binds the client instance to a quasardb cluster and connect to at least one node within.
//	Quasardb URI are in the form qdb://<address>:<port> where <address> is either an IPv4 or IPv6 (surrounded with square brackets), or a domain name. It is recommended to specify multiple addresses should the designated node be unavailable.
//
//	URI examples:
//		qdb://myserver.org:2836 - Connects to myserver.org on the port 2836
//		qdb://127.0.0.1:2836 - Connects to the local IPv4 loopback on the port 2836
//		qdb://myserver1.org:2836,myserver2.org:2836 - Connects to myserver1.org or myserver2.org on the port 2836
//		qdb://[::1]:2836 - Connects to the local IPv6 loopback on the port 2836
func (h HandleType) Connect(clusterURI string) error {
	err := C.qdb_connect(h.handle, C.CString(clusterURI))
	return makeErrorOrNil(err)
}

// Close : Closes the handle previously opened.
//	This results in terminating all connections and releasing all internal buffers,
//	including buffers which may have been allocated as or a result of batch operations or get operations.
func (h HandleType) Close() error {
	err := C.qdb_close(h.handle)
	return makeErrorOrNil(err)
}

// Release : Releases an API-allocated buffer.
//	Failure to properly call this function may result in excessive memory usage.
//	Most operations that return a content (e.g. batch operations, qdb_blob_get, qdb_blob_get_and_update, qdb_blob_compare_and_swap...)
//	will allocate a buffer for the content and will not release the allocated buffer until you either call this function or close the handle.
//
//	The function will be able to release any kind of buffer allocated by a quasardb API call, whether it’s a single buffer, an array or an array of buffers.
func (h HandleType) Release(buffer unsafe.Pointer) {
	C.qdb_release(h.handle, buffer)
}

// NewHandle : Create a new handle, return error if needed
//	The handle is already opened (not connected) with tcp protocol
func NewHandle() (HandleType, error) {
	var h HandleType
	err := C.qdb_open((*C.qdb_handle_t)(&h.handle), C.qdb_protocol_t(ProtocolTCP))
	return h, makeErrorOrNil(err)
}

// Entries creators

// Blob : Create a blob entry object
func (h HandleType) Blob(alias string) BlobEntry {
	return BlobEntry{Entry{h, alias}}
}

// Integer : Create an integer entry object
func (h HandleType) Integer(alias string) IntegerEntry {
	return IntegerEntry{Entry{h, alias}}
}

// Timeseries : Create a timeseries entry object
func (h HandleType) Timeseries(alias string, columns []TsColumnInfo) TimeseriesEntry {
	return TimeseriesEntry{Entry{h, alias}, columns}
}
