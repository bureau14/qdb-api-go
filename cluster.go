package qdb

/*
	#include <qdb/client.h>
	#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"
)

// Cluster : An object permitting calls to a cluster
type Cluster struct {
	HandleType
}

// PurgeAll : Removes irremediably all data from all the nodes of the cluster.
// 	This function is useful when quasardb is used as a cache and is not the golden source.
// 	This call is not atomic: if the command cannot be dispatched on the whole cluster, it will be dispatched on as many nodes as possible and the function will return with a qdb_e_ok code.
// 	By default cluster does not allow this operation and the function returns a qdb_e_operation_disabled error.
func (c Cluster) PurgeAll() error {
	err := C.qdb_purge_all(c.handle, 60*1000)
	return makeErrorOrNil(err)
}

// PurgeCache : Removes all cached data from all the nodes of the cluster.
//	This function is disabled on a transient cluster.
//	Prefer purge_all in this case.
//
// 	This call is not atomic: if the command cannot be dispatched on the whole cluster, it will be dispatched on as many nodes as possible and the function will return with a qdb_e_ok code.
func (c Cluster) PurgeCache() error {
	err := C.qdb_purge_cache(c.handle, 60*1000)
	return makeErrorOrNil(err)
}

// TrimAll : Trims all data on all the nodes of the cluster.
//	Quasardb uses Multi-Version Concurrency Control (MVCC) as a foundation of its transaction engine. It will automatically clean up old versions as entries are accessed.
//	This call is not atomic: if the command cannot be dispatched on the whole cluster, it will be dispatched on as many nodes as possible and the function will return with a qdb_e_ok code.
//	Entries that are not accessed may not be cleaned up, resulting in increasing disk usage.
//
//	This function will request each nodes to trim all entries, release unused memory and compact files on disk.
//	Because this operation is I/O and CPU intensive it is not recommended to run it when the cluster is heavily used.
func (c Cluster) TrimAll() error {
	err := C.qdb_trim_all(c.handle, 60*60*1000)
	return makeErrorOrNil(err)
}

// WaitForStabilization : Wait for all nodes of the cluster to be stabilized.
//	Takes a timeout value, in milliseconds.
func (c Cluster) WaitForStabilization(timeout time.Duration) error {
	err := C.qdb_wait_for_stabilization(c.handle, C.int(timeout/time.Millisecond))
	return makeErrorOrNil(err)
}

// Endpoint : A structure representing a qdb url endpoint
type Endpoint struct {
	Address string
	Port    int64
}

// URI : Returns a formatted URI of the endpoint
func (t Endpoint) URI() string {
	return fmt.Sprintf("%s:%d", t.Address, t.Port)
}

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (t Endpoint) toStructC() C.qdb_remote_node_t {
	address := convertToCharStar(string(t.Address))
	port := C.ushort(t.Port)
	// The [6]byte is some sort of padding necessary for Go : struct(char *, short, 6 byte of padding)
	return C.qdb_remote_node_t{address, port, [6]byte{}}
}

func (t C.qdb_remote_node_t) toStructG() Endpoint {
	return Endpoint{C.GoString(t.address), int64(t.port)}
}

func endpointArrayToC(edpts ...Endpoint) *C.qdb_remote_node_t {
	if len(edpts) == 0 {
		return nil
	}
	endpoints := make([]C.qdb_remote_node_t, len(edpts))
	for idx, pt := range edpts {
		endpoints[idx] = pt.toStructC()
	}
	return &endpoints[0]
}

func releaseEndpointArray(endpoints *C.qdb_remote_node_t, length int) {
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_remote_node_t)(unsafe.Pointer(endpoints))[:length:length]
		for _, s := range tmpslice {
			C.free(unsafe.Pointer(s.address))
		}
	}
}

func endpointArrayToGo(endpoints *C.qdb_remote_node_t, endpointsCount C.qdb_size_t) []Endpoint {
	length := int(endpointsCount)
	output := make([]Endpoint, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_remote_node_t)(unsafe.Pointer(endpoints))[:length:length]
		for i, s := range tmpslice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// Endpoints : Retrieve all endpoints accessible to this handle.
func (c Cluster) Endpoints() ([]Endpoint, error) {
	var endpoints *C.qdb_remote_node_t
	var endpointsCount C.qdb_size_t
	err := C.qdb_cluster_endpoints(c.handle, &endpoints, &endpointsCount)
	if err == 0 {
		defer c.Release(unsafe.Pointer(endpoints))
		return endpointArrayToGo(endpoints, endpointsCount), nil
	}
	return nil, makeErrorOrNil(err)
}
