package qdb

/*
	#include <qdb/client.h>
*/
import "C"
import "time"

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

// WaitStabilization : Wait for all nodes of the cluster to be stabilized.
//	Takes a timeout value, in milliseconds.
func (c Cluster) WaitStabilization(timeout time.Duration) error {
	err := C.qdb_wait_for_stabilization(c.handle, C.int(timeout/time.Millisecond))
	return makeErrorOrNil(err)
}