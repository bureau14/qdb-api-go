package qdb

/*
	#include <qdb/node.h>
	#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"unsafe"
)

// Node : a structure giving access to various
//	informations or actions on a node
type Node struct {
	HandleType
	uri string
}

// Status :
//	Returns the status of a node.
//
//	The status is a JSON object and contains current information of the node state, as described in the documentation.
func (n Node) Status() (NodeStatus, error) {
	data, err := n.RawStatus()
	if err != nil {
		return NodeStatus{}, err
	}
	var output NodeStatus
	err = json.Unmarshal(data, &output)
	return output, err
}

// RawStatus :
//	Returns the status of a node.
//
//	The status is a JSON object as a byte array and contains current information of the node state, as described in the documentation.
func (n Node) RawStatus() ([]byte, error) {
	var contentLength C.qdb_size_t
	var content *C.char
	err := C.qdb_node_status(n.handle, C.CString(n.uri), &content, &contentLength)
	var output []byte
	if err == 0 {
		output = C.GoBytes(unsafe.Pointer(content), C.int(contentLength-1))
		n.Release(unsafe.Pointer(content))
	}
	return output, makeErrorOrNil(err)
}

// Config :
//	Returns the configuration of a node.
//
//	The configuration is a JSON object, as described in the documentation.
func (n Node) Config() (NodeConfig, error) {
	data, err := n.RawConfig()
	if err != nil {
		return NodeConfig{}, err
	}
	var output NodeConfig
	err = json.Unmarshal(data, &output)
	return output, err
}

// RawConfig :
//	Returns the configuration of a node.
//
//	The configuration is a JSON object as a byte array, as described in the documentation.
func (n Node) RawConfig() ([]byte, error) {
	var contentLength C.qdb_size_t
	var content *C.char
	err := C.qdb_node_config(n.handle, C.CString(n.uri), &content, &contentLength)
	var output []byte
	if err == 0 {
		output = C.GoBytes(unsafe.Pointer(content), C.int(contentLength-1))
		n.Release(unsafe.Pointer(content))
	}
	return output, makeErrorOrNil(err)
}

// Topology :
//	Returns the topology of a node.
//
//	The topology is a JSON object containing the node address, and the addresses of its successor and predecessor.
func (n Node) Topology() (NodeTopology, error) {
	data, err := n.RawTopology()
	if err != nil {
		return NodeTopology{}, err
	}
	var output NodeTopology
	err = json.Unmarshal(data, &output)
	return output, err
}

// RawTopology :
//	Returns the topology of a node.
//
//	The topology is a JSON object as a byte array containing the node address, and the addresses of its successor and predecessor.
func (n Node) RawTopology() ([]byte, error) {
	var contentLength C.qdb_size_t
	var content *C.char
	err := C.qdb_node_topology(n.handle, C.CString(n.uri), &content, &contentLength)
	var output []byte
	if err == 0 {
		output = C.GoBytes(unsafe.Pointer(content), C.int(contentLength-1))
		n.Release(unsafe.Pointer(content))
	}
	return output, makeErrorOrNil(err)
}
