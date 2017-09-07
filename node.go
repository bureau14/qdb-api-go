package qdb

/*
	#include <qdb/node.h>
	#include <stdlib.h>
*/
import "C"
import (
	"time"
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
	err = ConvertConfig(data, &output)
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

// NodeStatus : a json representation object containing the status of a node
type NodeStatus struct {
	Memory struct {
		VM struct {
			Used  int64 `json:"used"`
			Total int64 `json:"total"`
		} `json:"vm"`
		Physmem struct {
			Used  int64 `json:"used"`
			Total int64 `json:"total"`
		} `json:"physmem"`
	} `json:"memory"`
	CPUTimes struct {
		Idle   int64 `json:"idle"`
		System int   `json:"system"`
		User   int64 `json:"user"`
	} `json:"cpu_times"`
	DiskUsage struct {
		Free  int64 `json:"free"`
		Total int64 `json:"total"`
	} `json:"disk_usage"`
	Network struct {
		ListeningEndpoint string `json:"listening_endpoint"`
		Partitions        struct {
			Count             int   `json:"count"`
			MaxSessions       int   `json:"max_sessions"`
			AvailableSessions []int `json:"available_sessions"`
		} `json:"partitions"`
	} `json:"network"`
	NodeID              string    `json:"node_id"`
	OperatingSystem     string    `json:"operating_system"`
	HardwareConcurrency int       `json:"hardware_concurrency"`
	Timestamp           time.Time `json:"timestamp"`
	Startup             time.Time `json:"startup"`
	EngineVersion       string    `json:"engine_version"`
	EngineBuildDate     time.Time `json:"engine_build_date"`
	Entries             struct {
		Resident struct {
			Count int `json:"count"`
			Size  int `json:"size"`
		} `json:"resident"`
		Persisted struct {
			Count int `json:"count"`
			Size  int `json:"size"`
		} `json:"persisted"`
	} `json:"entries"`
	Operations struct {
		Get struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"get"`
		GetAndRemove struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"get_and_remove"`
		Put struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"put"`
		Update struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"update"`
		GetAndUpdate struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"get_and_update"`
		CompareAndSwap struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"compare_and_swap"`
		Remove struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"remove"`
		RemoveIf struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"remove_if"`
		PurgeAll struct {
			Count     int `json:"count"`
			Successes int `json:"successes"`
			Failures  int `json:"failures"`
			Pageins   int `json:"pageins"`
			Evictions int `json:"evictions"`
			InBytes   int `json:"in_bytes"`
			OutBytes  int `json:"out_bytes"`
		} `json:"purge_all"`
	} `json:"operations"`
	Overall struct {
		Count     int `json:"count"`
		Successes int `json:"successes"`
		Failures  int `json:"failures"`
		Pageins   int `json:"pageins"`
		Evictions int `json:"evictions"`
		InBytes   int `json:"in_bytes"`
		OutBytes  int `json:"out_bytes"`
	} `json:"overall"`
}
