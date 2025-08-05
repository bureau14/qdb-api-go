package qdb

/*
	#include <qdb/client.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"reflect"
)

// Statistics : json adptable structure with node information
type Statistics struct {
	EngineBuildDate     string `json:"engine_build_date"`
	EngineVersion       string `json:"engine_version"`
	HardwareConcurrency int64  `json:"hardware_concurrency_count"`
	Memory              struct {
		BytesResident int64 `json:"resident_bytes"`
		ResidentCount int64 `json:"resident_count"`
		Physmem       struct {
			Used  int64 `json:"used_bytes"`
			Total int64 `json:"total_bytes"`
		} `json:"physmem"`
		VM struct {
			Used  int64 `json:"used_bytes"`
			Total int64 `json:"total_bytes"`
		} `json:"vm"`
	} `json:"memory"`
	Network struct {
		CurrentUsersCount int64 `json:"current_users_count"`
		PartitionsCount   int64 `json:"partitions_count"`
		Sessions          struct {
			AvailableCount   int64 `json:"available_count"`
			UnavailableCount int64 `json:"unavailable_count"`
			MaxCount         int64 `json:"max_count"`
		} `json:"sessions"`
	} `json:"network"`
	NodeID          string `json:"chord.node_id"`
	OperatingSystem string `json:"operating_system"`
	Persistence     struct {
		BytesRead    int64 `json:"read_bytes"`
		BytesWritten int64 `json:"written_bytes"`
	} `json:"persistence"`
	Requests struct {
		BytesOut       int64 `json:"out_bytes"`
		SuccessesCount int64 `json:"successes_count"`
		TotalCount     int64 `json:"total_count"`
	} `json:"requests"`
	Startup int64 `json:"startup_epoch"`
}

func (h DirectHandleType) getStatistics(prefix string, s interface{}) error {
	sType := reflect.ValueOf(s).Type()
	v := reflect.ValueOf(s)
	if sType.Kind() == reflect.Ptr {
		sType = reflect.ValueOf(s).Elem().Type()
		v = reflect.ValueOf(s).Elem()
	}

	for i := range sType.NumField() {
		vType := sType.Field(i).Type
		vTag := sType.Field(i).Tag

		name := vTag.Get("json")
		switch vType.Kind() {
		case reflect.Struct:
			inner := v.Field(i).Addr().Interface()
			newPrefix := prefix + name + "."
			err := h.getStatistics(newPrefix, inner)
			if err != nil {
				return err
			}
		case reflect.String:
			content, err := h.Blob(prefix + name).Get()
			if err != nil {
				return err
			}
			content = bytes.ReplaceAll(content, []byte("\x00"), []byte{})
			v.Field(i).SetString(string(content))
		case reflect.Int64:
			value, err := h.Integer(prefix + name).Get()
			if err != nil {
				return err
			}
			v.Field(i).SetInt(value)
		case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
			reflect.Array, reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
			reflect.Pointer, reflect.Slice, reflect.UnsafePointer:

			return fmt.Errorf("could not retrieve all values")
		}
	}

	return nil
}

// NodeStatistics : Retrieve statistics for a specific node
//
// Deprecated: Statistics will be fetched directly from the node using the new
// direct API
func (h HandleType) NodeStatistics(nodeID string) (Statistics, error) {
	return Statistics{}, nil
}

func (h DirectHandleType) nodeStatistics() (Statistics, error) {
	stat := Statistics{}
	prefix := "$qdb.statistics."
	err := h.getStatistics(prefix, &stat)

	return stat, err
}

// Statistics : Retrieve statistics for all nodes
func (h HandleType) Statistics() (map[string]Statistics, error) {
	results := map[string]Statistics{}

	endpoints, err := h.Cluster().Endpoints()
	if err != nil {
		return results, err
	}

	for _, endpoint := range endpoints {
		uri := endpoint.URI()
		dh, err := h.DirectConnect(uri)
		if err != nil {
			return results, err
		}

		stats, err := dh.nodeStatistics()
		if err != nil {
			return results, err
		}

		results[stats.NodeID] = stats
	}

	return results, err
}
