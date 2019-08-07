package qdb

/*
	#include <qdb/client.h>
*/
import "C"
import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
)

// Statistics : json adptable structure with node information
type Statistics struct {
	CPU struct {
		Idle   int64 `json:"idle"`
		System int64 `json:"system"`
		User   int64 `json:"user"`
	} `json:"cpu"`
	Disk struct {
		BytesFree  int64  `json:"bytes_free"`
		BytesTotal int64  `json:"bytes_total"`
		Path       string `json:"path"`
	} `json:"disk"`
	EngineBuildDate     string `json:"engine_build_date"`
	EngineVersion       string `json:"engine_version"`
	HardwareConcurrency int64  `json:"hardware_concurrency"`
	Memory              struct {
		BytesResident int64 `json:"bytes_resident_size"`
		ResidentCount int64 `json:"resident_count"`
		Physmem       struct {
			Used  int64 `json:"bytes_used"`
			Total int64 `json:"bytes_total"`
		} `json:"physmem"`
		VM struct {
			Used  int64 `json:"bytes_used"`
			Total int64 `json:"bytes_total"`
		} `json:"vm"`
	} `json:"memory"`
	Network struct {
		CurrentUsersCount int64 `json:"current_users_count"`
		Sessions          struct {
			AvailableCount   int64 `json:"available_count"`
			UnavailableCount int64 `json:"unavailable_count"`
			MaxCount         int64 `json:"max_count"`
		} `json:"sessions"`
	} `json:"network"`
	PartitionsCount int64  `json:"partitions_count"`
	NodeID          string `json:"node_id"`
	OperatingSystem string `json:"operating_system"`
	Persistence     struct {
		BytesCapacity int64 `json:"bytes_capacity"`
		BytesRead     int64 `json:"bytes_read"`
		BytesUtilized int64 `json:"bytes_utilized"`
		BytesWritten  int64 `json:"bytes_written"`
		EntriesCount  int64 `json:"entries_count"`
	} `json:"persistence"`
	Requests struct {
		BytesOut       int64 `json:"bytes_out"`
		SuccessesCount int64 `json:"successes_count"`
		TotalCount     int64 `json:"total_count"`
	} `json:"requests"`
	Startup int64 `json:"startup"`
}

func (h DirectHandleType) getStatistics(prefix string, s interface{}) error {
	sType := reflect.ValueOf(s).Type()
	v := reflect.ValueOf(s)
	if sType.Kind() == reflect.Ptr {
		sType = reflect.ValueOf(s).Elem().Type()
		v = reflect.ValueOf(s).Elem()
	}

	for i := 0; i < sType.NumField(); i++ {
		vType := sType.Field(i).Type
		vTag := sType.Field(i).Tag

		name := vTag.Get("json")
		if vType.Kind() == reflect.Struct {
			inner := v.Field(i).Addr().Interface()
			newPrefix := prefix + name + "."
			h.getStatistics(newPrefix, inner)
		} else if vType.Kind() == reflect.String {
			content, err := h.Blob(prefix + name).Get()
			if err != nil {
				return err
			}
			content = bytes.Replace(content, []byte("\x00"), []byte{}, -1)
			v.Field(i).SetString(string(content))
		} else if vType.Kind() == reflect.Int64 {
			value, err := h.Integer(prefix + name).Get()
			if err != nil {
				return err
			}
			v.Field(i).SetInt(value)
		} else {
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

func (h DirectHandleType) id() (string, error) {
	var nodeID string

	r := regexp.MustCompile(`\$qdb.statistics.([^\.]+)\..*`)
	entries, err := h.PrefixGet("$qdb.statistics.", 1000)

	if err != nil {
		return nodeID, err
	}

	for _, entry := range entries {
		matches := r.FindStringSubmatch(entry)
		if len(matches) >= 2 {
			nodeID = matches[1]
			break
		}
	}

	return nodeID, err
}

func (h DirectHandleType) nodeStatistics() (Statistics, error) {
	stat := Statistics{}
	nodeID, err := h.id()

	if err != nil {
		return stat, err
	}

	prefix := "$qdb.statistics." + nodeID + "."
	err = h.getStatistics(prefix, &stat)
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
