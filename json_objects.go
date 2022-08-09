package qdb

import "time"

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
		System int64 `json:"system"`
		User   int64 `json:"user"`
	} `json:"cpu_times"`
	DiskUsage struct {
		Free  int64 `json:"free"`
		Total int64 `json:"total"`
	} `json:"disk_usage"`
	Network struct {
		ListeningEndpoint string `json:"listening_endpoint"`
		Partitions        struct {
			Count             int `json:"count"`
			MaxSessions       int `json:"max_sessions"`
			AvailableSessions int `json:"available_sessions"`
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

type NodeTopology struct {
	Predecessor struct {
		Reference string `json:"reference"`
		Endpoint  string `json:"endpoint"`
	} `json:"predecessor"`
	Center struct {
		Reference string `json:"reference"`
		Endpoint  string `json:"endpoint"`
	} `json:"center"`
	Successor struct {
		Reference string `json:"reference"`
		Endpoint  string `json:"endpoint"`
	} `json:"successor"`
}
