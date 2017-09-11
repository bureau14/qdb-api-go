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

// NodeConfig : a json representation object containing the configuration of a node
type NodeConfig struct {
	Local struct {
		Depot struct {
			SyncEveryWrite         bool   `json:"sync_every_write"`
			Root                   string `json:"root"`
			HeliumURL              string `json:"helium_url"`
			MaxBytes               int64  `json:"max_bytes"`
			StorageWarningLevel    int    `json:"storage_warning_level"`
			StorageWarningInterval int    `json:"storage_warning_interval"`
			DisableWal             bool   `json:"disable_wal"`
			DirectRead             bool   `json:"direct_read"`
			DirectWrite            bool   `json:"direct_write"`
			MaxTotalWalSize        int    `json:"max_total_wal_size"`
			MetadataMemBudget      int    `json:"metadata_mem_budget"`
			DataCache              int    `json:"data_cache"`
			Threads                int    `json:"threads"`
			HiThreads              int    `json:"hi_threads"`
			MaxOpenFiles           int    `json:"max_open_files"`
		} `json:"depot"`
		User struct {
			LicenseFile string `json:"license_file"`
			LicenseKey  string `json:"license_key"`
			Daemon      bool   `json:"daemon"`
		} `json:"user"`
		Limiter struct {
			MaxResidentEntries int   `json:"max_resident_entries"`
			MaxBytes           int64 `json:"max_bytes"`
			MaxTrimQueueLength int   `json:"max_trim_queue_length"`
		} `json:"limiter"`
		Logger struct {
			LogLevel      int    `json:"log_level"`
			FlushInterval int    `json:"flush_interval"`
			LogDirectory  string `json:"log_directory"`
			LogToConsole  bool   `json:"log_to_console"`
			LogToSyslog   bool   `json:"log_to_syslog"`
		} `json:"logger"`
		Network struct {
			ServerSessions  int    `json:"server_sessions"`
			PartitionsCount int    `json:"partitions_count"`
			IdleTimeout     int    `json:"idle_timeout"`
			ClientTimeout   int    `json:"client_timeout"`
			ListenOn        string `json:"listen_on"`
		} `json:"network"`
		Chord struct {
			NodeID                   string        `json:"node_id"`
			NoStabilization          bool          `json:"no_stabilization"`
			BootstrappingPeers       []interface{} `json:"bootstrapping_peers"`
			MinStabilizationInterval int           `json:"min_stabilization_interval"`
			MaxStabilizationInterval int           `json:"max_stabilization_interval"`
		} `json:"chord"`
	} `json:"local"`
	Global struct {
		Cluster struct {
			Transient              bool `json:"transient"`
			History                bool `json:"history"`
			ReplicationFactor      int  `json:"replication_factor"`
			MaxVersions            int  `json:"max_versions"`
			MaxTransactionDuration int  `json:"max_transaction_duration"`
		} `json:"cluster"`
		Security struct {
			EnableStop         bool   `json:"enable_stop"`
			EnablePurgeAll     bool   `json:"enable_purge_all"`
			Enabled            bool   `json:"enabled"`
			EncryptTraffic     bool   `json:"encrypt_traffic"`
			ClusterPrivateFile string `json:"cluster_private_file"`
			UserList           string `json:"user_list"`
		} `json:"security"`
	} `json:"global"`
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
