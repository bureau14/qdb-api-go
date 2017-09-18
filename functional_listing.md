### [ general ]
    [v] qdb_version
    [v] qdb_build
    [v] qdb_open
    [x] qdb_open_tcp (doesn't return a pointer, no null return in case of failure)
    [v] qdb_option_set_timeout
    [v] qdb_option_set_max_cardinality
    [v] qdb_option_set_compression
    [v] qdb_connect
    [v] qdb_close
    [ ] qdb_copy_alloc_buffer
    [v] qdb_release
    [v] qdb_remove
    [v] qdb_expires_at
    [v] qdb_expires_from_now
    [x] qdb_get_expiry_time (deprecated, use qdb_get_metadata instead)
    [v] qdb_get_location
    [x] qdb_get_type (deprecated, use qdb_get_metadata instead)
    [v] qdb_get_metadata
    [ ] qdb_purge_all
    [ ] qdb_trim_all
    [v] qdb_error (used to define Error() 'overload')
    [ ] qdb_tag_iterator_copy
[ completion: 18/22 ]

### [ blobs ]
    [v] qdb_blob_get_noalloc
    [v] qdb_blob_get
    [x] qdb_reserved_blob_get (internal usage only, should not be exported in api)
    [v] qdb_blob_get_and_remove
    [v] qdb_blob_put
    [v] qdb_blob_update
    [x] qdb_reserved_blob_merge (internal usage only, should not be exported in api)
    [v] qdb_blob_get_and_update
    [v] qdb_blob_compare_and_swap
    [v] qdb_blob_remove_if
    [ ] qdb_blob_scan (experimental function, should I implement?)
    [ ] qdb_blob_scan_regex (experimental function, should I implement?)
[ completion: 10/12 ]

### [ cluster and nodes ]
    [v] qdb_node_status
    [v] qdb_node_config
    [v] qdb_node_topology
    [ ] qdb_node_stop
[ completion: 3/4 ]


### [ batches ]
    [ ] qdb_init_operations
    [ ] qdb_run_batch
    [ ] qdb_run_transaction
[ completion: 0/3 ]

### [ deques ]
    [ ] qdb_deque_size
    [ ] qdb_deque_get_at
    [ ] qdb_deque_set_at
    [ ] qdb_deque_push_front
    [ ] qdb_deque_push_back
    [ ] qdb_deque_pop_front
    [ ] qdb_deque_pop_back
    [ ] qdb_deque_front
    [ ] qdb_deque_back
[ completion: 0/9 ]

### [ integers ]
    [v] qdb_int_put
    [v] qdb_int_update
    [v] qdb_int_get
    [v] qdb_int_add
[ completion: 4/4 ]

### [ iterators ]
    [ ] qdb_iterator_begin
    [ ] qdb_iterator_rbegin
    [ ] qdb_iterator_next
    [ ] qdb_iterator_previous
    [ ] qdb_iterator_close
    [ ] qdb_iterator_copy
[ completion: 0/6 ]

### [ log ]
    [ ] qdb_log_add_callback
    [ ] qdb_log_remove_callback
[ completion: 0/2 ]

### [ hash sets ]
    [ ] qdb_hset_insert
    [ ] qdb_hset_erase
    [ ] qdb_hset_contains
[ completion: 0/3 ]

### [ prefix ]
    [ ] qdb_prefix_get
    [ ] qdb_prefix_count
[ completion: 0/2 ]

### [ query ]
    [v] qdb_query
[ completion: 1/1 ]

### [ streams ]
    [ ] qdb_stream_open
    [ ] qdb_stream_close
    [ ] qdb_stream_read
    [ ] qdb_stream_write
    [ ] qdb_stream_size
    [ ] qdb_stream_getpos
    [ ] qdb_stream_setpos
    [ ] qdb_stream_truncate
[ completion: 0/8 ]

### [ suffix ]
    [ ] qdb_suffix_get
    [ ] qdb_suffix_count
[ completion: 0/2 ]

### [ tags ]
    [v] qdb_attach_tag
    [v] qdb_attach_tags
    [v] qdb_has_tag
    [v] qdb_detach_tag
    [v] qdb_detach_tags
    [v] qdb_get_tagged
    [v] qdb_get_tags
    [ ] qdb_tag_iterator_begin
    [ ] qdb_tag_iterator_next
    [ ] qdb_tag_iterator_close
[ completion: 7/10 ]

### [ timeseries ]
    [v] qdb_ts_insert_columns
    [v] qdb_ts_double_insert
    [v] qdb_ts_double_get_ranges
    [v] qdb_ts_double_aggregate
    [v] qdb_ts_blob_insert
    [v] qdb_ts_blob_get_ranges
    [v] qdb_ts_blob_aggregate
    [v] qdb_ts_create
    [v] qdb_ts_erase_ranges
    [v] qdb_ts_list_columns
[ completion: 10/10 ]