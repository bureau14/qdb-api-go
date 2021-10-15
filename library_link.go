package qdb

// #cgo CFLAGS: -I${SRCDIR}/qdb/include
// #cgo LDFLAGS: -L${SRCDIR}/qdb/bin -L${SRCDIR}/qdb/lib -lasio -lboost_filesystem -lboost_program_options -lbrigand -lfmt -lgeohash -llibsodium -llz4 -lqdb_aggregation -lqdb_api_static -lqdb_application -lqdb_auth -lqdb_chord -lqdb_client -lqdb_compression -lqdb_config -lqdb_crypto -lqdb_id -lqdb_io -lqdb_json -lqdb_log -lqdb_memory -lqdb_metadata -lqdb_network -lqdb_network_resolver -lqdb_perf -lqdb_persistence -lqdb_protocol -lqdb_query -lqdb_query_client -lqdb_query_dsl -lqdb_serialization -lqdb_sys -lqdb_time -lqdb_timeseries -lqdb_util -lqdb_version -lrobin_hood -lrocksdb -lskein -ltbb -ltbbmalloc -lxxhash -lzlibstatic -lzstd -lstdc++ -lm
import "C"
