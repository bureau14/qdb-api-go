#!/bin/bash

set -xe

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/config.sh"
source "$SCRIPT_DIR/utils.sh"
source "$SCRIPT_DIR/cleanup.sh"

full_cleanup

qdb_add_user ${USER_LIST} ${USER_PRIVATE_KEY} "test-user"
qdb_gen_cluster_keys ${CLUSTER_PUBLIC_KEY} ${CLUSTER_PRIVATE_KEY}

echo "Cluster insecure:"
ARGS="-a ${URI_INSECURE} -r ${DATA_DIR_INSECURE} -l ${LOG_DIR_INSECURE}"
qdb_start "${ARGS}" qdbd_insecure.out.txt qdbd_insecure.err.txt

echo "Cluster secure:"
ARGS_SECURE="-c qdbd.cfg -a ${URI_SECURE} -r ${DATA_DIR_SECURE} -l ${LOG_DIR_SECURE} --security=true --cluster-private-file=${CLUSTER_PRIVATE_KEY} --user-list=${USER_LIST}"
qdb_start "${ARGS_SECURE}" qdbd_secure.out.txt qdbd_secure.err.txt
