#!/bin/bash

set -xe

QDB_DIR="qdb/bin"

if [[ ! -d ${QDB_DIR} ]]; then
    echo "Please provide a valid binary directory, got: ${QDB_DIR}"
    exit 1
fi

QDBD="${QDB_DIR}/qdbd"
QDBSH="${QDB_DIR}/qdbsh"
QDB_USER_ADD="${QDB_DIR}/qdb_user_add"
QDB_CLUSTER_KEYGEN="${QDB_DIR}/qdb_cluster_keygen"

if [[ ${CMAKE_BUILD_TYPE} == "Debug" ]]; then
    QDBD="${QDBD}d"
    QDBSH="${QDBSH}d"
    QDB_USER_ADD="${QDB_USER_ADD}d"
    QDB_CLUSTER_KEYGEN="${QDB_CLUSTER_KEYGEN}d"
fi

case "$(uname)" in
    MINGW*)
        QDBD=${QDBD}.exe
        QDBSH=${QDBSH}.exe
        QDB_USER_ADD=${QDB_USER_ADD}.exe
        QDB_CLUSTER_KEYGEN=${QDB_CLUSTER_KEYGEN}.exe
    ;;
    *)
    ;;
esac