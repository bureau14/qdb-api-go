#!/bin/bash

set -xe

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/config.sh"

function cleanup {
    echo "Removing ${DATA_DIR_INSECURE}..."
    rm -Rf ${DATA_DIR_INSECURE} || true

    echo "Removing ${DATA_DIR_SECURE}..."
    rm -Rf ${DATA_DIR_SECURE} || true

    rm -Rf ${USER_LIST} || true
    rm -Rf ${USER_PRIVATE_KEY} || true
    rm -Rf ${CLUSTER_PUBLIC_KEY} || true
    rm -Rf ${CLUSTER_PRIVATE_KEY} || true
}

function full_cleanup {
    cleanup
    echo "Removing ${LOG_DIR_INSECURE}..."
    rm -Rf ${LOG_DIR_INSECURE} || true
    echo "Removing qdbd_insecure.out.txt ..."
    rm -Rf qdbd_insecure.out.txt || true
    echo "Removing qdbd_insecure.err.txt ..."
    rm -Rf qdbd_insecure.err.txt || true

    echo "Removing ${LOG_DIR_SECURE}..."
    rm -Rf ${LOG_DIR_SECURE} || true
    echo "Removing qdbd_secure.out.txt ..."
    rm -Rf qdbd_secure.out.txt || true
    echo "Removing qdbd_secure.err.txt ..."
    rm -Rf qdbd_secure.err.txt || true
}