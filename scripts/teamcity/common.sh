#!/usr/bin/env bash

set -eu

##
# Define default commands/variables
REALPATH=$(command -v realpath)
SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
BASE_DIR=$(${REALPATH} "${SCRIPT_DIR}/../../")
QDB_API_DIR=$(${REALPATH} "${BASE_DIR}/qdb/")
QDB_LIB_DIR=$(${REALPATH} "${QDB_API_DIR}/lib/")

echo "SCRIPT_DIR: ${SCRIPT_DIR}"
echo "BASE_DIR: ${BASE_DIR}"
echo "QDB_API_DIR: ${QDB_API_DIR}"
echo "QDB_LIB_DIR: ${QDB_LIB_DIR}"

##
# Validation of the GOROOT and GOPATH env vars

GOROOT=${GOROOT:-}
GOPATH=${GOPATH:-}

if [[ -z "${GOPATH}" ]]
then
    echo "GOPATH environment variable is expect to be set"
    exit 1
fi

LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}
DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH:-}
CGO_CFLAGS=${CGO_CFLAGS:-}
CGO_LDFLAGS=${CGO_LDFLAGS:-}

##
# Add QuasarDB's library path to LD_LIBRARY_PATH since we dynamically
# link libqdb_api.so/dylib

case $(uname) in
    Linux | FreeBSD )
        export LD_LIBRARY_PATH="${QDB_LIB_DIR}:${LD_LIBRARY_PATH}"
        echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
        ;;

    Darwin )
        export DYLD_LIBRARY_PATH="${QDB_LIB_DIR}:${DYLD_LIBRARY_PATH}"
        export CGO_CFLAGS="$CGO_CFLAGS -I${QDB_API_DIR}/include"
        export CGO_LDFLAGS="$CGO_LDFLAGS -L${QDB_LIB_DIR} -Wl,-rpath -Wl,${QDB_LIB_DIR}"
        echo "DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}"
        echo "CGO_CFLAGS=${CGO_CFLAGS}"
        echo "CGO_LDFLAGS=${CGO_LDFLAGS}"
       ;;

    MINGW* )

        echo "Adding GCC to path"
        export PATH="/c/mingw64/bin:${PATH}"
        export PATH="${QDB_LIB_DIR}:${PATH}"
        export PATH="${QDB_API_DIR}/bin:${PATH}"
        echo "PATH: ${PATH}"
        ;;

    * )
        echo "Unable to probe environment"
        exit -1
        ;;
esac

##
# Validate installation of qdb/ base directory

GO=""

if [[ -z "${GOROOT}" ]]
then
    echo "GOROOT is not set, using go from path"
    GO=$(command -v go)
else
    echo "GOROOT is set, using go from GOROOT: ${GOROOT}/bin/go"
    GO=$(${REALPATH} "${GOROOT}/bin/go")
fi

if [[ ! -x "${GO}" ]]
then
    echo "Executable not found: ${GO}"
    exit 1
fi

echo "GOROOT: ${GOROOT}"
echo "GOPATH: ${GOPATH}"
echo "GO: ${GO}"

${GO} version

export GOROOT="${GOROOT}"
export GOPATH="${GOPATH}"
export GO="${GO}"
