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

LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}
DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH:-}
CGO_CFLAGS=${CGO_CFLAGS:-}
CGO_LDFLAGS=${CGO_LDFLAGS:-}

# Validate that go-junit-report is installed, if not install it
if ! command -v go-junit-report > /dev/null 2>&1; then
    echo "go-junit-report not found, installing"
    ${GO} install github.com/jstemmer/go-junit-report/v2@latest
else
    echo "go-junit-report is already installed; skipping installation."
fi
export GO_JUNIT_REPORT="${GOPATH}/bin/go-junit-report"
$GO_JUNIT_REPORT --version
export TEST_REPORT_DIR="${BASE_DIR}/test-reports"
mkdir -p "${TEST_REPORT_DIR}"

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

        # We need to decide whether to use mingw64 or mingw32, we will probe whether the
        # go binary is 32bit or 64bit to decide this.
        VERSION=$(${GO} version)

        echo "Adding GCC to path"

        if [[ "${VERSION}" == *386 ]]
        then
            echo "32bit go detected, using 32bit mingw"
            export PATH="/c/mingw32/bin:${PATH}"
        else
            echo "64bit go detected, using 64bit mingw"
            export PATH="/c/mingw64/bin:${PATH}"
        fi

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

export GOROOT="${GOROOT}"
export GOPATH="${GOPATH}"
export GO="${GO}"
