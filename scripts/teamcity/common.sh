#!/usr/bin/env bash

set -eu

##
# Define default commands
REALPATH=$(command -v realpath)

##
# Expect the GOROOT and GOPATH to be provided

GOROOT=${GOROOT:-}
GOPATH=${GOPATH:-}

if [[ -z "${GOROOT}" ]]
then
    echo "GOROOT environment variable is expect to be set"
    exit 1
fi

if [[ -z "${GOPATH}" ]]
then
    echo "GOPATH environment variable is expect to be set"
    exit 1
fi

GO=$(${REALPATH} "${GOROOT}/bin/go")

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
