#!/usr/bin/env bash

set -eu

# Print go env 
echo "Environment variables:"
echo "GOROOT: ${GOROOT}"
echo "GOPATH: ${GOPATH}"
echo "QDB_CICD_AGENT_GO123_GOPATH: ${QDB_CICD_AGENT_GO123_GOPATH}"
echo "QDB_CICD_AGENT_GO124_GOPATH: ${QDB_CICD_AGENT_GO124_GOPATH}"

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Fix permission issue when using docker builds
git config --global --add safe.directory '*'

${GO} build -v -x
