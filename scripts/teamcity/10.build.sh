#!/usr/bin/env bash

set -eu

# sc-20192 TEST OF THE FAILURE PATH, reverted by the next commit: FreeBSD fails here.
if [ "$(uname -s)" = "FreeBSD" ]; then
    echo "sc-20192 failure-path test: failing the FreeBSD build on purpose"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Fix permission issue when using docker builds
git config --global --add safe.directory '*'

${GO} build -v -x
