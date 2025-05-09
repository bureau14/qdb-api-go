#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"
$SCRIPT_DIR/set_version.sh

# Fix permission issue when using docker builds
git config --global --add safe.directory '*'

${GO} build -v -x
