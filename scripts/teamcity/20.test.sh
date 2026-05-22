#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Convert test output to JUnit XML so buildkite can understand it
${GO} test -v -test.run "Test*" -coverprofile=test-coverage.out | ${GO_JUNIT_REPORT} -out "${TEST_REPORT_DIR}/junit-test-report.xml" -iocopy
