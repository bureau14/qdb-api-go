#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# We need the output of `go test` to be in JUnit format for Buildkite's test reporting, but `go test` doesn't support that natively.
# We use the go-junit-report tool to convert the output of `go test` into JUnit XML format. The `-iocopy` flag is used to ensure that the output from the tests is still visible in the console, while also being captured in the JUnit report.
${GO} test -v -test.run "Test*" -coverprofile=test-coverage.out | ${GO_JUNIT_REPORT} -out "${TEST_REPORT_DIR}/junit-test-report.xml" -iocopy
