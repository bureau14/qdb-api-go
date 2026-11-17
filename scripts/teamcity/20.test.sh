#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Convert test output to JUnit XML so buildkite can understand it
# sc-20192 test branch only: TestReaderArrowEqualsPushedTablesPerType/4 (string
# columns) fails against quasardb master since 2026-11-08 (master builds 296,
# 298); skipped here so that the snapshot plugin can be tested on passing builds.
${GO} test -v -test.run "Test*" -test.skip "TestReaderArrowEqualsPushedTablesPerType/4" -coverprofile=test-coverage.out | ${GO_JUNIT_REPORT} -out "${TEST_REPORT_DIR}/junit-test-report.xml" -iocopy
