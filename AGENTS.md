# Project Agents.md Guide for OpenAI Codex

This Agents.md file provides comprehensive guidance for OpenAI Codex and other AI agents working with this codebase.

## Project Structure for OpenAI Codex Navigation

- `qdb/`: QuasarDB dependencies: header files, libraries and utilities. Not part of this git repo, never change.
  - `include/qdb/`: C API header files that define types and functions that CGO integrates directly with.
- `/scripts`: Utility scripts
  - `/tests/setup/`: Git submodule that defines scripts for starting and stopping the QuasarDB daemon in the background. Do **not** modify these scripts; syncing the submodule to a newer revision is allowed.
  - `/teamcity/`: Scripts invoked by TeamCity, our CI/CD tool
    - `10.build.sh`: Compiles the module without running test, to verify whether the build works.
    - `20.test.sh`: Runs the tests. Expects QuasarDB daemon to run in the background.
 - `/codex/`: Scripts used by OpenAI Codex to download dependencies and prepare the environment.

## Testing Requirements for OpenAI Codex

### Test setup

From the project root execute the startup script to launch the test clusters:

```bash
bash scripts/tests/setup/start-services.sh
```

### Running tests

To run the entire test suite, execute:

```bash
go test -v ./...
```

Because the full test suite can take a long time to run minutes, it is strongly recommended that OpenAI Codex first runs individual test modules related specifically to recent changes, providing quicker feedback.

To run a single test, for example `TestReaderCanReadDataFromSingleTable`, execute:

```bash
go test -v ./...  -run 'TestReaderCanReadDataFromSingleTable'
```

To run tests based on prefix, for example all tests with the `TestReader` prefix, execute:

```bash
go test -v ./...  -run 'TestReader*'
```

### Tests teardown
From the project root execute the shutdown script:

```bash
bash scripts/tests/setup/stop-services.sh
```
