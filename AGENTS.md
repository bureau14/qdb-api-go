# Project Agents.md Guide for OpenAI Codex

This Agents.md file provides comprehensive guidance for OpenAI Codex and other AI agents working with this codebase.

## Project Structure for OpenAI Codex Navigation

- `qdb/`: QuasarDB dependencies: header files, libraries and utilities. Not part of this git repo, never change.
  - `include/qdb/`: C API header files that define types and functions that CGO integrates directly with.
- `/scripts`: Utility scripts
  - `/tests/setup/`: Git submodule that defines scripts for starting and stopping the QuasarDB daemon in the background. Do **not** modify these scripts; syncing the submodule to a newer revision is allowed.
  - `/teamcity/`: Scripts invoked by TeamCity, our CI/CD tool
  - `/codex/`: Scripts used by OpenAI Codex to download dependencies and prepare the environment.

## Testing Requirements for OpenAI Codex


### Test setup

From the project root execute:

```bash
bash scripts/tests/setup/start-services.sh
```

### Running tests

Reuse our TeamCity test script:

```bash
bash scripts/teamcity/20.test.sh
```

### Tests teardown
From the project root execute:

```bash
bash scripts/tests/setup/stop-services.sh
```
