# Project Agents.md Guide for OpenAI Codex

This Agents.md file provides comprehensive guidance for OpenAI Codex and other AI agents working with this codebase.

## Project Structure for OpenAI Codex Navigation

- `qdb/`: QuasarDB dependencies: header files, libraries and utilities. Not part of this git repo, never change.
  - `include/qdb/`: C API header files that define types and functions that CGO integrates directly with.
- `/scripts`: Utility scripts
  - `/tests/setup/`: Git submodule that define utility scripts for starting and launching QuasarDB daemon in the background. Not part of this git repo, never change contents, but syncing the submodule to a newer revision is allowed.
  - `/teamcity/`: Scripts invoked by Teamcity, our CI/CD tool
  - `/codex/`: Scripts invoked by OpenAI Codex to download dependencies.

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
