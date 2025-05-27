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
Before running tests, from the git project's root, always execute:

```bash
bash scripts/tests/setup/start-services.sh
```

### Running tests

For running tests, reuse our teamcity test script:

```bash
bash scripts/teamcity/20.test.sh
```

### Tests teardown
After running tests, from the git project's root, always execute (even in case of test failure):

```bash
bash scripts/tests/setup/stop-services.sh
```
