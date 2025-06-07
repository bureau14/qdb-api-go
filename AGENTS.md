---
Source: .ruler/10-introduction.md
---
# QuasarDB Go API

This project contains the source code for the QuasarDB Go API, implemented on top of the QuasarDB C API using CGO.

---
Source: .ruler/20-golang-guidelines.md
---
## Golang guidelines

* Version: ≥1.24
* Use strictly idiomatic, performance-oriented solutions with the standard library.
* Recommend third-party packages only when they provide documented, measurable advantages.
* Treat new language features as fully supported; e.g.:
  * integer range loops:
    `for i := range n { … }    // valid in Go 1.23+`
  * cloning slices:
    `slices.Clone()`
* Treat new library features as fullky supported; e.g.

---
Source: .ruler/21-cgo-guidelines.md
---
## CGO

This project relies on CGO for interaction with the QuasarDB C API.

In CGO, balance performance, correctness, and memory safety:
 * Clearly indicate when unsafe CGO variants improve performance by avoiding copies or allocations.
 * Name such variants using the ...Unsafe suffix to explicitly indicate their risk.
 * Always strictly adhere to Go 1.23+ runtime CGO pointer-safety constraints:
  * Never pass Go-managed memory pointers (e.g., pointers to slices or structs allocated by Go) directly into C functions.
  * Allocate memory intended for direct use by C explicitly via C functions (e.g., C.malloc) or approved Go→C allocation wrappers (such as qdbAllocBytes).
 * Always document:
  * Pointer lifetimes, memory ownership, and required release procedures.
  * Assumptions about type sizes, alignment, endianness, and binary representations, especially when directly copying between Go and C memory (e.g., float64 ↔ C.double).
 * Explicitly note if any assumption relies on IEEE-754 compliance or platform-specific ABI guarantees.
 * Provide robust, context-aware error handling, and detailed comments describing memory and performance trade-offs.

---
Source: .ruler/22-tests.md
---
## Testing Requirements

### Test setup

From the project root execute the startup script to launch the test clusters:

```bash
bash scripts/tests/setup/start-services.sh
```

### Running tests

To run the entire test suite, execute:

```bash
direnv exec . go test -v ./...
```

Because running the full test suite may take several minutes, it is strongly recommended that OpenAI Codex first runs individual test modules related specifically to recent changes, providing quicker feedback.

To run a single test, for example `TestReaderCanReadDataFromTables`, execute:

```bash
direnv exec . go test -v ./...  -run 'TestReaderCanReadDataFromTables'
```

To run tests based on prefix, for example all tests with the `TestReader` prefix, execute:

```bash
direnv exec . go test -v ./...  -run 'TestReader*'
```

### Tests teardown
From the project root execute the shutdown script:

```bash
bash scripts/tests/setup/stop-services.sh
```

---
Source: .ruler/30-project-structure.md
---
## Project Structure for Code Navigation

- `qdb/`: QuasarDB dependencies: header files, libraries and utilities. Not part of this git repo, never change.
  - `include/qdb/`: C API header files that define types and functions that CGO integrates directly with.
- `/scripts`: Utility scripts
  - `/tests/setup/`: Git submodule that defines scripts for starting and stopping the QuasarDB daemon in the background. Do **not** modify these scripts; syncing the submodule to a newer revision is allowed.
  - `/teamcity/`: Scripts invoked by TeamCity, our CI/CD tool
    - `10.build.sh`: Compiles the module without running test, to verify whether the build works.
    - `20.test.sh`: Runs the tests. Expects QuasarDB daemon to run in the background.
 - `/codex/`: Scripts used by OpenAI Codex to download dependencies and prepare the environment.

---
Source: .ruler/31-command-prefixing.md
---
## Environment Variables and Command Prefixing

This project uses `direnv` to manage environment variables essential for CGo compilation.
When generating any shell commands, **always prefix each command explicitly with**:

```bash
direnv exec . <command>
```

For example, to run `go env`, use:

```bash
direnv exec . go env
```

**Always** include the `direnv exec .` prefix on **every** generated shell command. Omitting it can lead to compilation failures due to missing environment variables.

### Resolving direnv Permission Errors:

If executing commands prefixed with `direnv exec` . results in the following error:

```bash
direnv: error /path/to/project/.envrc is blocked. Run `direnv allow` to approve its content
```

This indicates the .envrc file has not yet been explicitly trusted.
Immediately run the following command to resolve this issue:

```bash
direnv allow .
```

After running this, retry the original command:

```bash
direnv exec . <original command>
```

---
Source: .ruler/40-code-guidelines.md
---
## Performance

Performance is critical. Code will run trillions of times per day in a high-performance timeseries database.

- Avoid copies of arrays, but instead prefer zero-copy pointer-passing where possible
- Explicitly document trade-offs (complexity, maintainability, readability vs performance).
- Recommend explicitly when each approach should be chosen.
- Provide concise, actionable examples of recommended performance techniques.

## Function Documentation Style

When documenting functions, closely follow the commenting approach used at the bottom of `utils.go` and the generators in `test_utils.go`.

- Use concise, high-signal comments that explain *why* and *how* rather than repeating *what* the code already shows.
- Include bullet lists for key sections, each prefixed with `//`:
  - `// Decision rationale:` followed by short bullets describing choices made.
  - `// Key assumptions:` bullets stating the required preconditions or invariants.
  - `// Performance trade-offs:` bullets outlining costs versus benefits.
- Provide an inline usage example with each line starting with `//`. Avoid fenced code blocks so the comment format mirrors production code.
- Avoid trivial comments. Focus on explaining design choices, trade-offs and assumptions.
- Optimize for clarity and LLM readability. Never use emojis.

## Helper Function Guidelines

Create small, composable helper functions where practical. Prefer reusing existing helpers from `utils.go` and `test_utils.go` instead of duplicating logic.
