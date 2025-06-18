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
