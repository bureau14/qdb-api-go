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
