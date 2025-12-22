# Command Substitution Issue in wasmer-js

## Summary

`$()` command substitution fails silently with `sharrattj/bash` but works correctly with `wasmer/bash`.

## Solution

**Use `wasmer/bash` instead of `sharrattj/bash`**:

```javascript
// This works correctly
const pkg = await Wasmer.fromRegistry("wasmer/bash");
let instance = await pkg.commands["bash"].run({ args: ["-c", "echo $(echo hello)"] });
let result = await instance.wait();
console.log(result.stdout); // "hello\n"
```

## Current Status

| Feature | wasmer/bash | sharrattj/bash |
|---------|-------------|----------------|
| `` `cmd` `` | ✅ Works | ✅ Works |
| `` `echo \`nested\`` `` | ✅ Works | ✅ Works (fixed) |
| `$(cmd)` | ✅ Works | ❌ Broken (package bug) |
| `$($(nested))` | ✅ Works | ❌ Broken |
| `<(process sub)` | ❌ Needs `/dev/fd` | ❌ Needs `/dev/fd` |
| `(subshell)` | ✅ Works | ✅ Works |
| `$((1+1))` | ✅ Works | ✅ Works |

## Root Cause

### $() with sharrattj/bash

The `sharrattj/bash` package has a bug that causes `$()` command substitution to fail silently. This is **NOT** a WASIX issue - the `wasmer/bash` package works correctly.

The exact cause in the sharrattj/bash build is unknown, but it appears to be a compilation or configuration issue specific to that package.

### Nested Backticks (Fixed)

Nested backticks previously failed with "cannot duplicate pipe as fd 1: Invalid argument".

**Root cause**: When `fd_renumber(pipe_fd, 1)` was called in a nested subprocess, the flush operation on fd 1 (stdout) failed because stdout had been replaced with a VirtualPipe by the outer subprocess.

**Fix**: Modified `flush()` in `fs/mod.rs` to handle the `FsError::NotAFile` case for stdio fds gracefully.

**Commit**: `57f0d76b5` in wasmer

### Process Substitution `<()`

Process substitution like `cat <(echo hello)` fails with both bash packages because it requires `/dev/fd` which WASIX does not provide:

```
cat: /dev/fd/63: No such file or directory
```

**Solution**: Add virtual `/dev/fd` support to WASIX (future work).

## Fixes Applied

### 1. VirtualPipe Poll Guard Support (commit `ebf9a024e`)

Added VirtualPipe handling to `InodeValFilePollGuard::new()`:
```rust
Kind::VirtualPipeTx { tx } => InodeValFilePollGuardMode::File(tx.clone()),
Kind::VirtualPipeRx { rx } => InodeValFilePollGuardMode::File(rx.clone()),
```

### 2. Flush VirtualPipe on Stdio (commit `57f0d76b5`)

Modified `flush()` to handle `FsError::NotAFile` for stdout/stderr:
```rust
__WASI_STDOUT_FILENO => {
    match WasiInodes::stdout_mut(&self.fd_map) {
        Ok(mut file) => file.flush().await.map_err(map_io_err)?,
        Err(FsError::NotAFile) => {
            // fd 1 was replaced with a non-File (e.g., VirtualPipe)
            // Pipes don't need explicit flushing, so just succeed
            ()
        }
        Err(e) => return Err(fs_error_into_wasi_err(e)),
    }
}
```

## Test Script

```javascript
import { init, Wasmer } from '@wasmer/sdk';

await init();

// Use wasmer/bash for full $() support
const pkg = await Wasmer.fromRegistry("wasmer/bash");

// All of these work with wasmer/bash
let r1 = await pkg.commands["bash"].run({ args: ["-c", "echo $(echo hello)"] });
console.log((await r1.wait()).stdout); // "hello\n"

let r2 = await pkg.commands["bash"].run({ args: ["-c", "echo $(echo $(echo nested))"] });
console.log((await r2.wait()).stdout); // "nested\n"

let r3 = await pkg.commands["bash"].run({ args: ["-c", "for i in $(seq 1 3); do echo $i; done"] });
console.log((await r3.wait()).stdout); // "1\n2\n3\n"
```

## References

- [Bash Process Substitution Wiki](https://wiki.bash-hackers.org/syntax/expansion/cmdsubst)
- [Process Substitution Wikipedia](https://en.wikipedia.org/wiki/Process_substitution)
- [Alpine /dev/fd issue](https://gitlab.alpinelinux.org/alpine/aports/-/issues/1465)
