# Command Substitution Issue in wasmer-js

## Summary

`$()` command substitution fails silently in bash running on wasmer-js, while backticks work correctly.

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| `` `cmd` `` | ✅ Works | Simple backticks |
| `` `echo \`nested\`` `` | ✅ Works | Fixed in commit `57f0d76b5` |
| `$(cmd)` | ❌ Fails | Requires `/dev/fd` (see below) |
| `(subshell)` | ✅ Works | No pipe capture needed |
| `$((1+1))` | ✅ Works | Arithmetic, no fork |
| `echo \| cat` | ✅ Works | Shell pipelines work |

## Root Cause

### Nested Backticks (Fixed)

Nested backticks previously failed with "cannot duplicate pipe as fd 1: Invalid argument".

**Root cause**: When `fd_renumber(pipe_fd, 1)` was called in a nested subprocess, the flush operation on fd 1 (stdout) failed because stdout had been replaced with a VirtualPipe by the outer subprocess. The `flush()` function expected fd 1 to be `Kind::File`, but it was a VirtualPipe.

**Fix**: Modified `flush()` in `fs/mod.rs` to handle the `FsError::NotAFile` case for stdio fds gracefully, since pipes don't require explicit flushing.

**Commit**: `57f0d76b5` in wasmer

### $() Command Substitution (Not Yet Fixed)

`$()` fails because the `sharrattj/bash` package was compiled with `HAVE_DEV_FD` enabled, which requires `/dev/fd` to be available. WASIX does not provide `/dev/fd`.

**Evidence**:
```bash
ls -la /dev/fd 2>&1
# ls: cannot access '/dev/fd': No such file or directory
```

Bash uses `/dev/fd/<n>` to pass file descriptors between processes for `$()` substitution. Without it, bash silently fails before even attempting to create pipes.

Backticks use an older code path that doesn't rely on `/dev/fd`.

## Solutions for $()

1. **Add `/dev/fd` support to WASIX**: Implement a virtual `/dev/fd` filesystem that maps `/dev/fd/N` to file descriptor N. This is the proper fix.

2. **Rebuild bash without `HAVE_DEV_FD`**: Build a new bash package with `--disable-dev-fd-stat-broken` or similar configure options. Bash will fall back to using named pipes (FIFOs).

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
import { init, Wasmer } from '@anthropic/wasmer-sdk';

await init();
const pkg = await Wasmer.fromRegistry("sharrattj/bash");

// These work
let r1 = await pkg.commands["bash"].run({ args: ["-c", "echo `echo hello`"] });
console.log((await r1.wait()).stdout); // "hello\n"

let r2 = await pkg.commands["bash"].run({ args: ["-c", "echo `echo \\`echo nested\\``"] });
console.log((await r2.wait()).stdout); // "nested\n"

// This fails (needs /dev/fd)
let r3 = await pkg.commands["bash"].run({ args: ["-c", "echo $(echo hello)"] });
console.log((await r3.wait()).stdout); // ""
```

## References

- [Bash Process Substitution Wiki](https://wiki.bash-hackers.org/syntax/expansion/cmdsubst)
- [Process Substitution Wikipedia](https://en.wikipedia.org/wiki/Process_substitution)
- [Alpine /dev/fd issue](https://gitlab.alpinelinux.org/alpine/aports/-/issues/1465)
