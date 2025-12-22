# Scheduler-Routed Pipes: Implementation Status

## Overview

This document tracks the implementation status of scheduler-routed pipes for wasmer-js subprocess support.

## Current Status: Mostly Working

**Date:** 2025-12-21

### Test Results

| Test | Command | Result | Notes |
|------|---------|--------|-------|
| Simple echo | `echo hello` | ✅ PASS | |
| Multi-word echo | `echo foo bar baz` | ✅ PASS | |
| Simple pipe | `echo test \| cat` | ✅ PASS | |
| Double pipe | `echo abc \| cat \| cat` | ✅ PASS | |
| Sequential && | `echo a && echo b` | ✅ PASS | |
| Sequential ; | `echo c; echo d` | ✅ PASS | |
| Backticks | `` echo `echo hello` `` | ✅ PASS | |
| $() substitution | `echo $(echo world)` | ❌ FAIL | bash-WASIX compatibility |
| Write and cat | `echo test > /tmp/f.txt && cat /tmp/f.txt` | ✅ PASS | |
| Env var | `X=hello; echo $X` | ✅ PASS | |

**Summary: 9/10 tests pass**

## Fixes Applied

### 1. Sequential Execution Hang (Fixed)

**Problem:** Running multiple commands sequentially would hang on the 3rd+ command.

**Root Cause:** Interactive TTY mode creates a clone of `stdout_pipe` for the TTY. For stdout to get EOF:
1. The runner must drop its `stdout_pipe` (happens when WASM exits)
2. The TTY must drop its `stdout_pipe` clone (happens when TTY task exits)
3. The TTY task exits when stdin gets EOF via `u_stdin_rx.read_buf()`

There was a race condition where the TTY's stdin read didn't reliably see EOF when the JavaScript stdin WritableStream was closed, causing stdout to hang indefinitely.

**Fix:** Changed `setup_tty()` to use NonInteractive mode for all cases:
```rust
fn setup_tty(options: &SpawnOptions, tty_options: TtyOptions) -> TerminalMode {
    // Use non-interactive mode for all cases. Interactive mode has a race condition
    // where the TTY's stdin read may not see EOF reliably when the stdin
    // WritableStream is closed, causing stdout to never complete.
    let stdin_data = options.read_stdin().unwrap_or_default();
    return TerminalMode::NonInteractive {
        stdin: virtual_fs::StaticFile::new(stdin_data),
    };
    // ... Interactive TTY code disabled ...
}
```

**Commit:** `92882c8`

### 2. Scheduler Debug Logging (Added)

Added console logging for debugging pipe and subprocess issues:
- Scheduler message processing (every 50th message)
- Worker busy/idle state transitions
- Pipe create/write/read/close operations
- Scheduler close/shutdown events

**Commit:** `69f2751`

### 3. VirtualPipe Poll Guard Support (Fixed)

**Problem:** Polling on VirtualPipe file descriptors via poll_oneoff would return `Errno::Badf`.

**Root Cause:** `InodeValFilePollGuard::new()` didn't handle `Kind::VirtualPipeTx` and `Kind::VirtualPipeRx`, causing it to return `None` for these fd types.

**Fix:** Added VirtualPipe handling by mapping them to the File poll guard mode:
```rust
Kind::VirtualPipeTx { tx } => InodeValFilePollGuardMode::File(tx.clone()),
Kind::VirtualPipeRx { rx } => InodeValFilePollGuardMode::File(rx.clone()),
```

**Commit:** `ebf9a024e`

## Remaining Issue: $() Command Substitution

### Symptom

`echo $(echo hello)` produces empty output while `` echo `echo hello` `` works correctly.

### Error Message

When running nested backticks (which triggers similar code paths):
```
bash: command_substitute: cannot duplicate pipe as fd 1: Invalid argument
```

### Investigation Findings (2024-12-21)

Detailed syscall tracing revealed:

**For backticks (works):**
- `fd_pipe` syscall is called - pipe fds created
- `proc_fork` syscall is called - subprocess spawned
- Pipe I/O operations occur through scheduler
- Output captured successfully

**For $() (fails):**
- `fd_pipe` is **NOT called**
- `proc_fork` is **NOT called**
- No pipe operations occur
- bash exits silently with no output

**What works vs what fails:**
| Feature | Status | Notes |
|---------|--------|-------|
| `` `cmd` `` (simple backticks) | ✅ Works | Uses older bash code path |
| `$(cmd)` | ❌ Fails | bash doesn't call pipe/fork |
| `` `echo \`nested\`` `` | ❌ Fails | Same error as $() |
| `(subshell)` | ✅ Works | No pipe capture needed |
| `$((1+1))` | ✅ Works | Arithmetic, no fork needed |
| `echo | cat` | ✅ Works | Shell-level piping works |

### Root Cause Hypothesis

The issue appears to be in bash's internal handling of $() command substitution. When bash encounters $(), it uses a different code path than backticks that:
1. Checks for some system capability or feature
2. This check fails silently in WASIX/wasmer-js
3. Bash skips the entire command substitution without error

This is **NOT** a wasmer-js scheduler-routed pipes issue - the pipes work correctly for backticks and shell pipelines. It's a bash-WASIX compatibility issue.

### Fixes Applied

**VirtualPipe Poll Guard Support (commit `ebf9a024e`):**
Added VirtualPipeTx and VirtualPipeRx handling to `InodeValFilePollGuard::new()` in wasmer-wasix. Previously, polling on VirtualPipe fds would return `Errno::Badf`.

This fix enables proper poll_oneoff support for scheduler-routed pipes, though it didn't resolve the $() issue (since $() fails before creating pipes).

### Future Investigation

The $() issue requires investigation at the bash-WASIX interface level:
1. Trace bash's internal command_substitute() function behavior
2. Check what system call or check bash makes before creating the pipe for $()
3. May require changes to the bash WASM package or wasix-libc

## Architecture

### Scheduler-Routed Pipes

All pipe I/O is routed through the scheduler via postMessage:

```
Worker A                    Scheduler                   Worker B
   |                            |                           |
   |--PipeWrite(id, data)------>|                           |
   |                            |---(buffer data)---------->|
   |                            |<--PipeRead(id, len)-------|
   |                            |---PipeReadResponse------->|
   |--PipeClose(id)------------>|                           |
   |                            |---(signal EOF)----------->|
```

Messages:
- `PipeCreate { pipe_id, worker_id }` - Register a new pipe
- `PipeWrite { pipe_id, worker_id, data }` - Write data to pipe buffer
- `PipeRead { pipe_id, worker_id, max_len }` - Request data from pipe
- `PipeClose { pipe_id, worker_id }` - Close pipe, signal EOF

### Key Files

- `src/pipes/mod.rs` - SimplePipe, SimplePipeTx, SimplePipeRx implementations
- `src/tasks/scheduler.rs` - Pipe buffer management and message handling
- `src/runtime.rs` - Runtime::create_pipe() returns SimplePipe
- `src/wasmer.rs` - setup_tty() and TTY handling (NonInteractive fix)

## Performance

After the NonInteractive fix, execution times improved significantly:

| Before | After |
|--------|-------|
| ~8000ms first run | ~120ms first run |
| ~600ms subsequent | ~80ms subsequent |

The improvement is because NonInteractive mode doesn't need TTY stdin/stdout copying overhead.
