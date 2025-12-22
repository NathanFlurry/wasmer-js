# Scheduler-Routed Pipes: Implementation Status

## Overview

This document tracks the implementation status of scheduler-routed pipes for wasmer-js subprocess support.

## Current Status: Mostly Working

**Date:** 2024-12-21

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
| $() substitution | `echo $(echo world)` | ❌ FAIL | WASIX dup2 issue |
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

## Remaining Issue: $() Command Substitution

### Symptom

`echo $(echo hello)` produces empty output while `` echo `echo hello` `` works correctly.

### Error Message

When running nested backticks (which triggers similar code paths):
```
bash: command_substitute: cannot duplicate pipe as fd 1: Invalid argument
```

### Root Cause Analysis

The error indicates that bash's internal `dup2()` call is failing when setting up file descriptors for command substitution. Specifically:
- bash tries to duplicate a pipe fd to fd 1 (stdout)
- WASIX returns EINVAL (Invalid argument)

This is a WASIX-level issue, not a scheduler-routed pipes issue. The pipe I/O itself works correctly (as proven by `echo test | cat` working), but the file descriptor duplication syscall is failing.

### Difference Between $() and Backticks

While both should be equivalent in bash, they may use different internal mechanisms:
- Backticks: Older mechanism, simpler pipe setup
- `$()`: Newer mechanism, may use more complex fd manipulation

Simple backticks work, but `$()` and nested backticks fail, suggesting the issue is with specific fd duplication patterns.

### Investigation Path

1. Check WASIX `fd_dup` / `fd_dup2` implementation in wasmer-wasix
2. Trace which syscall is failing and with what arguments
3. Compare successful backtick fd setup vs failing $() fd setup
4. May require wasmer-wasix changes to fix

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
