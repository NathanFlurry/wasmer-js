# WASIX Pipe SIGPIPE Bug

## Status: ROOT CAUSE IDENTIFIED

## Summary

After fixing the BorrowMutError panics, bash pipe commands now fail with a SIGPIPE ("Broken pipe") signal instead of panicking. The pipe is created but data doesn't flow properly between processes.

## Root Cause

**SharedArrayBuffer is not transferred when forking child processes.**

When bash creates a pipe with `fd_pipe()`:
1. A SharedPipe is created using SharedArrayBuffer (SAB)
2. The SAB allows cross-Worker communication
3. When bash forks for child processes (echo, cat), the WasiEnv is cloned
4. The cloned WasiEnv is sent to a new Worker via postMessage
5. **BUT the SharedArrayBuffer inside the SharedPipe is NOT included in the postMessage transfer list**
6. The child worker has a "dead" reference to a SAB it can't access
7. Any pipe read/write fails, causing SIGPIPE

The SharedArrayBuffer in JavaScript MUST be explicitly included in the postMessage transfer list to be shared across Workers. Currently, only the main process stdio pipes handle this (via `subprocess_stdio` in `PostMessagePayload`).

## Reproduction

```javascript
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo test | cat"],
  stdin: ''
});
const result = await instance.wait();
// result.stdout = ""
// result.stderr = "Program recieved termination signal: Broken pipe\n"
// result.code = 45
```

## Error Analysis

The error message "Broken pipe" indicates:
1. Bash forks to create the pipe
2. `echo` tries to write to the pipe's write end
3. The pipe's read end (connected to `cat`) is closed before `echo` finishes writing
4. SIGPIPE is delivered to `echo`, killing it

## Working Cases

These work correctly, showing basic subprocess functionality is fine:

```javascript
// Simple echo - WORKS
await bash.run({ args: ["-c", "echo hello"] });
// stdout = "hello\n"

// Multiple commands with semicolon - WORKS
await bash.run({ args: ["-c", "echo a; echo b"] });
// stdout = "a\nb\n"

// File redirect with cat - WORKS
await bash.run({ args: ["-c", "echo test > /tmp/x && cat /tmp/x"] });
// stdout = "test\n"
```

## Hypotheses

### Hypothesis 1: Subprocess spawn timing issue

The pipe might be set up correctly, but the reader process (`cat`) might not be spawned before `echo` tries to write. In a single-threaded WASM environment with async scheduling, race conditions between process spawning could cause this.

### Hypothesis 2: Pipe file descriptor not inherited correctly

When bash forks to create the pipe, the child processes might not properly inherit the pipe file descriptors. The pipe could be created but not connected to the child's stdin/stdout.

### Hypothesis 3: SharedPipe implementation issue

The wasmer-js SharedPipe (using SharedArrayBuffer) might have issues with the close/EOF signaling. If the read end signals EOF prematurely, the write would fail with SIGPIPE.

### Hypothesis 4: posix_spawn vs fork issue

Bash might use different mechanisms for pipes:
- Pipes between commands use fork()
- Command substitution might use posix_spawn()

The fork() implementation in WASIX might have issues with pipe inheritance.

## Investigation Plan

1. Add tracing to wasmer-wasix to log pipe operations
2. Check if both child processes are spawned
3. Trace pipe file descriptor inheritance during fork
4. Check SharedPipe read/write/close operations
5. Compare with file redirect (which works)

## Proposed Fix

The fix requires extracting SharedArrayBuffers from the forked WasiEnv's file descriptors and including them in the postMessage transfer list.

### Option 1: Enumerate pipes in WasiEnv before spawning

Before sending the SpawnWasm message, scan the WasiEnv's fd_map for SharedPipe entries:

```rust
// In to_scheduler_message() for fork:

// 1. Scan fd_map for VirtualPipeRx/VirtualPipeTx with SharedPipe
let pipe_buffers = env.state.fs.enumerate_shared_pipe_buffers();

// 2. Include them in the transfer list
SchedulerMessage::SpawnWithModuleAndMemory {
    ...
    pipe_buffers: Some(pipe_buffers),  // Vec<(WasiFd, SharedArrayBuffer)>
}
```

On the child side, reconnect the SharedArrayBuffers to the pipes.

### Option 2: Use wasm shared memory for pipe buffers

Instead of using JavaScript SharedArrayBuffer, allocate pipe buffers in WASM shared memory (which is already transferred). This would require changes to SharedPipe to use WASM memory directly.

### Option 3: Lazy pipe creation in child

Don't try to share pipes across fork. Instead:
1. Mark pipe FDs as "deferred" when forking
2. On first access in child, re-create the connection via the scheduler
3. Use a message-passing mechanism between workers for the actual data

## Related Files

- `/home/nathan/misc/wasmer/lib/wasix/src/syscalls/wasix/proc_fork.rs`
- `/home/nathan/misc/wasmer-js/src/pipes/mod.rs` - SharedPipe implementation
- `/home/nathan/misc/wasmer-js/src/tasks/task_wasm.rs` - SpawnWasm creation
- `/home/nathan/misc/wasmer-js/src/tasks/post_message_payload.rs` - postMessage serialization
- `/home/nathan/misc/wasmer/lib/wasix/src/fs/mod.rs` - File descriptor handling

## Related Issues

- The backticks timeout is likely the same issue (command substitution uses fork+pipe)
- Exit code 45 is related to signal handling (45 = signal 13 + 32 = SIGPIPE + base)
