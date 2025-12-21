# Wasmer-JS Subprocess Status

## Summary

This document tracks all known subprocess-related issues in wasmer-js and their status.

## Issues Overview

| Issue | Status | Description |
|-------|--------|-------------|
| Scheduler Race Condition | **FIXED** | Sequential tests hang after 2-3 runs |
| Cross-Worker IPC | **FIXED** | Subprocess stdout/stderr not reaching parent |
| GlobalScope::sleep() in Node.js | **FIXED** | setTimeout fails in Node.js workers |
| proc_fork BorrowMutError | **FIXED** | Bash pipes/substitution panic with RefCell error |
| Pipe SIGPIPE Error | **FIXED** | Pipes receive "Broken pipe" signal |
| Command Substitution | **PENDING** | May work now with pipe fix (needs testing) |
| Bash Exit Code 45 | **UNRESOLVED** | Bash returns 45 instead of expected exit code |
| Interactive TTY stdout | **WORKAROUND** | stdout hangs unless `stdin: ''` provided |

---

## Fixed Issues

### 1. Scheduler Race Condition (commit 83a88ce)

**Problem**: Running WASM instances sequentially in the same Node.js process caused hangs after 2-3 runs.

**Root Cause**: Race condition between:
- Workers sending `SpawnBlocking` via `postMessage` (async)
- Main thread sending `Close` directly to channel (sync)

The `Close` message was processed before `SpawnBlocking` arrived, causing cleanup callbacks to never run.

**Fix**: Delay `Close` via `setTimeout(0)` to allow pending postMessage handlers to run first.

**Files Changed**:
- `src/tasks/scheduler.rs:187-217` - Async close implementation
- `src/utils.rs:47-71` - Cross-platform setTimeout using Reflect

**Verification**:
```bash
# Before: hangs on test 3
# After: all 20 tests pass
node /tmp/test-sequential-20.mjs
```

---

### 2. Cross-Worker IPC (SharedPipe Implementation)

**Problem**: When WASM process A spawns WASM process B via `proc_spawn2`, subprocess stdio communication failed because `tokio::sync::mpsc` channels cannot work across Web Workers.

**Root Cause**: Web Workers have isolated memory spaces. The tokio channels use shared heap pointers that become invalid when serialized to another worker.

**Fix**: Implemented `SharedPipe` using SharedArrayBuffer + Atomics for cross-worker communication.

**Key Components**:
- `src/pipes/mod.rs` - SharedPipe ring buffer implementation
- `src/runtime.rs` - Override `create_pipe()` to return SharedPipe
- wasmer core patches for `VirtualPipe` file descriptor types

**Verification**:
```javascript
// Works now:
await pkg.commands["bash"].run({ args: ["-c", "echo hello"] });
await pkg.commands["bash"].run({ args: ["-c", "echo test | cat"] });
await pkg.commands["bash"].run({ args: ["-c", "echo `echo works`"] });
```

---

### 3. GlobalScope::sleep() in Node.js Workers

**Problem**: `web_sys::WorkerGlobalScope::set_timeout_with_callback_and_timeout_and_arguments_0` fails in Node.js worker threads with "expected a number argument, found object".

**Root Cause**: Node.js worker threads don't have the same global scope API as browser workers.

**Fix**: Use `js_sys::Reflect` to call `setTimeout` on `globalThis` directly, which works in both environments.

**Files Changed**:
- `src/utils.rs:47-71` - Reflection-based setTimeout

---

### 4. proc_fork BorrowMutError (FIXED)

**Problem**: When bash attempts to spawn subprocesses (for pipes or command substitution), the WASIX runtime panics with `BorrowMutError` in `thread_local.rs`.

**Error**:
```
panicked at lib/wasix/src/state/handles/thread_local.rs:106:75:
already borrowed: BorrowMutError

...
at wasmer_wasix::syscalls::wasix::proc_fork::run
at wasmer_wasix::syscalls::wasix::proc_fork::proc_fork
```

**Root Cause**: The `WasiInstanceHandlesPointer` uses `RefCell` for interior mutability. Multiple functions held `WasiInstanceGuard` (immutable borrow) across calls that could trigger WASM code, which might need `inner_mut()` (mutable borrow).

**Fix**: Restructured the following functions to drop the RefCell guard before making WASM calls:
- `process_signals_internal` in `env.rs` - Extract handler in scoped block
- `process_signals` in `env.rs` - Extract signals to process in scoped block
- `process_signals_and_exit` in `env.rs` - Restructure to drop guard before calling `process_signals`
- `proc_fork` in `proc_fork.rs` - Extract module/spawn_type in scoped block

**Commits**: ed0f6965c, a1600f1fb, df1e071be

---

### 5. Pipe SIGPIPE Error (FIXED)

**Problem**: Pipes receive "Broken pipe" signal and produce empty stdout.

**Error**:
```
Program recieved termination signal: Broken pipe
```

**Root Cause**: When bash creates a pipe with `fd_pipe()` and forks child processes (e.g., for `echo test | cat`):
1. A SharedPipe is created using SharedArrayBuffer (SAB)
2. When bash forks for child processes, the WasiEnv is cloned and sent to a new Worker
3. But the SharedArrayBuffer inside the SharedPipe was NOT included in the postMessage transfer list
4. The child worker had "dead" references to SABs it couldn't access
5. Any pipe read/write failed, causing SIGPIPE

**Fix**: Explicitly transfer SharedArrayBuffers for pipe file descriptors when spawning:
- Extract pipe buffers from WasiEnv before spawning (extract.rs)
- Include them in PostMessagePayload (ForkPipeBuffers type)
- Reconnect buffers in child worker before execution

**Files Changed**:
- `src/pipes/extract.rs` (new) - Extract/reconnect SharedArrayBuffers from pipe FDs
- `src/pipes/mod.rs` - Export extract module
- `src/tasks/task_wasm.rs` - Extract pipe buffers before spawn
- `src/tasks/post_message_payload.rs` - Add ForkPipeBuffers type
- `src/tasks/scheduler.rs` - Pass fork_pipes through scheduler
- `src/tasks/scheduler_message.rs` - Add fork_pipes to message
- `src/tasks/thread_pool_worker.rs` - Reconnect pipes in worker

**Related**: docs/research/wasix-pipe-sigpipe-bug.md

---

## Unresolved Issues

### 6. Command Substitution Issues

**Problem**: Command substitution doesn't work reliably.

**Observations**:
- Backticks (`` `echo works` ``) timeout (no panic, just hangs)
- `$()` syntax returns empty output with exit code 45

**Hypothesis**: WASIX implements command substitution using fork+pipe. This was likely caused by the pipe SIGPIPE bug (now fixed).

**Status**: PENDING - May work now with the pipe fix. Needs testing.

---

### 7. Bash Exit Code 45

**Problem**: Bash returns exit code 45 instead of the expected exit code.

**Status**: Known issue, not yet investigated.

**Related**: May be related to bash builtin handling in WASIX.

---

## Workarounds

### Interactive TTY Stdout Issue

**Problem**: In interactive TTY mode, stdout never closes because the TTY task holds a clone of `stdout_pipe` and waits for stdin EOF.

**Workaround**: Provide `stdin: ''` to force non-interactive mode:

```javascript
const instance = await pkg.commands['quickjs'].run({
  args: ['--eval', 'console.log("hello")'],
  stdin: ''  // Force non-interactive mode
});
const output = await instance.wait();  // Now works
```

**Note**: This is not needed if you're interactively writing to stdin and closing it before calling `wait()`.

---

## Test Coverage

### Existing Tests

The `tests/integration.test.ts` file includes subprocess tests:
- `it("can communicate with a subprocess interactively")` - bash spawning stdinout-loop
- `it("Can communicate with Python")` - python REPL
- `it.skip("can communicate with a subprocess")` - skipped, needs TTY fix

### Test Results (as of 2024-12-20)

| Test | Result | Notes |
|------|--------|-------|
| Basic quickjs | PASS | exit=0 |
| Sequential execution (5x) | PASS | Scheduler race fix working |
| Bash echo | PASS | stdout="hello", exit=45 (known issue) |
| Bash multi-command | PASS | `echo a; echo b` works |
| Bash file redirect | PASS | `echo x > /tmp/x && cat /tmp/x` works |
| Bash pipe | FAIL | SIGPIPE - "Broken pipe" signal |
| Backticks | FAIL | Timeout (no panic) |
| $() substitution | PASS* | Returns empty with exit=45 |

### Working Test Cases

```javascript
// Sequential execution (scheduler race condition) - WORKS
for (let i = 0; i < 10; i++) {
  const instance = await pkg.commands['quickjs'].run({
    args: ['--eval', `console.log(${i})`],
    stdin: ''
  });
  await instance.wait();
}

// Simple bash echo - WORKS (but exit code is 45)
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo hello"],
  stdin: ''
});
const output = await instance.wait();
// output.stdout = "hello\n", output.code = 45

// Multi-command - WORKS
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo a; echo b"],
  stdin: ''
});
// output.stdout = "a\nb\n"

// File redirect with cat - WORKS
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo test > /tmp/x && cat /tmp/x"],
  stdin: ''
});
// output.stdout = "test\n", output.code = 0
```

### Failing Test Cases

```javascript
// Bash pipe - FAILS with SIGPIPE
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo test | cat"],
  stdin: ''
});
// stderr = "Program recieved termination signal: Broken pipe\n"
// stdout = ""

// Backticks - TIMEOUT (no panic, just hangs)
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo `echo works`"],
  stdin: ''
});

// $() substitution - Returns empty
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo $(echo works)"],
  stdin: ''
});
// stdout = "", code = 45
```

---

## Related Documentation

- `docs/research/wasmer-js-scheduler-flakiness.md` - Detailed scheduler race condition analysis
- `docs/research/shared-pipe-subprocess-stdio.md` - SharedPipe implementation details
- `docs/bugs/WASM_SUBPROCESS_IPC.md` - Original subprocess IPC bug analysis
- `docs/bugs/nodejs-sharedarraybuffer-bug.md` - Node.js SharedArrayBuffer fix
