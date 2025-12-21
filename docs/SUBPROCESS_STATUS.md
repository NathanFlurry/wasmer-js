# Wasmer-JS Subprocess Status

## Summary

This document tracks all known subprocess-related issues in wasmer-js and their status.

## Issues Overview

| Issue | Status | Description |
|-------|--------|-------------|
| Scheduler Race Condition | **FIXED** | Sequential tests hang after 2-3 runs |
| Cross-Worker IPC | **FIXED** | Subprocess stdout/stderr not reaching parent |
| GlobalScope::sleep() in Node.js | **FIXED** | setTimeout fails in Node.js workers |
| proc_fork BorrowMutError | **UNRESOLVED** | Bash pipes/substitution panic with RefCell error |
| $() Command Substitution | **UNRESOLVED** | `echo $(cmd)` hangs, but backticks work |
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

## Unresolved Issues

### 4. proc_fork BorrowMutError

**Problem**: When bash attempts to spawn subprocesses (for pipes or command substitution), the WASIX runtime panics with `BorrowMutError` in `thread_local.rs`.

**Error**:
```
panicked at lib/wasix/src/state/handles/thread_local.rs:106:75:
already borrowed: BorrowMutError

...
at wasmer_wasix::syscalls::wasix::proc_fork::run
at wasmer_wasix::syscalls::wasix::proc_fork::proc_fork
```

**Root Cause**: The `WasiInstanceHandlesPointer` uses `RefCell` for interior mutability. During `proc_fork`, the code attempts to borrow the RefCell mutably while it's already borrowed (likely by signal handling or another concurrent operation).

**Affected Operations**:
- `echo test | cat` (pipes)
- `` echo `echo works` `` (backticks)
- `echo $(echo works)` (command substitution)

**Status**: Requires investigation in wasmer-wasix `lib/wasix/src/state/handles/thread_local.rs` and the `proc_fork` implementation.

**Workaround**: Simple `echo hello` commands work. Avoid pipes and command substitution.

---

### 5. $() Command Substitution Hangs

**Problem**: `$(command)` syntax hangs indefinitely, while backticks `` `command` `` work correctly.

**Observations**:
- Backticks spawn 4 workers and complete successfully
- `$()` only spawns 3 workers and hangs
- The subprocess worker is never spawned with `$()`

**Hypothesis**: WASIX bash implements `$()` differently than backticks:
- Backticks use `posix_spawn()` which works
- `$()` may use `fork()` which has issues in WASIX

**Status**: Needs investigation in wasix-libc or bash WASIX port.

**Reproduction**:
```javascript
// Works:
await pkg.commands["bash"].run({ args: ["-c", "echo `echo works`"] });

// Hangs:
await pkg.commands["bash"].run({ args: ["-c", "echo $(echo works)"] });
```

---

### 5. Bash Exit Code 45

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
| Bash pipe | FAIL | BorrowMutError in proc_fork |
| Backticks | FAIL | BorrowMutError in proc_fork |
| $() substitution | FAIL | Timeout (known issue) |

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
```

### Failing Test Cases

```javascript
// Bash pipe - FAILS with BorrowMutError
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo test | cat"],
  stdin: ''
});

// Backticks - FAILS with BorrowMutError
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo `echo works`"],
  stdin: ''
});

// $() substitution - HANGS
const instance = await pkg.commands["bash"].run({
  args: ["-c", "echo $(echo works)"],
  stdin: ''
});
```

---

## Related Documentation

- `docs/research/wasmer-js-scheduler-flakiness.md` - Detailed scheduler race condition analysis
- `docs/research/shared-pipe-subprocess-stdio.md` - SharedPipe implementation details
- `docs/bugs/WASM_SUBPROCESS_IPC.md` - Original subprocess IPC bug analysis
- `docs/bugs/nodejs-sharedarraybuffer-bug.md` - Node.js SharedArrayBuffer fix
