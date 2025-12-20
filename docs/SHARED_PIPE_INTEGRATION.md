# SharedPipe Integration Plan

## Overview

This document describes how to integrate `SharedPipe` with the task spawning architecture to fix the WASM subprocess IPC bug.

## Current State

- `SharedPipe` is implemented in `src/pipes/mod.rs`
- Uses SharedArrayBuffer + Atomics for cross-Worker communication
- Implements `VirtualFile`, `AsyncRead`, `AsyncWrite` traits
- Compiles and is ready for integration

## Problem Summary

When a parent WASM process spawns a child WASM process:
1. Parent creates `WasiEnv` via `fork()`
2. `WasiEnv` contains pipes using `tokio::sync::mpsc` channels
3. `WasiEnv` is sent to child Worker via `SchedulerMessage::SpawnWithModuleAndMemory`
4. The tokio channels become disconnected copies
5. Child writes to dead channel, parent never receives

## Integration Approach

### Option A: Replace Pipes in SpawnWasm (Recommended)

Add SharedArrayBuffer references to the spawn message and replace stdio on the child side.

**Files to modify:**

1. **`src/tasks/post_message_payload.rs`**
   ```rust
   BlockingJob::SpawnWithModuleAndMemory {
       module: WebAssembly::Module,
       memory: Option<WebAssembly::Memory>,
       spawn_wasm: SpawnWasm,
       // NEW: Optional SharedArrayBuffers for subprocess stdio
       subprocess_stdio: Option<SubprocessStdioBuffers>,
   }

   struct SubprocessStdioBuffers {
       stdin: SharedArrayBuffer,
       stdout: SharedArrayBuffer,
       stderr: SharedArrayBuffer,
   }
   ```

2. **`src/tasks/scheduler_message.rs`**
   - Add same `subprocess_stdio` field to `SchedulerMessage::SpawnWithModuleAndMemory`

3. **`src/tasks/task_wasm.rs`**
   - In `to_scheduler_message()`, detect subprocess spawns
   - Create `SharedStdioPipes` for subprocess
   - Store parent-side handles for later reading
   - Add buffers to the message

4. **`src/tasks/thread_pool_worker.rs`**
   - In `SpawnWithModuleAndMemory` handler, if `subprocess_stdio` is present:
   - Create `SharedPipe` from buffers
   - Replace stdio fds (0, 1, 2) in WasiEnv with SharedPipes

5. **`src/tasks/scheduler.rs`**
   - Track subprocess relationships
   - Allow parent to read from child's stdout/stderr SharedPipes

### Option B: Message-Based I/O (Simpler fallback)

Route subprocess I/O through the scheduler like host_exec.

1. Child sends `WasmSubprocessOutput { child_id, data }` to scheduler
2. Scheduler queues data for parent
3. Parent reads via `WasmSubprocessRead { child_id }` messages

This is simpler but adds latency for every I/O operation.

## Implementation Steps

### Phase 1: Detect Subprocess Spawns

Add a way to detect when `task_wasm()` is called for a subprocess vs initial program.

```rust
// In task_wasm.rs
pub(crate) fn to_scheduler_message(task: TaskWasm<'_>) -> Result<SchedulerMessage, WasiThreadError> {
    let is_subprocess = task.env.process.pid().raw() != 0;
    // or check if WasiEnv was forked
    ...
}
```

### Phase 2: Create SharedPipes for Subprocess

When spawning a subprocess:
1. Create `SharedStdioPipes::new()`
2. Get `child_buffers()` for child side
3. Get `parent_handles()` for parent side

```rust
if is_subprocess {
    let stdio_pipes = SharedStdioPipes::new();
    let (stdin_buf, stdout_buf, stderr_buf) = stdio_pipes.child_buffers();
    let (stdin_tx, stdout_rx, stderr_rx) = stdio_pipes.parent_handles();

    // Store parent handles somewhere accessible
    SUBPROCESS_HANDLES.insert(child_pid, (stdin_tx, stdout_rx, stderr_rx));
}
```

### Phase 3: Pass Buffers Through Spawn Message

Serialize `SharedArrayBuffer` refs in the spawn message:

```rust
SchedulerMessage::SpawnWithModuleAndMemory {
    module,
    memory,
    spawn_wasm,
    subprocess_stdio: Some(SubprocessStdioBuffers {
        stdin: stdin_buf,
        stdout: stdout_buf,
        stderr: stderr_buf,
    }),
}
```

### Phase 4: Replace Stdio on Child Side

In `build_ctx_and_store()` or `execute()`:

```rust
if let Some(stdio_buffers) = subprocess_stdio {
    let pipes = SharedStdioPipes::child_from_buffers(
        stdio_buffers.stdin,
        stdio_buffers.stdout,
        stdio_buffers.stderr,
    );

    // Replace fd 0, 1, 2 in WasiEnv with SharedPipes
    env.replace_stdio(pipes);
}
```

### Phase 5: Parent Reads Child Output

Parent uses stored handles to read:

```rust
let (stdin_tx, stdout_rx, stderr_rx) = SUBPROCESS_HANDLES.get(&child_pid)?;
let mut buf = [0u8; 4096];
let n = stdout_rx.try_read(&mut buf);
```

## Key Challenges

1. **Replacing WasiEnv stdio**: Need a way to swap file descriptors 0,1,2
   - May need to add a method to WasiEnv or WasiFs
   - Or replace at Kind/Inode level

2. **Detecting subprocess spawns**: Need to differentiate from initial program
   - Check process ID or parent reference

3. **Storing parent handles**: Need thread-safe storage accessible from spawn site
   - Could use scheduler's thread-local storage

4. **Cleanup on child exit**: Need to close pipes and remove handles
   - Add cleanup in child process exit handling

## Testing

1. Create test that spawns WASM subprocess
2. Verify parent receives child's stdout/stderr
3. Test stdin write from parent to child
4. Test subprocess exit code handling

## Future Optimizations

1. Pool SharedArrayBuffers for reuse
2. Dynamic buffer sizing based on usage
3. Zero-copy path for large data transfers
