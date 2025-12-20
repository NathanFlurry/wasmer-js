# WASM Subprocess IPC Bug

## Summary

WASM-to-WASM subprocess spawning fails in wasmer-js because `virtual_fs::Pipe` uses `tokio::sync::mpsc` channels which cannot work across Web Workers.

## Symptoms

- `Command::new("echo").spawn()` from within WASM hangs indefinitely
- Child process runs and exits successfully (exit code 0 visible in logs)
- Parent never receives child's stdout/stderr
- Native wasmer CLI works perfectly for the same code

## Root Cause

### The Problem

Web Workers have **isolated memory spaces**. When a subprocess is spawned:

1. Parent WASM runs in Worker A
2. Child WASM is dispatched to Worker B via `task_wasm()`
3. Child's `WasiEnv` (including Pipes) is serialized and sent to Worker B
4. The `tokio::sync::mpsc` channels in the Pipe become **disconnected copies**
5. Child writes to stdout → goes to dead channel → parent never receives

### Code Location

`lib/virtual-fs/src/pipe.rs`:
```rust
pub struct Pipe {
    send: PipeTx,
    recv: PipeRx,
}

pub struct PipeTx {
    tx: Option<mpsc::UnboundedSender<Vec<u8>>>,  // ← tokio channel - can't cross Workers
    rx_end: Weak<Mutex<PipeReceiver>>,
}

pub struct PipeRx {
    rx: Option<Arc<Mutex<PipeReceiver>>>,  // ← Contains mpsc::UnboundedReceiver
}
```

### Why Native Wasmer Works

- Native wasmer uses real OS threads
- Threads share the same memory space
- `tokio::sync::mpsc` channels use shared memory pointers
- Parent and child can communicate via inherited pipe file descriptors

### Why Wasmer-JS Fails

- Wasmer-js uses Web Workers for parallelism
- Web Workers have isolated memory (no shared heap)
- When `task_wasm()` sends work to another Worker, the Pipe is copied
- The copied `mpsc::UnboundedSender` points to memory in the wrong Worker
- Writes succeed locally but never reach the original receiver

## Spawn Flow Analysis

```
run_wasix()
  → spawn_with_module()                    [src/run.rs:55]
    → ThreadPool sends to Worker A
      → Worker A runs main WASM (e.g., wasix-runtime)
        → WASM calls Command::new("echo").spawn()
          → posix_spawnp()                 [wasix-libc]
            → proc_spawn2 syscall          [lib/wasix/src/syscalls/wasix/proc_spawn2.rs]
              → find_executable_in_path()  [lib/wasix/src/syscalls/wasix/proc_exec3.rs:307]
              → bin_factory.spawn()        [lib/wasix/src/bin_factory/mod.rs:71]
                → spawn_exec_module()      [lib/wasix/src/bin_factory/exec.rs:122]
                  → task_wasm()            [src/tasks/thread_pool.rs:114]
                    → SchedulerMessage::SpawnWithModuleAndMemory
                      → Scheduler dispatches to Worker B  [src/tasks/scheduler.rs:244]
                        → Worker B runs child WASM
                          → Child writes to stdout
                            → Goes to disconnected Pipe copy
                              → Parent in Worker A never receives
```

## Key Files

| File | Description |
|------|-------------|
| `lib/virtual-fs/src/pipe.rs` | Pipe implementation using tokio channels |
| `lib/wasix/src/syscalls/wasix/proc_spawn2.rs` | proc_spawn2 syscall implementation |
| `lib/wasix/src/bin_factory/exec.rs` | spawn_exec_module() - creates TaskWasm |
| `src/tasks/thread_pool.rs` | ThreadPool::task_wasm() - sends to scheduler |
| `src/tasks/scheduler.rs` | Scheduler dispatches to Workers |
| `src/tasks/task_wasm.rs` | SpawnWasm - child WASM execution |
| `src/tasks/worker_handle.rs` | WorkerHandle - Web Worker management |

## Potential Fixes

### Option 1: Run Parent and Child in Same Worker

**Approach**: Instead of dispatching child WASM to a different Worker, run it in the same Worker as the parent. This keeps tokio channels working since they share memory.

**Implementation**:
- Modify `task_wasm()` to detect subprocess spawns
- Execute child synchronously in the same Worker context
- Parent blocks while child runs

**Pros**:
- Minimal code changes
- Tokio channels work as-is
- No new IPC mechanism needed

**Cons**:
- **Breaks parallelism completely** - parent cannot do anything while child runs
- Defeats the purpose of Web Workers
- Would cause deadlocks if child waits for parent input
- Subprocess model becomes fundamentally different from native wasmer

**Verdict**: Not recommended. The loss of parallelism is unacceptable.

---

### Option 2: SharedArrayBuffer-Based Pipes (Recommended)

**Approach**: Replace `tokio::sync::mpsc` channels with SharedArrayBuffer + Atomics for pipe communication. The SharedArrayBuffer is shared memory that works across Workers.

**Implementation**:
- Create new `SharedPipe` type using SharedArrayBuffer ring buffer
- Use `Atomics.wait()` / `Atomics.notify()` for synchronization
- Pass SharedArrayBuffer references when spawning child Worker
- Child and parent both access the same underlying memory

**Pros**:
- Full parallelism preserved
- True shared memory - no copying overhead
- Proper blocking semantics via Atomics
- Clean abstraction matching existing Pipe API

**Cons**:
- Requires changes to wasmer core (`lib/virtual-fs`) or wasmer-js wrapper
- More complex implementation (ring buffer, synchronization)
- SharedArrayBuffer requires specific security headers in browsers

**Verdict**: Recommended. This is the proper architectural fix that maintains parallelism and performance.

---

### Option 3: Route All Subprocess I/O Through Scheduler

**Approach**: Child writes to a local buffer, sends data via `postMessage` to the scheduler (main thread), which forwards to the parent Worker. Similar to how `host_exec` already works.

**Implementation**:
- Register parent's pipe handlers with scheduler before spawn
- Child sends `SchedulerMessage::ChildOutput { child_id, data }` on write
- Scheduler forwards via `postMessage` to parent Worker
- Parent receives data through callback, writes to local pipe

**Pros**:
- Uses existing scheduler infrastructure
- No changes to wasmer core needed
- Works without SharedArrayBuffer (browser compatibility)

**Cons**:
- **All data goes through main thread** - bottleneck for high-throughput
- Added latency for every write (Worker → Main → Worker)
- More complex data flow
- Main thread can become overwhelmed with I/O

**Verdict**: Acceptable fallback if SharedArrayBuffer is unavailable, but not ideal for performance.

---

### Comparison Summary

| Aspect | Option 1: Same Worker | Option 2: SharedArrayBuffer | Option 3: Via Scheduler |
|--------|----------------------|----------------------------|------------------------|
| Parallelism | ❌ None | ✅ Full | ✅ Full |
| Performance | ✅ Fast (no IPC) | ✅ Fast (shared memory) | ⚠️ Slower (postMessage) |
| Complexity | ✅ Simple | ⚠️ Moderate | ⚠️ Moderate |
| Core changes | ✅ None | ⚠️ virtual-fs or wrapper | ✅ wasmer-js only |
| Correctness | ❌ Deadlock risk | ✅ Correct | ✅ Correct |

**Decision**: Proceed with Option 2 (SharedArrayBuffer-based pipes) as it provides the best balance of correctness, performance, and parallelism.

---

## Existing Cross-Worker IPC

Wasmer-js already has infrastructure for cross-Worker communication:

### SharedArrayBuffer + Atomics (used by host_exec)

```rust
// src/tasks/worker_handle.rs
pub(crate) fn host_exec_buffer(&self) -> &SharedArrayBuffer {
    &self.host_exec_buffer
}

// src/runtime.rs - blocking read using Atomics.wait()
Atomics::wait(view, 0, 0)  // Blocks until notified
Atomics::notify(&int32_view, 0)  // Wakes blocked worker
```

### Child Output Handlers (used by host_exec)

```rust
// src/tasks/scheduler.rs
thread_local! {
    static CHILD_OUTPUT_HANDLERS: RefCell<HashMap<u64, (Function, Function, Function)>>
        = RefCell::new(HashMap::new());
}

// Handles HostExecChildOutput messages
SchedulerMessage::HostExecChildOutput { child_id, msg_type, data } => {
    CHILD_OUTPUT_HANDLERS.with(|handlers| {
        if let Some((stdout_fn, stderr_fn, exit_fn)) = handlers.borrow().get(&child_id) {
            match msg_type {
                MSG_TYPE_CHILD_STDOUT => stdout_fn.call1(&JsValue::NULL, &data_array),
                MSG_TYPE_CHILD_STDERR => stderr_fn.call1(&JsValue::NULL, &data_array),
                MSG_TYPE_CHILD_EXIT => exit_fn.call1(&JsValue::NULL, &exit_code_val),
            }
        }
    });
}
```

## Proposed Fix: SharedArrayBuffer-Based Pipes

### Overview

Replace `tokio::sync::mpsc` with SharedArrayBuffer + Atomics for cross-Worker pipe communication.

### Design

```
┌─────────────────┐     SharedArrayBuffer      ┌─────────────────┐
│   Worker A      │◄──────────────────────────►│   Worker B      │
│   (Parent)      │                            │   (Child)       │
│                 │    ┌──────────────────┐    │                 │
│  PipeRx ────────┼───►│  Ring Buffer     │◄───┼──── PipeTx      │
│                 │    │  + Atomics       │    │                 │
│                 │    └──────────────────┘    │                 │
└─────────────────┘                            └─────────────────┘
```

### SharedArrayBuffer Layout

```
Offset  Size   Field
──────  ────   ─────
0       4      write_pos (Atomics)
4       4      read_pos (Atomics)
8       4      closed flag (Atomics)
12      4      data_available (for Atomics.wait/notify)
16      N      ring buffer data
```

### Implementation Steps

1. **Create `SharedPipe` struct** in wasmer-js
   - Uses SharedArrayBuffer for data
   - Uses Atomics for synchronization
   - Implements same API as virtual_fs::Pipe

2. **Modify `task_wasm()` spawn path**
   - Detect when spawning subprocess (vs initial program)
   - Create SharedArrayBuffer for each stdio pipe
   - Pass SharedArrayBuffer references to child Worker

3. **Wrap child's stdio with SharedPipe**
   - In `build_ctx_and_store()`, intercept WasiEnv creation
   - Replace tokio-based Pipes with SharedPipe wrappers

4. **Implement blocking read/write**
   - Write: Copy to ring buffer, Atomics.notify()
   - Read: Atomics.wait() until data available, copy from ring buffer

### Files to Modify

| File | Changes |
|------|---------|
| `src/pipes/mod.rs` (new) | SharedPipe implementation |
| `src/tasks/task_wasm.rs` | Pass SharedArrayBuffer to child |
| `src/tasks/thread_pool.rs` | Create SharedArrayBuffers on spawn |
| `src/tasks/scheduler.rs` | Track SharedArrayBuffers per subprocess |

### Considerations

1. **Memory Management**: SharedArrayBuffers must be sized appropriately and cleaned up when subprocess exits

2. **Backpressure**: If child writes faster than parent reads, need ring buffer full handling

3. **EOF Signaling**: Need explicit closed flag since channel disconnect doesn't work

4. **Multiple Readers/Writers**: Subprocess stdio is 1:1, so simpler than general-purpose channels

## Testing

### Test Case

```rust
// In WASM
use std::process::Command;

fn main() {
    let output = Command::new("echo")
        .arg("hello")
        .output()
        .expect("failed to execute");

    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
}
```

### Expected Behavior

- Child process runs in separate Worker
- stdout "hello\n" is captured via SharedArrayBuffer
- Parent receives output without hanging

### Current Behavior

- Child process runs and exits with code 0
- stdout goes to disconnected tokio channel
- Parent hangs indefinitely waiting for output

## Related Issues

- **ThreadPool lifecycle fix** (d822560): Ensured Workers stay alive long enough; unmasked this deeper IPC issue
- **setTimeout Node.js fix**: Fixed `setTimeout` returning object instead of number in Node.js Workers

## References

- [SharedArrayBuffer MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer)
- [Atomics MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Atomics)
- [WASIX Subprocess Spawning](https://wasix.org/docs/api-reference/wasix/proc_spawn2)
