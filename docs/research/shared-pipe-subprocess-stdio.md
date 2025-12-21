# SharedPipe: Cross-Worker Subprocess Stdio Fix

## Problem Statement

When WASM process A spawns WASM process B via `proc_spawn2`, subprocess stdio communication fails because:

1. **Isolated Memory Spaces**: Each Web Worker has its own JavaScript heap
2. **Tokio Channel Failure**: `virtual_fs::Pipe` uses `tokio::sync::mpsc` channels which rely on shared heap memory (Arc, pointers)
3. **Serialization Breaks Pipes**: When the child's WasiEnv is sent to another Worker, the pipe's internal channel state becomes invalid

```
Worker A (Parent)              Worker B (Child)
┌─────────────────┐           ┌─────────────────┐
│ tokio::mpsc::tx │──────X────│ tokio::mpsc::rx │
│  (heap ptr A)   │  broken   │  (heap ptr B)   │
└─────────────────┘           └─────────────────┘
     Different heaps - pointers are invalid!
```

## Solution: SharedArrayBuffer-Based Pipes

SharedArrayBuffer is explicitly designed for cross-Worker memory sharing. We replace tokio pipes with a custom ring buffer implementation using SharedArrayBuffer + Atomics.

```
Worker A (Parent)              Worker B (Child)
┌─────────────────┐           ┌─────────────────┐
│ SharedPipeRx    │           │ SharedPipeTx    │
│  (read end)     │◄─────────►│  (write end)    │
└────────┬────────┘           └────────┬────────┘
         │                             │
         │    ┌───────────────────┐    │
         └────┤ SharedArrayBuffer ├────┘
              │  (shared memory)  │
              └───────────────────┘
                 Ring buffer with
                 Atomics sync
```

## Architecture

### Ring Buffer Layout

```
Offset  Size   Field
──────  ────   ─────
0       4      write_pos (Atomics) - next write position
4       4      read_pos (Atomics) - next read position
8       4      closed flag (Atomics) - 0=open, 1=closed
12      4      notify flag (Atomics.wait/notify)
16      N      ring buffer data
```

### Key Components

#### 1. SharedPipe (`wasmer-js/src/pipes/mod.rs`)

```rust
pub struct SharedPipe {
    tx: SharedPipeTx,  // Write end
    rx: SharedPipeRx,  // Read end
}

pub struct SharedPipeTx {
    buffer: SharedArrayBuffer,
    data_capacity: usize,
}

pub struct SharedPipeRx {
    buffer: SharedArrayBuffer,
    data_capacity: usize,
}
```

Both ends reference the same SharedArrayBuffer. The buffer can be cloned and sent to another Worker - they'll still share the same underlying memory.

#### 2. VirtualFile Implementations

SharedPipe, SharedPipeTx, and SharedPipeRx all implement `virtual_fs::VirtualFile`:
- `SharedPipeTx`: Write-only (AsyncRead returns error)
- `SharedPipeRx`: Read-only (AsyncWrite returns error)
- `SharedPipe`: Full duplex

#### 3. Subprocess Detection (`wasmer-js/src/tasks/task_wasm.rs`)

```rust
// In to_scheduler_message():
let is_subprocess = env.pid().raw() > 1;
let subprocess_stdio = if is_subprocess {
    let stdio_pipes = SharedStdioPipes::new();
    let (stdin_buf, stdout_buf, stderr_buf) = stdio_pipes.child_buffers();
    Some(SubprocessStdioBuffers { stdin, stdout, stderr })
} else {
    None
};
```

PID 1 is the main process; subprocesses get PID > 1.

#### 4. WasiEnv::replace_stdio (`wasmer/lib/wasix/src/state/env.rs`)

New public method to replace stdio FDs with custom VirtualFile handles:

```rust
pub fn replace_stdio(
    &self,
    stdin: Option<Box<dyn VirtualFile + Send + Sync + 'static>>,
    stdout: Option<Box<dyn VirtualFile + Send + Sync + 'static>>,
    stderr: Option<Box<dyn VirtualFile + Send + Sync + 'static>>,
) -> Result<(), FsError>
```

#### 5. Worker-Side Injection (`wasmer-js/src/tasks/task_wasm.rs`)

```rust
// In SpawnWasm::inject_subprocess_stdio():
let stdin_pipe = SharedPipe::from_buffer(buffers.stdin.clone());
let stdout_pipe = SharedPipe::from_buffer(buffers.stdout.clone());
let stderr_pipe = SharedPipe::from_buffer(buffers.stderr.clone());

let (_, stdin_rx) = stdin_pipe.split();   // Child reads
let (stdout_tx, _) = stdout_pipe.split(); // Child writes
let (stderr_tx, _) = stderr_pipe.split(); // Child writes

self.env.replace_stdio(
    Some(Box::new(stdin_rx)),
    Some(Box::new(stdout_tx)),
    Some(Box::new(stderr_tx)),
)
```

#### 6. Scheduler Polling (`wasmer-js/src/tasks/scheduler.rs`)

The scheduler stores parent-side pipes and polls them:

```rust
// When subprocess spawns:
SUBPROCESS_OUTPUT_PIPES.insert(subprocess_id, (stdout_pipe, stderr_pipe));
spawn_local(poll_subprocess_output(subprocess_id));

// Polling loop:
async fn poll_subprocess_output(subprocess_id: u64) {
    loop {
        // Read from stdout/stderr pipes
        // Log to console
        // Sleep 50ms
    }
}
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         Main Thread                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      Scheduler                           │    │
│  │  1. Receives SpawnWithModuleAndMemory                    │    │
│  │  2. Creates SharedPipes from buffers                     │    │
│  │  3. Stores parent-side pipes for polling                 │    │
│  │  4. Forwards buffers to child Worker                     │    │
│  │  5. Polls stdout/stderr, logs to console                 │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┴────────────────────┐
         ▼                                         ▼
┌─────────────────────┐               ┌─────────────────────┐
│   Worker A (Parent) │               │   Worker B (Child)  │
│                     │               │                     │
│ proc_spawn2() ──────┼───────────────┼──► Receives buffers │
│                     │               │    inject_stdio()   │
│                     │               │                     │
│                     │    stdout     │ Child writes to     │
│                     │◄══════════════│ SharedPipeTx (FD 1) │
│                     │  SharedArray  │                     │
│                     │    Buffer     │ Child writes to     │
│                     │◄══════════════│ SharedPipeTx (FD 2) │
│                     │    stderr     │                     │
└─────────────────────┘               └─────────────────────┘
```

## Message Types

### SubprocessStdioBuffers

```rust
pub struct SubprocessStdioBuffers {
    pub stdin: SharedArrayBuffer,   // Parent writes, child reads
    pub stdout: SharedArrayBuffer,  // Child writes, parent reads
    pub stderr: SharedArrayBuffer,  // Child writes, parent reads
}
```

### BlockingJob::SpawnWithModuleAndMemory

```rust
SpawnWithModuleAndMemory {
    module: WebAssembly::Module,
    memory: Option<JsValue>,
    spawn_wasm: SpawnWasm,
    subprocess_stdio: Option<SubprocessStdioBuffers>,
}
```

## Synchronization

### Ring Buffer Operations

**Write (SharedPipeTx):**
1. Load read_pos with Atomics
2. Calculate free space
3. Copy data to buffer at write_pos
4. Store new write_pos with Atomics
5. Notify waiting readers

**Read (SharedPipeRx):**
1. Load write_pos with Atomics
2. Calculate available data
3. Copy data from buffer at read_pos
4. Store new read_pos with Atomics

### Thread Safety

```rust
// Safety: SharedArrayBuffer is designed for cross-Worker sharing
// Each Worker creates its own typed array views
// All shared access uses Atomics operations
unsafe impl Send for SharedPipe {}
unsafe impl Sync for SharedPipe {}
```

## Limitations and Future Work

### Current Limitations

The current implementation only fixes the **child side** of the pipe. The parent process still has broken tokio pipe handles:

```
Worker A (Parent)              Main Thread (Scheduler)       Worker B (Child)
┌─────────────────┐           ┌─────────────────┐           ┌─────────────────┐
│ tokio::mpsc::rx │──X──      │ SharedPipeRx    │◄──────────│ SharedPipeTx    │
│  (broken!)      │   │       │ (polls output)  │           │  (child writes) │
└─────────────────┘   │       └────────┬────────┘           └─────────────────┘
                      │                │
                      │                ▼
                      │         Console.log()   <-- output goes here, not to parent
                      │
                      └── Parent can't read child output!
```

Specific limitations:

1. **Output Goes to Console, Not Parent**: The scheduler polls subprocess output and logs to console. The parent process (Worker A) has no way to read the child's output because its original `tokio::mpsc::rx` handles are broken/orphaned. The SharedPipe data goes to the main thread, not back to the parent Worker.

2. **Stdin Not Usable**: While we create a stdin SharedPipe and inject the Rx end into the child, the parent has no way to write to it. The parent's `tokio::mpsc::tx` handle is broken. We'd need to give the parent a SharedPipeTx end instead.

3. **No Cleanup Signal**: Subprocess exit doesn't explicitly signal pipe closure (set the closed flag in the ring buffer).

### Why This Is Hard

The fundamental issue: **we need to fix both ends of the pipe, not just one**.

When `fd_pipe()` creates a pipe, it creates TWO separate inodes:
- `inode_tx` with `Kind::PipeTx { tx }` - the write end
- `inode_rx` with `Kind::PipeRx { rx }` - the read end

These are connected internally via tokio channels. For stdout:
- Child's FD 1 → `inode_tx` (child writes here)
- Parent's read_fd → `inode_rx` (parent reads here)

Currently:
- Child side: ✅ Gets SharedPipe ends via `inject_subprocess_stdio()` - replaces FD 0/1/2
- Parent side: ❌ Still has `inode_rx` with broken tokio handles

The problem: **we can only see `inode_tx` from the child's FD table, but `inode_rx` is in the parent's FD table**. These are different inodes, and we don't have access to the parent's FD table in `to_scheduler_message`.

### Solutions for Parent-Side

We added `Kind::VirtualPipeTx` and `Kind::VirtualPipeRx` variants to wasmer that can hold any `Box<dyn VirtualFile>`. This enables using SharedPipe ends in place of tokio pipes.

**Option 1: Hook fd_pipe** (Recommended)

Override `fd_pipe` in wasmer-js to create SharedPipe-based pipes from the start:
```rust
// In wasmer-js fd_pipe override:
let shared_pipe = SharedPipe::new();
let (tx, rx) = shared_pipe.split();
// Create inodes with Kind::VirtualPipeTx and Kind::VirtualPipeRx
```

This way, both parent and child automatically use SharedPipes.

**Option 2: Find both inodes at spawn time**

Store a reverse mapping from `PipeTx.rx_end` back to its inode, or add bidirectional links between pipe inodes. Then in `to_scheduler_message`, find and replace both inodes.

**Option 3: Add parent context to spawn**

Modify the spawn path to pass the parent's WasiEnv through to `to_scheduler_message`, allowing us to modify the parent's FD table directly.

### Future Improvements

1. **Implement Option 1 (Hook fd_pipe)**: Override fd_pipe syscall to create SharedPipe-based pipes, eliminating the need for post-spawn injection.

2. **Exit Notification**: Set the ring buffer `closed` flag when subprocess exits.

3. **Buffer Backpressure**: Handle full buffer conditions more gracefully (currently may spin or lose data).

4. **Remove console.log polling**: Once parent-side works, remove scheduler polling and let parent read directly.

## Files Modified

### wasmer-js

| File | Changes |
|------|---------|
| `src/pipes/mod.rs` | New SharedPipe implementation (~750 lines) |
| `src/lib.rs` | Export pipes module |
| `src/tasks/task_wasm.rs` | Subprocess detection, inject_subprocess_stdio |
| `src/tasks/thread_pool_worker.rs` | Call inject_subprocess_stdio |
| `src/tasks/scheduler.rs` | Subprocess output polling |
| `src/tasks/scheduler_message.rs` | subprocess_stdio field |
| `src/tasks/post_message_payload.rs` | SubprocessStdioBuffers, serialization |

### wasmer

| File | Changes |
|------|---------|
| `lib/wasix/src/state/env.rs` | WasiEnv::replace_stdio method |
| `lib/wasix/src/fs/fd.rs` | Kind::VirtualPipeTx and Kind::VirtualPipeRx variants |
| `lib/wasix/src/fs/inode_guard.rs` | Handle VirtualPipe in poll guards |
| `lib/wasix/src/fs/mod.rs` | VirtualPipe in directory traversal error |
| `lib/wasix/src/syscalls/wasi/fd_read.rs` | Read from VirtualPipeRx |
| `lib/wasix/src/syscalls/wasi/fd_write.rs` | Write to VirtualPipeTx |
| `lib/wasix/src/syscalls/wasi/*.rs` | Various match patterns updated |
| `lib/wasix/src/syscalls/wasix/*.rs` | Various match patterns updated |

## Testing

To test subprocess stdio:

```javascript
// In browser with wasmer-js
const instance = await Wasmer.spawn("bash", {
    args: ["-c", "echo hello && echo world >&2"]
});
// Should see in console:
// [subprocess 1] stdout: hello
// [subprocess 1] stderr: world
```

## References

- [SharedArrayBuffer MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer)
- [Atomics MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Atomics)
- [virtual_fs::Pipe source](https://github.com/wasmerio/wasmer/blob/main/lib/virtual-fs/src/pipe.rs)
