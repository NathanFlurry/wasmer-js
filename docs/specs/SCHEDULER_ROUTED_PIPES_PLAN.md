# Scheduler-Routed Pipes Implementation Plan

## Problem Statement

WASM subprocess spawning in wasmer-js fails because `tokio::sync::mpsc` channels cannot work across Web Workers. When a parent WASM process spawns a child:

1. Parent creates pipes using `tokio::sync::mpsc` channels
2. Child WASM is dispatched to a different Worker via `task_wasm()`
3. The tokio channels become **disconnected copies** (Web Workers have isolated memory)
4. Child writes to dead channel, parent never receives output

## Previous Approach: SharedArrayBuffer Ring Buffers

We attempted to fix this with SharedArrayBuffer-based pipes (`SharedPipe`):

- Ring buffer in SharedArrayBuffer with read_pos/write_pos
- Atomics.wait/notify for cross-worker synchronization
- TX reference counting to handle fork() correctly
- Pipe pool allocated on main thread, shared with all workers

### Why SharedArrayBuffer Approach Failed

After extensive implementation and debugging, the SharedArrayBuffer approach proved too complex:

1. **Ring buffer complexity**: Managing read_pos, write_pos, capacity, wrap-around required careful atomics coordination

2. **Fork semantics broke TX reference counting**: When bash forks, the parent drops its copy of pipe FDs. With reference counting, this triggered close() prematurely. We added TX_COUNT but the timing was fragile.

3. **Debugging was nearly impossible**: Worker threads don't have console.log output during WASM syscall execution. We resorted to using `panic!()` to verify code paths were reached.

4. **Multiple failure modes discovered**:
   - Pipe closed before data written (fixed with ref counting)
   - RX drop closing shared flag (fixed by making RX drop a no-op)
   - Missing TX end after fork/dup2 (partially fixed)
   - Tests work individually but fail when run sequentially (state leakage suspected)

5. **Pool offset transfer issues**: SharedArrayBuffer references become null when transferred via postMessage in some contexts. We switched to pool offsets but this added more complexity.

6. **~1000 lines of intricate code**: The implementation in `src/pipes/mod.rs`, `pool.rs`, `extract.rs` became difficult to reason about.

### Test Results with SharedArrayBuffer Approach

| Test | Result |
|------|--------|
| `echo hello` | PASS |
| `echo test \| cat` | PASS (after TX ref counting fix) |
| `echo \`echo bt\`` | TIMEOUT |
| `echo $(echo sub)` | TIMEOUT |
| `echo abc \| cat \| cat` | TIMEOUT |
| `ls /` | PASS individually, TIMEOUT in sequence |

The approach was fundamentally working for simple cases but had subtle bugs causing hangs in complex scenarios.

## New Approach: Scheduler-Routed Pipes

Route all pipe I/O through the main scheduler thread via `postMessage`. This is the same pattern used by `host_exec` which already works reliably.

### Why This Approach Is Better

1. **Proven pattern**: The `host_exec` infrastructure already routes data through the scheduler successfully. We reuse the same Atomics.wait/notify pattern for blocking reads.

2. **No shared mutable state**: Pipe buffers live in the scheduler's thread-local storage. Workers only hold pipe IDs (integers). Fork can't corrupt anything.

3. **Debuggable**: The scheduler runs on the main thread where console.log works. We can trace every write and read.

4. **Simple code**: ~200 lines instead of ~1000 lines. No ring buffer, no ref counting, no pool offsets.

5. **No fork issues**: Passing a `pipe_id: u64` to a child worker is trivial. The child creates a handle pointing to the same scheduler-managed buffer.

### Trade-offs

| Aspect | SharedArrayBuffer | Scheduler-Routed |
|--------|------------------|------------------|
| Latency | ~0ms (direct memory) | ~1-2ms (postMessage round-trip) |
| Throughput | High (no copying) | Lower (data copied through main thread) |
| Complexity | High (~1000 lines) | Low (~200 lines) |
| Debugging | Very hard | Easy |
| Correctness | Fragile | Robust |
| Fork handling | Complex ref counting | Trivial (just pass ID) |

For shell commands and typical subprocess I/O, the latency is acceptable. If high-throughput pipes become necessary later, we can optimize specific cases.

## Starting Point

Checkout commit `d822560` (pre-SharedPipe) and cherry-pick essential fixes:

```bash
git checkout -b scheduler-routed-pipes d822560
git cherry-pick 9d7b627  # Node.js SharedArrayBuffer support (resolve .cargo/config.toml conflict)
git cherry-pick 83a88ce  # Scheduler race condition fix
```

Commits to cherry-pick:
- `9d7b627`: Adds shared memory linker flags for Node.js Atomics support
- `83a88ce`: Fixes scheduler race condition where Close was processed before SpawnBlocking

## Architecture

```
Worker A (echo)              SCHEDULER (main)              Worker B (cat)
━━━━━━━━━━━━━━━              ━━━━━━━━━━━━━━━━              ━━━━━━━━━━━━━━

write(stdout, "test\n")
        │
        ▼
SimplePipeTx::write()
        │
        ▼
postMessage(PipeWrite) ──────────────────▶ handle_pipe_write()
                                                  │
                                                  ▼
                                           PIPE_BUFFERS[id].push(data)
                                                  │
                                                  ▼
                                           Wake pending readers
                                                  │
                                           ◀──────┘

                                           PipeRead { pipe_id }
                                           ◀─────────────────────── SimplePipeRx::read()
                                                  │                         │
                                                  ▼                         │
                                           Atomics.store(data)              │
                                           Atomics.notify() ────────────────▶
                                                                     Worker wakes,
                                                                     reads from buffer
```

## Implementation Steps

### Step 1: Create Branch and Cherry-Pick Fixes

```bash
cd /home/nathan/misc/wasmer-js
git checkout -b scheduler-routed-pipes d822560

# Cherry-pick Node.js SAB fix (has conflict in .cargo/config.toml)
git cherry-pick 9d7b627 --no-commit
# Resolve conflict: keep the new rustflags with shared memory flags:
#   rustflags = [
#     "-C", "target-feature=+atomics,+bulk-memory,+mutable-globals",
#     "-C", "link-arg=--shared-memory",
#     "-C", "link-arg=--import-memory",
#     "-C", "link-arg=--export-memory",
#     "-C", "link-arg=--max-memory=4294967296",
#     "--cfg=web_sys_unstable_apis",
#   ]
git add .cargo/config.toml rust-toolchain.toml
git commit -m "Cherry-pick: Node.js SharedArrayBuffer support"

# Cherry-pick scheduler race fix (should apply cleanly)
git cherry-pick 83a88ce
```

### Step 2: Create SimplePipe Type

Create new file `src/pipes/mod.rs`:

```rust
//! Scheduler-routed pipes for cross-Worker IPC.
//!
//! All pipe I/O is routed through the main scheduler thread via postMessage.
//! This trades some latency for simplicity and correctness.

use std::io::{self, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use js_sys::Atomics;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use virtual_fs::VirtualFile;

use crate::tasks::{SchedulerMessage, WorkerMessage};
use crate::tasks::thread_pool_worker::HOST_EXEC_INT32_VIEW;

/// Global counter for unique pipe IDs
static NEXT_PIPE_ID: AtomicU64 = AtomicU64::new(1);

/// A pipe that routes all I/O through the scheduler.
#[derive(Debug)]
pub struct SimplePipe {
    pipe_id: u64,
    tx: SimplePipeTx,
    rx: SimplePipeRx,
}

/// Transmit (write) end of a SimplePipe.
#[derive(Debug, Clone)]
pub struct SimplePipeTx {
    pipe_id: u64,
}

/// Receive (read) end of a SimplePipe.
#[derive(Debug, Clone)]
pub struct SimplePipeRx {
    pipe_id: u64,
}

unsafe impl Send for SimplePipe {}
unsafe impl Sync for SimplePipe {}
unsafe impl Send for SimplePipeTx {}
unsafe impl Sync for SimplePipeTx {}
unsafe impl Send for SimplePipeRx {}
unsafe impl Sync for SimplePipeRx {}

impl SimplePipe {
    /// Create a new pipe with a unique ID.
    pub fn new() -> Self {
        let pipe_id = NEXT_PIPE_ID.fetch_add(1, Ordering::SeqCst);

        // Register pipe with scheduler
        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeCreate { pipe_id });
        let _ = msg.emit();

        SimplePipe {
            pipe_id,
            tx: SimplePipeTx { pipe_id },
            rx: SimplePipeRx { pipe_id },
        }
    }

    /// Create a pipe from an existing ID (for child side after fork).
    pub fn from_id(pipe_id: u64) -> Self {
        SimplePipe {
            pipe_id,
            tx: SimplePipeTx { pipe_id },
            rx: SimplePipeRx { pipe_id },
        }
    }

    pub fn id(&self) -> u64 { self.pipe_id }

    pub fn split(self) -> (SimplePipeTx, SimplePipeRx) {
        (self.tx, self.rx)
    }
}

impl Default for SimplePipe {
    fn default() -> Self { Self::new() }
}

impl SimplePipeTx {
    pub fn id(&self) -> u64 { self.pipe_id }

    pub fn write_data(&self, data: &[u8]) -> io::Result<usize> {
        if data.is_empty() { return Ok(0); }

        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeWrite {
            pipe_id: self.pipe_id,
            data: data.to_vec(),
        });

        msg.emit().map_err(|e| {
            io::Error::new(io::ErrorKind::BrokenPipe, format!("{:?}", e))
        })?;

        Ok(data.len())
    }

    pub fn close(&self) {
        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeClose {
            pipe_id: self.pipe_id
        });
        let _ = msg.emit();
    }
}

impl SimplePipeRx {
    pub fn id(&self) -> u64 { self.pipe_id }

    /// Read data, blocking via Atomics.wait until data available or EOF.
    pub fn read_blocking(&self, buf: &mut [u8]) -> io::Result<usize> {
        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeRead {
            pipe_id: self.pipe_id,
            max_len: buf.len() as u32,
        });

        msg.emit().map_err(|e| {
            io::Error::new(io::ErrorKind::BrokenPipe, format!("{:?}", e))
        })?;

        // Block using Atomics.wait until scheduler responds
        HOST_EXEC_INT32_VIEW.with(|view_cell| {
            let view_opt = view_cell.borrow();
            let view = view_opt.as_ref()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "buffer not initialized"))?;

            // Reset status and wait
            Atomics::store(view, 0, 0).map_err(|e|
                io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
            let _ = Atomics::wait(view, 0, 0);

            // Read response: [0]=status, [1]=len, [16..]=data
            let status = Atomics::load(view, 0).map_err(|e|
                io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
            let data_len = Atomics::load(view, 1).map_err(|e|
                io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))? as usize;

            match status {
                1 => { // Data
                    let sab = view.buffer();
                    let uint8_view = js_sys::Uint8Array::new(&sab);
                    let to_copy = data_len.min(buf.len());
                    for i in 0..to_copy {
                        buf[i] = uint8_view.get_index((16 + i) as u32);
                    }
                    Ok(to_copy)
                }
                2 => Ok(0), // EOF
                _ => Err(io::Error::new(io::ErrorKind::BrokenPipe, "Pipe error")),
            }
        })
    }
}

impl Drop for SimplePipeTx {
    fn drop(&mut self) { self.close(); }
}

// std::io traits
impl Read for SimplePipeRx {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.read_blocking(buf) }
}

impl std::io::Write for SimplePipeTx {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.write_data(buf) }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl Seek for SimplePipeRx {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> { Ok(0) }
}

impl Seek for SimplePipeTx {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> { Ok(0) }
}

// Tokio async traits
impl AsyncRead for SimplePipeRx {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let temp_buf = buf.initialize_unfilled();
        match self.read_blocking(temp_buf) {
            Ok(n) => { buf.advance(n); Poll::Ready(Ok(())) }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for SimplePipeTx {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(self.write_data(buf))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.close(); Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for SimplePipeRx {
    fn start_seek(self: Pin<&mut Self>, _: SeekFrom) -> io::Result<()> { Ok(()) }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> { Poll::Ready(Ok(0)) }
}

impl AsyncSeek for SimplePipeTx {
    fn start_seek(self: Pin<&mut Self>, _: SeekFrom) -> io::Result<()> { Ok(()) }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> { Poll::Ready(Ok(0)) }
}

// Wrong-direction stubs
impl AsyncRead for SimplePipeTx {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Write-only pipe")))
    }
}

impl AsyncWrite for SimplePipeRx {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Read-only pipe")))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
}

// VirtualFile traits
impl VirtualFile for SimplePipeTx {
    fn last_accessed(&self) -> u64 { 0 }
    fn last_modified(&self) -> u64 { 0 }
    fn created_time(&self) -> u64 { 0 }
    fn size(&self) -> u64 { 0 }
    fn set_len(&mut self, _: u64) -> virtual_fs::Result<()> { Ok(()) }
    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> { Ok(()) }
    fn is_open(&self) -> bool { true }
    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Write-only")))
    }
    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(65536))
    }
}

impl VirtualFile for SimplePipeRx {
    fn last_accessed(&self) -> u64 { 0 }
    fn last_modified(&self) -> u64 { 0 }
    fn created_time(&self) -> u64 { 0 }
    fn size(&self) -> u64 { 0 }
    fn set_len(&mut self, _: u64) -> virtual_fs::Result<()> { Ok(()) }
    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> { Ok(()) }
    fn is_open(&self) -> bool { true }
    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(1)) // Claim ready, actual read will block
    }
    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Read-only")))
    }
}
```

### Step 3: Add Scheduler Message Types

Add to `src/tasks/scheduler_message.rs` enum:

```rust
/// Create a new pipe buffer in the scheduler
PipeCreate { pipe_id: u64 },

/// Write data to a pipe
PipeWrite { pipe_id: u64, data: Vec<u8> },

/// Read data from a pipe (worker will block on Atomics.wait)
PipeRead { pipe_id: u64, max_len: u32, worker_id: u32 },

/// Close a pipe (TX end closed, signals EOF to readers)
PipeClose { pipe_id: u64 },
```

Add serialization/deserialization for these types.

### Step 4: Add Scheduler Pipe Buffer Management

Add to `src/tasks/scheduler.rs`:

```rust
use std::collections::VecDeque;

struct PipeBuffer {
    data: VecDeque<u8>,
    closed: bool,
    pending_read: Option<(u32, u32)>, // (worker_id, max_len)
}

static PIPE_BUFFERS: Lazy<Mutex<HashMap<u64, PipeBuffer>>> = Lazy::new(|| Mutex::new(HashMap::new()));

// In execute() match:
SchedulerMessage::PipeCreate { pipe_id } => {
    PIPE_BUFFERS.lock().unwrap().insert(pipe_id, PipeBuffer::default());
    Ok(())
}

SchedulerMessage::PipeWrite { pipe_id, data } => {
    let mut buffers = PIPE_BUFFERS.lock().unwrap();
    if let Some(buffer) = buffers.get_mut(&pipe_id) {
        buffer.data.extend(data);
        if let Some((worker_id, max_len)) = buffer.pending_read.take() {
            let to_send: Vec<u8> = buffer.data.drain(..buffer.data.len().min(max_len as usize)).collect();
            drop(buffers);
            self.send_pipe_read_response(worker_id, pipe_id, Some(to_send))?;
        }
    }
    Ok(())
}

SchedulerMessage::PipeRead { pipe_id, max_len, worker_id } => {
    let mut buffers = PIPE_BUFFERS.lock().unwrap();
    if let Some(buffer) = buffers.get_mut(&pipe_id) {
        if !buffer.data.is_empty() {
            let to_send: Vec<u8> = buffer.data.drain(..buffer.data.len().min(max_len as usize)).collect();
            drop(buffers);
            self.send_pipe_read_response(worker_id, pipe_id, Some(to_send))?;
        } else if buffer.closed {
            drop(buffers);
            self.send_pipe_read_response(worker_id, pipe_id, None)?; // EOF
        } else {
            buffer.pending_read = Some((worker_id, max_len));
        }
    }
    Ok(())
}

SchedulerMessage::PipeClose { pipe_id } => {
    let mut buffers = PIPE_BUFFERS.lock().unwrap();
    if let Some(buffer) = buffers.get_mut(&pipe_id) {
        buffer.closed = true;
        if let Some((worker_id, _)) = buffer.pending_read.take() {
            drop(buffers);
            self.send_pipe_read_response(worker_id, pipe_id, None)?; // EOF
        }
    }
    Ok(())
}

fn send_pipe_read_response(&mut self, worker_id: u32, _pipe_id: u64, data: Option<Vec<u8>>) -> Result<(), Error> {
    for worker in self.idle.iter().chain(self.busy.iter()) {
        if worker.id() == worker_id {
            let view = worker.host_exec_int32_view();
            match data {
                Some(bytes) => {
                    Atomics::store(&view, 0, 1)?; // status=data
                    Atomics::store(&view, 1, bytes.len() as i32)?;
                    let sab = view.buffer();
                    let uint8_view = js_sys::Uint8Array::new(&sab);
                    for (i, b) in bytes.iter().enumerate() {
                        uint8_view.set_index((16 + i) as u32, *b);
                    }
                }
                None => {
                    Atomics::store(&view, 0, 2)?; // status=EOF
                }
            }
            Atomics::notify(&view, 0)?;
            return Ok(());
        }
    }
    Err(anyhow::anyhow!("Worker {} not found", worker_id))
}
```

### Step 5: Modify runtime.rs

Add `create_pipe()` to the Runtime impl:

```rust
fn create_pipe(&self) -> wasmer_wasix::runtime::CreatedPipe {
    let pipe = crate::pipes::SimplePipe::new();
    let (tx, rx) = pipe.split();
    wasmer_wasix::runtime::CreatedPipe::Virtual {
        tx: Box::new(tx),
        rx: Box::new(rx),
    }
}
```

### Step 6: Update lib.rs

```rust
pub mod pipes;
```

### Step 7: Handle Fork Pipe Transfer

Add to `post_message_payload.rs`:

```rust
#[derive(Debug, Default)]
pub(crate) struct ForkPipes {
    /// Vec of (fd, pipe_id, is_tx)
    pub pipes: Vec<(u32, u64, bool)>,
}
```

Modify task_wasm.rs to extract pipe IDs and pass them to child workers.

## Files to Create

| File | Description |
|------|-------------|
| `src/pipes/mod.rs` | SimplePipe implementation (~200 lines) |

## Files to Modify

| File | Changes |
|------|---------|
| `src/lib.rs` | Add `pub mod pipes;` |
| `src/runtime.rs` | Add `create_pipe()` method |
| `src/tasks/scheduler_message.rs` | Add PipeCreate, PipeWrite, PipeRead, PipeClose |
| `src/tasks/scheduler.rs` | Add PIPE_BUFFERS and handlers |
| `src/tasks/post_message_payload.rs` | Add ForkPipes type |
| `src/tasks/thread_pool_worker.rs` | Handle fork_pipes reconnection |

## Testing Plan

### Test Categories

#### Category 1: Basic Subprocess (no pipes)

These test that subprocesses can spawn and return output at all.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 1.1 | `echo hello` | stdout="hello\n" | Bash builtin |
| 1.2 | `/bin/echo hello` | stdout="hello\n" | External command |
| 1.3 | `ls /` | stdout contains "bin" | External, directory listing |
| 1.4 | `/bin/ls /` | stdout contains "bin" | Explicit path |
| 1.5 | `echo a; echo b` | stdout="a\nb\n" | Sequential commands |
| 1.6 | `exit 42` | exit_code=42 | Exit code propagation |

#### Category 2: Simple Pipes (two processes)

These test basic pipe functionality between two child processes.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 2.1 | `echo test \| cat` | stdout="test\n" | Builtin to external |
| 2.2 | `/bin/echo test \| cat` | stdout="test\n" | External to external |
| 2.3 | `echo test \| /bin/cat` | stdout="test\n" | Explicit cat path |
| 2.4 | `ls / \| head -3` | 3 lines | External to external |
| 2.5 | `echo hello \| wc -c` | stdout contains "6" | Byte counting |
| 2.6 | `echo hello \| wc -l` | stdout contains "1" | Line counting |
| 2.7 | `cat /etc/passwd \| head -1` | First line of passwd | File to filter |

#### Category 3: Multi-stage Pipes (3+ processes)

These test pipe chains with multiple stages.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 3.1 | `echo abc \| cat \| cat` | stdout="abc\n" | 3-stage passthrough |
| 3.2 | `echo abc \| cat \| cat \| cat` | stdout="abc\n" | 4-stage passthrough |
| 3.3 | `ls / \| head -5 \| tail -2` | 2 lines | Filter chain |
| 3.4 | `echo "a b c" \| tr ' ' '\n' \| wc -l` | stdout contains "3" | Transform chain |

#### Category 4: Command Substitution

These test `$(...)` and backtick substitution which use pipes internally.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 4.1 | ``echo `echo inner` `` | stdout="inner\n" | Backticks |
| 4.2 | `echo $(echo inner)` | stdout="inner\n" | $() syntax |
| 4.3 | `echo $(echo $(echo deep))` | stdout="deep\n" | Nested substitution |
| 4.4 | `x=$(echo val); echo $x` | stdout="val\n" | Assign from substitution |
| 4.5 | ``echo `ls / \| head -1` `` | First dir entry | Pipe inside backticks |

#### Category 5: Stdin Handling

These test stdin being passed to subprocesses.

| Test | Command | stdin | Expected | Notes |
|------|---------|-------|----------|-------|
| 5.1 | `cat` | "hello" | stdout="hello" | Pass-through |
| 5.2 | `wc -c` | "test" | stdout contains "4" | Count stdin |
| 5.3 | `head -1` | "a\nb\nc" | stdout="a\n" | Filter stdin |
| 5.4 | `cat -n` | "x\ny" | Numbered lines | Transform stdin |

#### Category 6: Stderr Handling

These test stderr is captured separately from stdout.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 6.1 | `echo err >&2` | stderr="err\n", stdout="" | Redirect to stderr |
| 6.2 | `echo out; echo err >&2` | stdout="out\n", stderr="err\n" | Both streams |
| 6.3 | `ls /nonexistent` | stderr contains error | Command error |

#### Category 7: Large Data

These test pipes with larger data volumes.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 7.1 | `seq 1 1000 \| wc -l` | stdout contains "1000" | 1000 lines |
| 7.2 | `seq 1 100 \| cat \| cat \| wc -l` | stdout contains "100" | Multi-pipe with data |
| 7.3 | `dd if=/dev/zero bs=1024 count=10 \| wc -c` | ~10240 bytes | Binary data |

#### Category 8: Sequential Execution (State Isolation)

These test that running multiple commands sequentially doesn't leak state.

| Test | Description | Expected |
|------|-------------|----------|
| 8.1 | Run `echo 1`, `echo 2`, `echo 3` sequentially | Each succeeds |
| 8.2 | Run 10 `echo test \| cat` in sequence | All succeed |
| 8.3 | Run pipe test, then simple test, then pipe test | All succeed |
| 8.4 | Run 5 different pipe commands | All succeed, no hangs |

#### Category 9: Process Lifecycle

These test proper cleanup and exit handling.

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 9.1 | `true` | exit_code=0 | Success exit |
| 9.2 | `false` | exit_code=1 | Failure exit |
| 9.3 | `exit 0` | exit_code=0 | Explicit exit |
| 9.4 | `sh -c 'exit 5'` | exit_code=5 | Nested shell exit |
| 9.5 | `echo x \| false` | exit_code=1 | Pipe exit code (last command) |

#### Category 10: Edge Cases

| Test | Command | Expected | Notes |
|------|---------|----------|-------|
| 10.1 | `echo ""` | stdout="\n" | Empty string |
| 10.2 | `echo -n ""` | stdout="" | No output |
| 10.3 | `true \| true \| true` | exit_code=0 | All-true pipe |
| 10.4 | `echo x \| cat \| false` | exit_code=1 | Pipe with failure at end |
| 10.5 | `(echo a; echo b) \| cat` | stdout="a\nb\n" | Subshell to pipe |
| 10.6 | `{ echo a; echo b; } \| cat` | stdout="a\nb\n" | Group to pipe |

### Test Implementation

```javascript
const tests = [
  // Category 1: Basic Subprocess
  { name: "1.1 echo builtin", cmd: "echo hello", expect: { stdout: "hello\n" } },
  { name: "1.2 /bin/echo", cmd: "/bin/echo hello", expect: { stdout: "hello\n" } },
  { name: "1.3 ls /", cmd: "ls /", expect: { stdoutContains: "bin" } },
  { name: "1.5 sequential", cmd: "echo a; echo b", expect: { stdout: "a\nb\n" } },

  // Category 2: Simple Pipes
  { name: "2.1 echo|cat", cmd: "echo test | cat", expect: { stdout: "test\n" } },
  { name: "2.5 echo|wc -c", cmd: "echo hello | wc -c", expect: { stdoutContains: "6" } },

  // Category 3: Multi-stage Pipes
  { name: "3.1 echo|cat|cat", cmd: "echo abc | cat | cat", expect: { stdout: "abc\n" } },

  // Category 4: Command Substitution
  { name: "4.1 backticks", cmd: "echo `echo inner`", expect: { stdout: "inner\n" } },
  { name: "4.2 $()", cmd: "echo $(echo inner)", expect: { stdout: "inner\n" } },

  // Category 8: Sequential (run in loop)
  // Category 9: Exit codes
  { name: "9.1 true", cmd: "true", expect: { exitCode: 0 } },
  { name: "9.2 false", cmd: "false", expect: { exitCode: 1 } },
];

async function runTest(pkg, test) {
  const instance = await pkg.commands["bash"].run({ args: ["-c", test.cmd] });
  const result = await Promise.race([
    instance.wait(),
    new Promise((_, r) => setTimeout(() => r(new Error("TIMEOUT")), 10000))
  ]);

  if (test.expect.stdout !== undefined && result.stdout !== test.expect.stdout) {
    throw new Error(`stdout mismatch: got ${JSON.stringify(result.stdout)}`);
  }
  if (test.expect.stdoutContains && !result.stdout.includes(test.expect.stdoutContains)) {
    throw new Error(`stdout missing ${test.expect.stdoutContains}`);
  }
  if (test.expect.exitCode !== undefined && result.code !== test.expect.exitCode) {
    throw new Error(`exit code mismatch: got ${result.code}`);
  }
}

async function runAllTests() {
  const pkg = await Wasmer.fromRegistry("sharrattj/bash");
  for (const test of tests) {
    try {
      await runTest(pkg, test);
      console.log(`✓ ${test.name}`);
    } catch (e) {
      console.log(`✗ ${test.name}: ${e.message}`);
    }
  }
}
```

### Known Issues to Watch For

| Issue | Symptom | Cause |
|-------|---------|-------|
| Pipe closed early | SIGPIPE / "Broken pipe" | TX dropped before data written |
| Read hangs forever | TIMEOUT | TX never closed, RX waiting for EOF |
| Sequential test failure | Works alone, fails in sequence | State not cleaned up between runs |
| Empty stdout | stdout="" but should have data | Pipe buffer not flushed/transferred |
| Wrong exit code | exit=45 instead of expected | Known bash/WASIX issue |

## Rollback Plan

The SharedArrayBuffer implementation is preserved in git history at commits `617735f` through `a71e5ef` if needed for reference or if this approach proves insufficient.
