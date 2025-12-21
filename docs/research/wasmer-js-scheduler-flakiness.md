# Wasmer-JS Scheduler Flakiness

## Status: ROOT CAUSE IDENTIFIED

## Summary

When running wasmer-js SDK tests sequentially in Node.js, the 3rd+ instance hangs during `instance.wait()`. The root cause is a **race condition in scheduler shutdown** where `Close` is processed before pending `SpawnBlocking` cleanup callbacks.

## Key Finding

**Tests work perfectly in separate processes but fail after 2-3 runs in the same process.**

```bash
# This works (5/5 pass):
for i in 1 2 3 4 5; do node /tmp/single-test.mjs $i; done

# This fails (hangs on test 3):
node /tmp/sequential-test.mjs  # runs 5 tests in same process
```

## Root Cause

### The Race Condition

1. Task runs on worker, completes, and WASI runtime schedules a `SpawnBlocking` cleanup callback
2. Main thread's `instance.wait()` sees the exit condition and returns
3. `Instance` is dropped, triggering `ThreadPool::drop()` which calls `scheduler.close()`
4. `Close` message is sent to scheduler channel
5. Scheduler's async loop processes `Close` BEFORE the `SpawnBlocking` callback
6. Cleanup task never runs, leaving dangling state
7. Next test encounters corrupted/dangling state and hangs

### Evidence from Logs

```
wasi[1]::main() has exited with ExitCode::0     // Task done
Sending msg=SpawnBlocking(_)                     // Cleanup scheduled
...
Dropping Scheduler                               // Scheduler closes!
```

The `SpawnBlocking` message is sent AFTER main() exits but the scheduler closes before processing it.

### The Problematic Code

In `scheduler.rs`:
```rust
while let Some(msg) = receiver.recv().await {
    if let SchedulerMessage::Close = msg {
        break;  // <-- Breaks immediately, pending messages not processed!
    }
    scheduler.execute(msg)?;
}
```

## Proposed Fixes

### Option 1: Drain pending messages before close

```rust
SchedulerMessage::Close => {
    // Process all remaining messages before closing
    while let Ok(msg) = receiver.try_recv() {
        if !matches!(msg, SchedulerMessage::Close) {
            scheduler.execute(msg)?;
        }
    }
    break;
}
```

### Option 2: Reference count pending tasks

Track the number of pending tasks and don't allow close until all complete.

### Option 3: Wait for cleanup in Instance::wait()

Modify `wait()` to not just wait for exit code but also for a "cleanup complete" signal.

## Workarounds

For now, run tests in separate Node.js processes:

```javascript
import { spawn } from 'child_process';

async function runIsolated(script) {
  const child = spawn('node', [script]);
  await new Promise(resolve => child.on('close', resolve));
}
```

## Environment

- Node.js v24.3.0
- wasmer-js v0.8.0 (rivet-patches branch)
- Rust nightly-2024-12-01
- SharedArrayBuffer enabled

## Related Files

- `src/tasks/scheduler.rs` - Scheduler close handling (line 220)
- `src/tasks/thread_pool.rs` - ThreadPool::drop() (line 58)
- `src/instance.rs` - Instance::wait() (line 189)
