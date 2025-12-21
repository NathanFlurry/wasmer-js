# Wasmer-JS Scheduler Flakiness

## Status: FIXED

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

## Deeper Analysis

Through detailed logging, the issue is more specific:

1. **Stdout/stderr streams never close**: The test hangs because `wait()` is waiting for streams to close, but they never do.

2. **SpawnBlocking is the culprit**: After main() exits, WASI schedules a `SpawnBlocking` callback that closes the streams. This callback never runs.

3. **PostMessage timing**: SpawnBlocking is sent via `postMessage` from the worker. The scheduler's `Close` message races with the postMessage delivery.

4. **Multiple schedulers**: The logs show multiple "Dropping Scheduler" events, suggesting schedulers from different operations interfere with each other.

## Proposed Fixes

### Option 1: Drain pending messages before close (implemented, not sufficient)

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

This doesn't work because SpawnBlocking arrives via postMessage AFTER Close is sent to the channel.

### Option 2: Delay Close via event loop

Schedule Close via `spawn_local` to let pending postMessage handlers run first. This was tried but didn't fully solve the issue.

### Option 3: Reference count pending tasks

Track the number of pending tasks and don't allow close until all complete.

### Option 4: Ensure stream closure before Instance destruction

Have the WASI task close streams synchronously before signaling exit, rather than scheduling a SpawnBlocking callback.

## The Fix

### 1. Delay Close via Event Loop (scheduler.rs)

The fix is to delay sending `Close` to the scheduler channel, allowing pending `postMessage` handlers to be processed first:

```rust
pub fn close(&self) {
    let channel = self.channel.clone();
    wasm_bindgen_futures::spawn_local(async move {
        // Yield to macrotask queue via setTimeout(0)
        let global = js_sys::global();
        let set_timeout = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
            .expect("setTimeout should exist");
        let set_timeout: js_sys::Function = set_timeout.into();
        let promise = js_sys::Promise::new(&mut |resolve, _| {
            let _ = set_timeout.call2(&global, &resolve, &JsValue::from_f64(0.0));
        });
        let _ = wasm_bindgen_futures::JsFuture::from(promise).await;

        // Now send Close - pending messages should have arrived
        let _ = channel.send(SchedulerMessage::Close);
    });
}
```

### 2. Fix GlobalScope::sleep() for Node.js (utils.rs)

The `GlobalScope::sleep()` method used `web_sys` APIs that don't work in Node.js worker threads. Fixed to use reflection-based setTimeout call:

```rust
pub fn sleep(&self, milliseconds: i32) -> Promise {
    Promise::new(&mut |resolve, reject| {
        let global = js_sys::global();
        let set_timeout = match js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout")) {
            Ok(f) => f,
            Err(_) => {
                let error = js_sys::Error::new("Unable to find setTimeout()");
                reject.call1(&reject, &error).unwrap();
                return;
            }
        };
        let set_timeout: js_sys::Function = match set_timeout.dyn_into() {
            Ok(f) => f,
            Err(_) => {
                let error = js_sys::Error::new("setTimeout is not a function");
                reject.call1(&reject, &error).unwrap();
                return;
            }
        };
        let _ = set_timeout.call2(&global, &resolve, &JsValue::from_f64(milliseconds as f64));
    })
}
```

### 3. Use Non-Interactive Mode in Tests

For tests that don't require interactive TTY, provide empty stdin to force non-interactive mode:

```javascript
const instance = await pkg.commands['quickjs'].run({
  args: ['--eval', 'console.log("hello")'],
  stdin: ''  // Force non-interactive mode
});
```

In interactive mode, the TTY task holds a clone of `stdout_pipe` and waits for stdin EOF. If stdin isn't closed, stdout never gets EOF.

## Workarounds (No Longer Needed)

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
