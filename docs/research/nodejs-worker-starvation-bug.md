# Node.js Worker Threads Starvation Bug

## Status: FIXED

## Summary

When Node.js `worker_threads` are created in quick succession, some workers experience microtask queue starvation - their async functions stop executing after the first `await`, and `queueMicrotask` callbacks never fire. This is a fundamental bug in Node.js that affects the `web-worker` package and native `worker_threads` equally.

## Symptoms

1. Workers receive messages (message handler fires and logs)
2. Async function is called and executes up to first `await`
3. After `await` completes, the async function never resumes
4. `queueMicrotask` callbacks never execute in affected workers
5. `setImmediate` and `setTimeout` callbacks never execute

## Pattern

- When 2 workers are created simultaneously, the FIRST worker often fails
- Single workers created alone always succeed
- The issue is timing-dependent but highly reproducible

Example from logs:
```
[worker] message type: init id: 1  <- Worker 1 receives message
[worker] message type: init id: 2  <- Worker 2 receives message
[handleInit] ENTERED, id: 2        <- Worker 2 proceeds
                                   <- Worker 1 NEVER proceeds!
```

## Root Cause Analysis

The bug appears to be in Node.js's event loop management for worker threads:

1. When multiple workers are created rapidly, their V8 isolates start concurrently
2. During module loading and initial execution, something corrupts the microtask queue for some workers
3. The main synchronous code executes, but the microtask queue is never drained
4. All async operations (Promise resolution, queueMicrotask, etc.) fail silently

## Verification

We verified the microtask queue is broken with this test:
```javascript
let microtaskRan = false;
await new Promise(r => {
  queueMicrotask(() => {
    microtaskRan = true;
    r();
  });
});
// For affected workers, this Promise never resolves
```

## The Fix

Serialize worker creation with a delay between each worker. This ensures each worker's event loop stabilizes before the next worker is created.

### Implementation in `node.ts`:

```typescript
// Serialize worker creation to avoid Node.js worker_threads bug where
// workers created simultaneously can have broken microtask queues.
const workerCreationQueue: Array<{
  url: string;
  options: { type?: string; name?: string } | undefined;
  resolve: (worker: NodeWorker) => void;
}> = [];
let isCreatingWorker = false;
const WORKER_CREATION_DELAY = 100; // ms between worker creations

async function processWorkerCreationQueue() {
  if (isCreatingWorker) return;
  isCreatingWorker = true;

  while (workerCreationQueue.length > 0) {
    // Wait BEFORE creating each worker
    await new Promise(r => setTimeout(r, WORKER_CREATION_DELAY));

    const request = workerCreationQueue.shift()!;
    const worker = new NodeWorker(new URL(request.url), {
      workerData: { name: request.options?.name }
    });
    request.resolve(worker);
  }

  isCreatingWorker = false;
}

class Worker {
  private _ready: Promise<NodeWorker>;
  private _messageQueue: Array<{ data: any; transferList?: any[] }> = [];

  constructor(url: string, options?: { type?: string; name?: string }) {
    this._ready = new Promise((resolve) => {
      workerCreationQueue.push({ url, options, resolve });
      setTimeout(() => processWorkerCreationQueue(), 0);
    });
    // Queue messages until worker is ready, etc.
  }
}
```

## Trade-offs

- **Latency**: Worker creation takes longer (100ms per worker)
- **Throughput**: Impacts scenarios that create many workers quickly
- **Reliability**: Eliminates random worker failures

The 100ms delay could potentially be reduced with more testing.

## Alternative Approaches Tried (Failed)

1. **Using `setImmediate` to defer message processing**: Didn't help - setImmediate itself never fires
2. **Random delays in workers**: Didn't help - the corruption happens before delays execute
3. **Using web-worker package vs native worker_threads**: Same issue with both
4. **Staggered delays based on worker ID**: Didn't help - damage is already done by creation time

## Environment

- Node.js v24.3.0
- wasmer-js v0.8.0
- All tested on Linux x64

## Related Files

- `src-js/node.ts` - Worker creation serialization
- `src-js/worker.js` - Worker message handling (with syncLog for debugging)
