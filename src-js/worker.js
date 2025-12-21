Error.stackTraceLimit = 50;

// Use native worker_threads API for Node.js
// This avoids issues with the web-worker package
import { parentPort, workerData, threadId } from 'node:worker_threads';

let pendingMessages = [];
let worker = undefined;
let workerId = undefined;

// Handle messages from parent thread
parentPort.on('message', data => {
  const evType = data?.type;
  const evId = data?.id;

  if (evType == "init") {
    handleInit(data).catch(err => {
      console.error(`[worker ${evId}] init error:`, err);
    });
  } else {
    handleMessage(data).catch(console.error);
  }
});

// Polyfill postMessage for code that expects Web Worker API
globalThis.postMessage = (data, transfer) => {
  parentPort.postMessage(data, transfer);
};

async function handleInit(data) {
  const { memory, module, id, sdkUrl, hostExecBuffer, pipePool } = data;

  workerId = id;

  const sdk = await import(sdkUrl);
  const { init, ThreadPoolWorker } = sdk;

  await init({ module: module, sdkUrl: sdkUrl, memory: memory });

  worker = new ThreadPoolWorker(id, hostExecBuffer, pipePool);

  // Handle any messages that arrived before init completed
  for (const msg of pendingMessages.splice(0, pendingMessages.length)) {
    await worker.handle(msg);
  }
}

async function handleMessage(data) {
  if (worker) {
    await worker.handle(data);
  } else {
    pendingMessages.push(data);
  }
}
