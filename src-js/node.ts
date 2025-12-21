export * from "./";
import { Worker as NodeWorker } from "node:worker_threads";
import { fileURLToPath, pathToFileURL } from 'node:url';
import { init as load, InitOutput, WasmerInitInput, VolumeTree } from "./";
import fs from "node:fs/promises";
import path from "node:path";

// Serialize worker creation to avoid Node.js worker_threads bug where
// workers created simultaneously can have broken microtask queues.
// We queue worker creation requests and process them one at a time.
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
    // Wait BEFORE creating each worker to ensure previous worker has time to initialize
    // This works around a Node.js bug where workers created simultaneously can have
    // their microtask queues corrupted (see docs/research/nodejs-worker-starvation-bug.md)
    await new Promise(r => setTimeout(r, WORKER_CREATION_DELAY));

    const request = workerCreationQueue.shift()!;

    // Convert file path to file:// URL if needed
    const workerUrl = request.url.startsWith('file://') ? request.url : pathToFileURL(request.url).href;

    const worker = new NodeWorker(new URL(workerUrl), {
      workerData: { name: request.options?.name }
    });

    request.resolve(worker);
  }

  isCreatingWorker = false;
}

// Custom Worker wrapper that adapts Node.js worker_threads to Web Worker API
class Worker {
  private _worker: NodeWorker | null = null;
  private _ready: Promise<NodeWorker>;
  private _messageQueue: Array<{ data: any; transferList?: any[] }> = [];
  public onmessage: ((ev: { data: any }) => void) | null = null;
  public onerror: ((ev: any) => void) | null = null;

  constructor(url: string, options?: { type?: string; name?: string }) {
    // Queue worker creation
    this._ready = new Promise<NodeWorker>((resolve) => {
      workerCreationQueue.push({ url, options, resolve });
      // Trigger queue processing
      setTimeout(() => processWorkerCreationQueue(), 0);
    });

    // Set up worker once it's created
    this._ready.then((worker) => {
      this._worker = worker;

      worker.on('message', (data: any) => {
        if (this.onmessage) {
          this.onmessage({ data });
        }
      });

      worker.on('error', (error: Error) => {
        if (this.onerror) {
          (error as any).type = 'error';
          this.onerror(error);
        }
      });

      // Send any queued messages
      for (const msg of this._messageQueue) {
        worker.postMessage(msg.data, msg.transferList);
      }
      this._messageQueue = [];
    });
  }

  postMessage(data: any, transferList?: any[]) {
    if (this._worker) {
      this._worker.postMessage(data, transferList);
    } else {
      // Queue message until worker is ready
      this._messageQueue.push({ data, transferList });
    }
  }

  terminate() {
    if (this._worker) {
      this._worker.terminate();
    } else {
      this._ready.then(w => w.terminate());
    }
  }
}

//@ts-ignore
globalThis.Worker = Worker;

/**
 * Initialize the underlying WebAssembly module, defaulting to an embedded
 * copy of the `*.wasm` file.
 */
export const init = async (
  initValue?: WasmerInitInput,
): Promise<InitOutput> => {
  if (!initValue) {
    initValue = {};
  }

  if (!initValue.module) {
    const path = new URL("wasmer_js_bg.wasm", import.meta.url).pathname;
    initValue.module = await fs.readFile(path);
  }
  if (!initValue.workerUrl) {
    initValue.workerUrl = fileURLToPath(new URL("worker.mjs", import.meta.url));
  }
  if (!initValue.sdkUrl) {
    initValue.sdkUrl = fileURLToPath(new URL("node.mjs", import.meta.url));
  }
  return load(initValue);
};

export async function walkDir(dir: string, result: VolumeTree = {}) {
  let list = await fs.readdir(dir);
  for (let item of list) {
    const itemPath = path.join(dir, item);
    let stats = await fs.stat(itemPath);
    if (await stats.isDirectory()) {
      result[item] = {};
      await walkDir(itemPath, result[item] as VolumeTree);
    } else {
      const fileName = path.basename(item);
      result[fileName] = {
        data: new Uint8Array(await fs.readFile(itemPath)), // , { encoding: 'utf-8'}
        modified: stats.mtime,
      };
    }
  }
  return result;
}
