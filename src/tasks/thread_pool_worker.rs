use std::cell::Cell;
use std::cell::RefCell;

use js_sys::{Int32Array, SharedArrayBuffer};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

use crate::tasks::{AsyncJob, BlockingJob, Notification, PostMessagePayload, WorkerMessage};

/// Thread-local storage for the current worker's ID.
/// This is set when the worker is created and used when sending messages back to the scheduler.
thread_local! {
    pub(crate) static CURRENT_WORKER_ID: Cell<Option<u32>> = Cell::new(None);
    /// The SharedArrayBuffer for host_exec IPC, stored as thread-local since each worker has its own.
    pub(crate) static HOST_EXEC_BUFFER: RefCell<Option<SharedArrayBuffer>> = RefCell::new(None);
    /// Int32Array view of the host_exec buffer for atomic operations.
    pub(crate) static HOST_EXEC_INT32_VIEW: RefCell<Option<Int32Array>> = RefCell::new(None);
}

/// The Rust state for a worker in the threadpool.
#[wasm_bindgen(skip_typescript)]
pub struct ThreadPoolWorker {
    id: u32,
}

impl std::fmt::Debug for ThreadPoolWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPoolWorker")
            .field("id", &self.id)
            .finish()
    }
}

impl ThreadPoolWorker {
    fn busy(&self) -> impl Drop {
        struct BusyGuard;
        impl Drop for BusyGuard {
            fn drop(&mut self) {
                let _ = WorkerMessage::MarkIdle.emit();
            }
        }

        let _ = WorkerMessage::MarkBusy.emit();

        BusyGuard
    }

    #[tracing::instrument(level = "debug", skip_all, fields(worker.id = self.id))]
    pub async fn handle(&self, msg: JsValue) -> Result<(), crate::utils::Error> {
        // Safety: The message was created using PostMessagePayload::to_js()
        let msg = unsafe { PostMessagePayload::try_from_js(msg)? };

        tracing::trace!(?msg, "Handling a message");

        match msg {
            PostMessagePayload::Async(async_job) => self.execute_async(async_job).await,
            PostMessagePayload::Blocking(blocking) => self.execute_blocking(blocking).await,
            PostMessagePayload::Notification(Notification::CacheModule { hash, module: _ }) => {
                tracing::warn!(%hash, "TODO Caching module");

                Ok(())
            }
            // We no longer wait for these responses - host_exec uses Atomics for synchronization
            PostMessagePayload::HostExecStartResponse { .. } => {
                // Ignored - we return session_id immediately without waiting
                Ok(())
            }
            PostMessagePayload::HostExecReadResponse { .. } => {
                // Ignored - we use SharedArrayBuffer + Atomics for read responses
                Ok(())
            }
        }
    }

    async fn execute_async(&self, job: AsyncJob) -> Result<(), crate::utils::Error> {
        match job {
            AsyncJob::Thunk(thunk) => {
                thunk().await;
            }
        }

        Ok(())
    }

    async fn execute_blocking(&self, job: BlockingJob) -> Result<(), crate::utils::Error> {
        match job {
            BlockingJob::Thunk(thunk) => {
                let _guard = self.busy();
                thunk();
            }
            BlockingJob::SpawnWithModule { module, task } => {
                let _guard = self.busy();
                task(module.into());
            }
            BlockingJob::SpawnWithModuleAndMemory {
                module,
                memory,
                mut spawn_wasm,
                subprocess_stdio,
            } => {
                // If subprocess_stdio is Some, inject SharedPipes for child's stdio
                // This replaces the broken tokio pipes with SharedArrayBuffer-based pipes
                if let Some(ref buffers) = subprocess_stdio {
                    spawn_wasm.inject_subprocess_stdio(buffers);
                }

                let task = spawn_wasm.begin().await;
                let _guard = self.busy();
                task.execute(module, memory.into()).await?;
            }
        }

        Ok(())
    }
}

#[wasm_bindgen]
impl ThreadPoolWorker {
    #[wasm_bindgen(constructor)]
    pub fn new(id: u32, host_exec_buffer: SharedArrayBuffer) -> ThreadPoolWorker {
        // Store the worker ID in thread-local storage for use by HostExecImpl
        CURRENT_WORKER_ID.set(Some(id));

        // Create Int32Array view for atomic operations
        let int32_view = Int32Array::new(&host_exec_buffer);

        // Store the buffer and view in thread-local storage
        HOST_EXEC_BUFFER.with(|buf| {
            *buf.borrow_mut() = Some(host_exec_buffer);
        });
        HOST_EXEC_INT32_VIEW.with(|view| {
            *view.borrow_mut() = Some(int32_view);
        });

        ThreadPoolWorker { id }
    }

    #[wasm_bindgen(js_name = "handle")]
    pub async fn js_handle(&self, msg: JsValue) -> Result<(), crate::utils::Error> {
        self.handle(msg).await
    }
}
