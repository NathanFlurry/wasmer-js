use std::marker::PhantomData;

use derivative::Derivative;
use js_sys::{Uint8Array, WebAssembly};
use wasm_bindgen::JsValue;
use wasmer::js::AsJs;
use wasmer_types::ModuleHash;

use crate::{
    tasks::{
        interop::{Deserializer, Serializer},
        task_wasm::SpawnWasm,
        AsyncTask, BlockingModuleTask, BlockingTask,
    },
    utils::Error,
};

/// Messages sent from the [`crate::tasks::ThreadPool`] handle to the
/// `Scheduler`.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum SchedulerMessage {
    /// Close the scheduler.
    Close,
    /// Run a promise on a worker thread.
    SpawnAsync(#[derivative(Debug(format_with = "crate::utils::hidden"))] AsyncTask),
    /// Run a blocking operation on a worker thread.
    SpawnBlocking(#[derivative(Debug(format_with = "crate::utils::hidden"))] BlockingTask),
    /// A message sent from a worker thread.
    /// Mark a worker as idle.
    WorkerIdle { worker_id: u32 },
    /// Mark a worker as busy.
    WorkerBusy { worker_id: u32 },
    /// Host execution request from worker.
    HostExecStart {
        /// The worker that sent this request.
        worker_id: u32,
        /// Unique request ID for matching response.
        request_id: u64,
        /// JSON-encoded HostExecRequest.
        request_json: Vec<u8>,
    },
    /// Host execution read request.
    HostExecRead {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
    },
    /// Host execution write request.
    HostExecWrite {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
        data: Vec<u8>,
    },
    /// Host execution close stdin request.
    HostExecCloseStdin {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
    },
    /// Host execution try_read request (non-blocking).
    HostExecTryRead {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
    },
    /// Host execution poll request.
    HostExecPoll {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
    },
    /// Host execution signal request.
    HostExecSignal {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
        signal: u32,
    },
    /// Host execution child output - WASM sends child stdout/stderr/exit to Node.
    HostExecChildOutput {
        worker_id: u32,
        request_id: u64,
        session_id: u64,
        child_id: u64,
        msg_type: u32,  // 20=stdout, 21=stderr, 22=exit
        data: Vec<u8>,
    },
    /// Internal message: host_exec read completed (async Promise resolved).
    HostExecReadComplete {
        worker_id: u32,
        request_id: u64,
        msg_type: u32,
        data: Vec<u8>,
    },
    /// Tell all workers to cache a WebAssembly module.
    #[allow(dead_code)]
    CacheModule {
        hash: ModuleHash,
        module: wasmer::Module,
    },
    /// Run a task in the background, explicitly transferring the
    /// [`js_sys::WebAssembly::Module`] to the worker.
    SpawnWithModule {
        module: wasmer::Module,
        #[derivative(Debug(format_with = "crate::utils::hidden"))]
        task: BlockingModuleTask,
    },
    /// Run a task in the background, explicitly transferring the
    /// [`js_sys::WebAssembly::Module`] to the worker.
    SpawnWithModuleAndMemory {
        module: wasmer::Module,
        memory: Option<wasmer::Memory>,
        spawn_wasm: SpawnWasm,
    },
    #[doc(hidden)]
    #[allow(dead_code)]
    Markers {
        /// [`wasmer::Module`] and friends are `!Send` in practice.
        not_send: PhantomData<*const ()>,
        /// Mark this variant as unreachable.
        uninhabited: std::convert::Infallible,
    },
}

impl SchedulerMessage {
    pub(crate) unsafe fn try_from_js(value: JsValue) -> Result<Self, Error> {
        let de = Deserializer::new(value);

        match de.ty()?.as_str() {
            consts::TYPE_SPAWN_ASYNC => {
                let task = de.boxed(consts::PTR)?;
                Ok(SchedulerMessage::SpawnAsync(task))
            }
            consts::TYPE_SPAWN_BLOCKING => {
                let task = de.boxed(consts::PTR)?;
                Ok(SchedulerMessage::SpawnBlocking(task))
            }
            consts::TYPE_WORKER_IDLE => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                Ok(SchedulerMessage::WorkerIdle { worker_id })
            }
            consts::TYPE_WORKER_BUSY => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                Ok(SchedulerMessage::WorkerBusy { worker_id })
            }
            consts::TYPE_CACHE_MODULE => {
                let hash = de.string(consts::MODULE_HASH)?;
                let hash = if let Ok(hash) = ModuleHash::sha256_parse_hex(&hash) {
                    hash
                } else {
                    ModuleHash::xxhash_parse_hex(&hash)?
                };
                let module: WebAssembly::Module = de.js(consts::MODULE)?;
                Ok(SchedulerMessage::CacheModule {
                    hash,
                    module: module.into(),
                })
            }
            consts::TYPE_SPAWN_WITH_MODULE => {
                let module: WebAssembly::Module = de.js(consts::MODULE)?;
                let task = de.boxed(consts::PTR)?;
                Ok(SchedulerMessage::SpawnWithModule {
                    module: module.into(),
                    task,
                })
            }
            consts::TYPE_SPAWN_WITH_MODULE_AND_MEMORY => {
                let spawn_wasm: SpawnWasm = de.boxed(consts::PTR)?;
                let module: WebAssembly::Module = de.js(consts::MODULE)?;
                let module_bytes = spawn_wasm.module_bytes();
                let module = wasmer::Module::from((module, module_bytes));

                let memory = match spawn_wasm.shared_memory_type() {
                    Some(ty) => {
                        let memory: JsValue = de.js(consts::MEMORY)?;
                        let mut store = wasmer::Store::default();
                        wasmer::Memory::from_jsvalue(&mut store, &ty, &memory).ok()
                    }
                    None => None,
                };

                Ok(SchedulerMessage::SpawnWithModuleAndMemory {
                    module,
                    memory,
                    spawn_wasm,
                })
            }
            consts::TYPE_HOST_EXEC_START => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let request_array: Uint8Array = de.js(consts::REQUEST_JSON)?;
                let request_json = request_array.to_vec();
                Ok(SchedulerMessage::HostExecStart {
                    worker_id,
                    request_id,
                    request_json,
                })
            }
            consts::TYPE_HOST_EXEC_READ => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                Ok(SchedulerMessage::HostExecRead {
                    worker_id,
                    request_id,
                    session_id,
                })
            }
            consts::TYPE_HOST_EXEC_WRITE => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                let data_array: Uint8Array = de.js(consts::DATA)?;
                let data = data_array.to_vec();
                Ok(SchedulerMessage::HostExecWrite {
                    worker_id,
                    request_id,
                    session_id,
                    data,
                })
            }
            consts::TYPE_HOST_EXEC_CLOSE_STDIN => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                Ok(SchedulerMessage::HostExecCloseStdin {
                    worker_id,
                    request_id,
                    session_id,
                })
            }
            consts::TYPE_HOST_EXEC_TRY_READ => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                Ok(SchedulerMessage::HostExecTryRead {
                    worker_id,
                    request_id,
                    session_id,
                })
            }
            consts::TYPE_HOST_EXEC_POLL => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                Ok(SchedulerMessage::HostExecPoll {
                    worker_id,
                    request_id,
                    session_id,
                })
            }
            consts::TYPE_HOST_EXEC_SIGNAL => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                let signal = de.serde(consts::SIGNAL)?;
                Ok(SchedulerMessage::HostExecSignal {
                    worker_id,
                    request_id,
                    session_id,
                    signal,
                })
            }
            consts::TYPE_HOST_EXEC_CHILD_OUTPUT => {
                let worker_id = de.serde(consts::WORKER_ID)?;
                let request_id = de.serde(consts::REQUEST_ID)?;
                let session_id = de.serde(consts::SESSION_ID)?;
                let child_id = de.serde(consts::CHILD_ID)?;
                let msg_type = de.serde(consts::MSG_TYPE)?;
                let data_array: Uint8Array = de.js(consts::DATA)?;
                let data = data_array.to_vec();
                Ok(SchedulerMessage::HostExecChildOutput {
                    worker_id,
                    request_id,
                    session_id,
                    child_id,
                    msg_type,
                    data,
                })
            }
            other => {
                tracing::warn!(r#type = other, "Unknown message type");
                Err(anyhow::anyhow!("Unknown message type, \"{other}\"").into())
            }
        }
    }

    pub(crate) fn into_js(self) -> Result<JsValue, Error> {
        match self {
            SchedulerMessage::Close => Serializer::new(consts::TYPE_CLOSE).finish(),
            SchedulerMessage::SpawnAsync(task) => Serializer::new(consts::TYPE_SPAWN_ASYNC)
                .boxed(consts::PTR, task)
                .finish(),
            SchedulerMessage::SpawnBlocking(task) => Serializer::new(consts::TYPE_SPAWN_BLOCKING)
                .boxed(consts::PTR, task)
                .finish(),
            SchedulerMessage::WorkerIdle { worker_id } => Serializer::new(consts::TYPE_WORKER_IDLE)
                .set(consts::WORKER_ID, worker_id)
                .finish(),
            SchedulerMessage::WorkerBusy { worker_id } => Serializer::new(consts::TYPE_WORKER_BUSY)
                .set(consts::WORKER_ID, worker_id)
                .finish(),
            SchedulerMessage::CacheModule { hash, module } => {
                Serializer::new(consts::TYPE_CACHE_MODULE)
                    .set(consts::MODULE_HASH, hash.to_string())
                    .set(consts::MODULE, module)
                    .finish()
            }
            SchedulerMessage::SpawnWithModule { module, task } => {
                Serializer::new(consts::TYPE_SPAWN_WITH_MODULE)
                    .set(consts::MODULE, module)
                    .boxed(consts::PTR, task)
                    .finish()
            }
            SchedulerMessage::SpawnWithModuleAndMemory {
                module,
                memory,
                spawn_wasm,
            } => {
                let mut ser = Serializer::new(consts::TYPE_SPAWN_WITH_MODULE_AND_MEMORY)
                    .set(consts::MODULE, module)
                    .boxed(consts::PTR, spawn_wasm);

                if let Some(memory) = memory {
                    let store = wasmer::Store::default();
                    ser = ser.set(consts::MEMORY, memory.as_jsvalue(&store));
                }

                ser.finish()
            }
            SchedulerMessage::Markers { uninhabited, .. } => match uninhabited {},
            SchedulerMessage::HostExecStart {
                worker_id,
                request_id,
                request_json,
            } => {
                let request_array = Uint8Array::from(request_json.as_slice());
                Serializer::new(consts::TYPE_HOST_EXEC_START)
                    .set(consts::WORKER_ID, worker_id)
                    .set(consts::REQUEST_ID, request_id)
                    .set(consts::REQUEST_JSON, request_array)
                    .finish()
            }
            SchedulerMessage::HostExecRead {
                worker_id,
                request_id,
                session_id,
            } => Serializer::new(consts::TYPE_HOST_EXEC_READ)
                .set(consts::WORKER_ID, worker_id)
                .set(consts::REQUEST_ID, request_id)
                .set(consts::SESSION_ID, session_id)
                .finish(),
            SchedulerMessage::HostExecWrite {
                worker_id,
                request_id,
                session_id,
                data,
            } => {
                let data_array = Uint8Array::from(data.as_slice());
                Serializer::new(consts::TYPE_HOST_EXEC_WRITE)
                    .set(consts::WORKER_ID, worker_id)
                    .set(consts::REQUEST_ID, request_id)
                    .set(consts::SESSION_ID, session_id)
                    .set(consts::DATA, data_array)
                    .finish()
            }
            SchedulerMessage::HostExecCloseStdin {
                worker_id,
                request_id,
                session_id,
            } => Serializer::new(consts::TYPE_HOST_EXEC_CLOSE_STDIN)
                .set(consts::WORKER_ID, worker_id)
                .set(consts::REQUEST_ID, request_id)
                .set(consts::SESSION_ID, session_id)
                .finish(),
            SchedulerMessage::HostExecTryRead {
                worker_id,
                request_id,
                session_id,
            } => Serializer::new(consts::TYPE_HOST_EXEC_TRY_READ)
                .set(consts::WORKER_ID, worker_id)
                .set(consts::REQUEST_ID, request_id)
                .set(consts::SESSION_ID, session_id)
                .finish(),
            SchedulerMessage::HostExecPoll {
                worker_id,
                request_id,
                session_id,
            } => Serializer::new(consts::TYPE_HOST_EXEC_POLL)
                .set(consts::WORKER_ID, worker_id)
                .set(consts::REQUEST_ID, request_id)
                .set(consts::SESSION_ID, session_id)
                .finish(),
            SchedulerMessage::HostExecSignal {
                worker_id,
                request_id,
                session_id,
                signal,
            } => Serializer::new(consts::TYPE_HOST_EXEC_SIGNAL)
                .set(consts::WORKER_ID, worker_id)
                .set(consts::REQUEST_ID, request_id)
                .set(consts::SESSION_ID, session_id)
                .set(consts::SIGNAL, signal)
                .finish(),
            SchedulerMessage::HostExecChildOutput {
                worker_id,
                request_id,
                session_id,
                child_id,
                msg_type,
                data,
            } => {
                let data_array = Uint8Array::from(data.as_slice());
                Serializer::new(consts::TYPE_HOST_EXEC_CHILD_OUTPUT)
                    .set(consts::WORKER_ID, worker_id)
                    .set(consts::REQUEST_ID, request_id)
                    .set(consts::SESSION_ID, session_id)
                    .set(consts::CHILD_ID, child_id)
                    .set(consts::MSG_TYPE, msg_type)
                    .set(consts::DATA, data_array)
                    .finish()
            }
            // HostExecReadComplete is an internal message only sent via mpsc channel, never serialized
            SchedulerMessage::HostExecReadComplete { .. } => {
                unreachable!("HostExecReadComplete should not be serialized")
            }
        }
    }
}

mod consts {
    pub const TYPE_CLOSE: &str = "close";
    pub const TYPE_SPAWN_ASYNC: &str = "spawn-async";
    pub const TYPE_SPAWN_BLOCKING: &str = "spawn-blocking";
    pub const TYPE_WORKER_IDLE: &str = "worker-idle";
    pub const TYPE_WORKER_BUSY: &str = "worker-busy";
    pub const TYPE_CACHE_MODULE: &str = "cache-module";
    pub const TYPE_SPAWN_WITH_MODULE: &str = "spawn-with-module";
    pub const TYPE_SPAWN_WITH_MODULE_AND_MEMORY: &str = "spawn-with-module-and-memory";
    pub const TYPE_HOST_EXEC_START: &str = "host-exec-start";
    pub const TYPE_HOST_EXEC_READ: &str = "host-exec-read";
    pub const TYPE_HOST_EXEC_WRITE: &str = "host-exec-write";
    pub const TYPE_HOST_EXEC_CLOSE_STDIN: &str = "host-exec-close-stdin";
    pub const TYPE_HOST_EXEC_TRY_READ: &str = "host-exec-try-read";
    pub const TYPE_HOST_EXEC_POLL: &str = "host-exec-poll";
    pub const TYPE_HOST_EXEC_SIGNAL: &str = "host-exec-signal";
    pub const TYPE_HOST_EXEC_CHILD_OUTPUT: &str = "host-exec-child-output";
    pub const MEMORY: &str = "memory";
    pub const MODULE_HASH: &str = "module-hash";
    pub const MODULE: &str = "module";
    pub const PTR: &str = "ptr";
    pub const WORKER_ID: &str = "worker-id";
    pub const REQUEST_ID: &str = "request-id";
    pub const REQUEST_JSON: &str = "request-json";
    pub const SESSION_ID: &str = "session-id";
    pub const DATA: &str = "data";
    pub const SIGNAL: &str = "signal";
    pub const CHILD_ID: &str = "child-id";
    pub const MSG_TYPE: &str = "msg-type";
}
