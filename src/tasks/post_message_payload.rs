use derivative::Derivative;
use js_sys::{SharedArrayBuffer, Uint8Array, WebAssembly};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;
use wasmer_types::ModuleHash;

use crate::tasks::{
    interop::Serializer, task_wasm::SpawnWasm, AsyncTask, BlockingModuleTask, BlockingTask,
};

/// Pool offsets for subprocess stdio.
///
/// These offsets point to SharedPipe buffers in the shared pipe pool.
/// The pool is a SharedArrayBuffer created on the main thread and shared
/// with all workers, so pipes work across fork() boundaries.
#[derive(Debug)]
pub(crate) struct SubprocessStdioBuffers {
    pub stdin_offset: u32,
    pub stdin_size: u32,
    pub stdout_offset: u32,
    pub stdout_size: u32,
    pub stderr_offset: u32,
    pub stderr_size: u32,
}

/// Pool offsets for pipes inherited during fork.
///
/// When a process forks (e.g., bash creating a pipe with `echo | cat`),
/// the SharedPipe file descriptors need their pool offsets passed
/// to the child worker. The pipe pool is shared across all workers,
/// so forked processes can communicate through pipes.
#[derive(Debug, Default)]
pub(crate) struct ForkPipeBuffers {
    /// Vec of (fd, is_tx, pool_offset, buffer_size) tuples
    /// is_tx = true for write end (VirtualPipeTx), false for read end (VirtualPipeRx)
    /// pool_offset is the byte offset into the shared pipe pool
    /// buffer_size is the total size of the buffer including header
    pub buffers: Vec<(u32, bool, u32, u32)>,
    /// Debug info: all fds and their kinds
    pub all_fds_debug: String,
}

/// A message that will be sent from the scheduler to a worker using
/// `postMessage()`.
#[derive(Debug)]
pub(crate) enum PostMessagePayload {
    Async(AsyncJob),
    Blocking(BlockingJob),
    Notification(Notification),
    /// Response to a host_exec_start request.
    HostExecStartResponse {
        request_id: u64,
        /// Ok(session_id) or Err(error_message)
        result: Result<u64, String>,
    },
    /// Response to a host_exec_read request.
    HostExecReadResponse {
        request_id: u64,
        /// (msg_type, data) or error
        result: Result<(u32, Vec<u8>), String>,
    },
}

impl PostMessagePayload {
    pub(crate) fn would_block(&self) -> bool {
        matches!(self, PostMessagePayload::Blocking(_))
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum BlockingJob {
    Thunk(#[derivative(Debug(format_with = "crate::utils::hidden"))] BlockingTask),
    SpawnWithModule {
        module: WebAssembly::Module,
        #[derivative(Debug(format_with = "crate::utils::hidden"))]
        task: BlockingModuleTask,
    },
    SpawnWithModuleAndMemory {
        module: WebAssembly::Module,
        /// An instance of the WebAssembly linear memory that has already been
        /// created.
        memory: Option<WebAssembly::Memory>,
        spawn_wasm: SpawnWasm,
        /// Optional SharedArrayBuffer pipes for subprocess stdio.
        /// When present, the child should use these instead of the WasiEnv pipes.
        subprocess_stdio: Option<SubprocessStdioBuffers>,
        /// SharedArrayBuffer pipes inherited from parent during fork.
        /// These are reconnected after the child WasiEnv is set up.
        fork_pipes: Option<ForkPipeBuffers>,
    },
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum AsyncJob {
    Thunk(#[derivative(Debug(format_with = "crate::utils::hidden"))] AsyncTask),
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum Notification {
    CacheModule {
        hash: ModuleHash,
        module: WebAssembly::Module,
    },
}

mod consts {
    pub(crate) const TYPE_SPAWN_ASYNC: &str = "spawn-async";
    pub(crate) const TYPE_SPAWN_BLOCKING: &str = "spawn-blocking";
    pub(crate) const TYPE_CACHE_MODULE: &str = "cache-module";
    pub(crate) const TYPE_SPAWN_WITH_MODULE: &str = "spawn-with-module";
    pub(crate) const TYPE_SPAWN_WITH_MODULE_AND_MEMORY: &str = "spawn-with-module-and-memory";
    pub(crate) const TYPE_HOST_EXEC_START_RESPONSE: &str = "host-exec-start-response";
    pub(crate) const TYPE_HOST_EXEC_READ_RESPONSE: &str = "host-exec-read-response";
    pub(crate) const PTR: &str = "ptr";
    pub(crate) const MODULE: &str = "module";
    pub(crate) const MEMORY: &str = "memory";
    pub(crate) const MODULE_HASH: &str = "module-hash";
    pub(crate) const REQUEST_ID: &str = "request-id";
    pub(crate) const SESSION_ID: &str = "session-id";
    pub(crate) const ERROR: &str = "error";
    pub(crate) const MSG_TYPE: &str = "msg-type";
    pub(crate) const DATA: &str = "data";
    // Subprocess stdio WASM memory offset keys
    pub(crate) const SUBPROCESS_STDIN_OFFSET: &str = "subprocess-stdin-offset";
    pub(crate) const SUBPROCESS_STDIN_SIZE: &str = "subprocess-stdin-size";
    pub(crate) const SUBPROCESS_STDOUT_OFFSET: &str = "subprocess-stdout-offset";
    pub(crate) const SUBPROCESS_STDOUT_SIZE: &str = "subprocess-stdout-size";
    pub(crate) const SUBPROCESS_STDERR_OFFSET: &str = "subprocess-stderr-offset";
    pub(crate) const SUBPROCESS_STDERR_SIZE: &str = "subprocess-stderr-size";
    // Fork pipe WASM memory offset keys
    pub(crate) const FORK_PIPE_COUNT: &str = "fork-pipe-count";
    pub(crate) const FORK_PIPE_FD_PREFIX: &str = "fork-pipe-fd-";
    pub(crate) const FORK_PIPE_IS_TX_PREFIX: &str = "fork-pipe-is-tx-";
    pub(crate) const FORK_PIPE_OFFSET_PREFIX: &str = "fork-pipe-offset-";
    pub(crate) const FORK_PIPE_SIZE_PREFIX: &str = "fork-pipe-size-";
}

impl PostMessagePayload {
    pub(crate) fn into_js(self) -> Result<JsValue, crate::utils::Error> {
        match self {
            PostMessagePayload::Async(AsyncJob::Thunk(task)) => {
                Serializer::new(consts::TYPE_SPAWN_ASYNC)
                    .boxed(consts::PTR, task)
                    .finish()
            }
            PostMessagePayload::Blocking(BlockingJob::Thunk(task)) => {
                Serializer::new(consts::TYPE_SPAWN_BLOCKING)
                    .boxed(consts::PTR, task)
                    .finish()
            }
            PostMessagePayload::Blocking(BlockingJob::SpawnWithModule { module, task }) => {
                Serializer::new(consts::TYPE_SPAWN_WITH_MODULE)
                    .boxed(consts::PTR, task)
                    .set(consts::MODULE, module)
                    .finish()
            }
            PostMessagePayload::Blocking(BlockingJob::SpawnWithModuleAndMemory {
                module,
                memory,
                spawn_wasm,
                subprocess_stdio,
                fork_pipes,
            }) => {
                let mut ser = Serializer::new(consts::TYPE_SPAWN_WITH_MODULE_AND_MEMORY)
                    .boxed(consts::PTR, spawn_wasm)
                    .set(consts::MODULE, module)
                    .set(consts::MEMORY, memory);

                // Add subprocess stdio WASM memory offsets if present
                if let Some(stdio) = subprocess_stdio {
                    ser = ser
                        .set(consts::SUBPROCESS_STDIN_OFFSET, stdio.stdin_offset as u32)
                        .set(consts::SUBPROCESS_STDIN_SIZE, stdio.stdin_size as u32)
                        .set(consts::SUBPROCESS_STDOUT_OFFSET, stdio.stdout_offset as u32)
                        .set(consts::SUBPROCESS_STDOUT_SIZE, stdio.stdout_size as u32)
                        .set(consts::SUBPROCESS_STDERR_OFFSET, stdio.stderr_offset as u32)
                        .set(consts::SUBPROCESS_STDERR_SIZE, stdio.stderr_size as u32);
                }

                // Add fork pipe WASM memory offsets if present
                if let Some(pipes) = fork_pipes {
                    ser = ser.set(consts::FORK_PIPE_COUNT, pipes.buffers.len() as u32);
                    for (i, (fd, is_tx, pool_offset, buffer_size)) in pipes.buffers.into_iter().enumerate() {
                        ser = ser
                            .set(&format!("{}{}", consts::FORK_PIPE_FD_PREFIX, i), fd)
                            .set(&format!("{}{}", consts::FORK_PIPE_IS_TX_PREFIX, i), is_tx)
                            .set(&format!("{}{}", consts::FORK_PIPE_OFFSET_PREFIX, i), pool_offset as u32)
                            .set(&format!("{}{}", consts::FORK_PIPE_SIZE_PREFIX, i), buffer_size as u32);
                    }
                }

                ser.finish()
            }
            PostMessagePayload::Notification(Notification::CacheModule { hash, module }) => {
                Serializer::new(consts::TYPE_CACHE_MODULE)
                    .set(consts::MODULE_HASH, hash.to_string())
                    .set(consts::MODULE, module)
                    .finish()
            }
            PostMessagePayload::HostExecStartResponse { request_id, result } => {
                let mut ser = Serializer::new(consts::TYPE_HOST_EXEC_START_RESPONSE)
                    .set(consts::REQUEST_ID, request_id);
                match result {
                    Ok(session_id) => ser = ser.set(consts::SESSION_ID, session_id),
                    Err(error) => ser = ser.set(consts::ERROR, error),
                }
                ser.finish()
            }
            PostMessagePayload::HostExecReadResponse { request_id, result } => {
                let mut ser = Serializer::new(consts::TYPE_HOST_EXEC_READ_RESPONSE)
                    .set(consts::REQUEST_ID, request_id);
                match result {
                    Ok((msg_type, data)) => {
                        let data_array = Uint8Array::from(data.as_slice());
                        ser = ser.set(consts::MSG_TYPE, msg_type);
                        ser = ser.set(consts::DATA, data_array);
                    }
                    Err(error) => ser = ser.set(consts::ERROR, error),
                }
                ser.finish()
            }
        }
    }

    /// Try to convert a [`PostMessagePayload`] back from a [`JsValue`].
    ///
    /// # Safety
    ///
    /// This can only be called if the original [`JsValue`] was created using
    /// [`PostMessagePayload::into_js()`].
    pub(crate) unsafe fn try_from_js(value: JsValue) -> Result<Self, crate::utils::Error> {
        let de = crate::tasks::interop::Deserializer::new(value);

        // Safety: Keep this in sync with PostMessagePayload::to_js()
        let msg_type = de.ty()?;
        match msg_type.as_str() {
            consts::TYPE_SPAWN_ASYNC => {
                let task = de.boxed(consts::PTR)?;
                Ok(PostMessagePayload::Async(AsyncJob::Thunk(task)))
            }
            consts::TYPE_SPAWN_BLOCKING => {
                let task = de.boxed(consts::PTR)?;
                Ok(PostMessagePayload::Blocking(BlockingJob::Thunk(task)))
            }
            consts::TYPE_CACHE_MODULE => {
                let module = de.js(consts::MODULE)?;
                let hash = de.string(consts::MODULE_HASH)?;
                let hash = if let Ok(hash) = ModuleHash::sha256_parse_hex(&hash) {
                    hash
                } else {
                    ModuleHash::xxhash_parse_hex(&hash)?
                };

                Ok(PostMessagePayload::Notification(
                    Notification::CacheModule { hash, module },
                ))
            }
            consts::TYPE_SPAWN_WITH_MODULE => {
                let task = de.boxed(consts::PTR)?;
                let module = de.js(consts::MODULE)?;

                Ok(PostMessagePayload::Blocking(BlockingJob::SpawnWithModule {
                    module,
                    task,
                }))
            }
            consts::TYPE_SPAWN_WITH_MODULE_AND_MEMORY => {
                let module = de.js(consts::MODULE)?;
                let memory = de.js(consts::MEMORY).ok();
                let spawn_wasm = de.boxed(consts::PTR)?;

                // Try to get subprocess stdio pool offsets
                let subprocess_stdio = match (
                    de.serde::<u32>(consts::SUBPROCESS_STDIN_OFFSET),
                    de.serde::<u32>(consts::SUBPROCESS_STDIN_SIZE),
                    de.serde::<u32>(consts::SUBPROCESS_STDOUT_OFFSET),
                    de.serde::<u32>(consts::SUBPROCESS_STDOUT_SIZE),
                    de.serde::<u32>(consts::SUBPROCESS_STDERR_OFFSET),
                    de.serde::<u32>(consts::SUBPROCESS_STDERR_SIZE),
                ) {
                    (Ok(stdin_offset), Ok(stdin_size), Ok(stdout_offset), Ok(stdout_size), Ok(stderr_offset), Ok(stderr_size)) => Some(SubprocessStdioBuffers {
                        stdin_offset,
                        stdin_size,
                        stdout_offset,
                        stdout_size,
                        stderr_offset,
                        stderr_size,
                    }),
                    _ => None,
                };

                // Try to get fork pipe pool offsets
                let fork_pipes = if let Ok(count) = de.serde::<u32>(consts::FORK_PIPE_COUNT) {
                    let mut buffers = Vec::with_capacity(count as usize);
                    for i in 0..count as usize {
                        let fd: u32 = de.serde(&format!("{}{}", consts::FORK_PIPE_FD_PREFIX, i))?;
                        let is_tx: bool = de.serde(&format!("{}{}", consts::FORK_PIPE_IS_TX_PREFIX, i))?;
                        let pool_offset: u32 = de.serde(&format!("{}{}", consts::FORK_PIPE_OFFSET_PREFIX, i))?;
                        let buffer_size: u32 = de.serde(&format!("{}{}", consts::FORK_PIPE_SIZE_PREFIX, i))?;
                        buffers.push((fd, is_tx, pool_offset, buffer_size));
                    }
                    Some(ForkPipeBuffers { buffers, all_fds_debug: String::new() })
                } else {
                    None
                };

                Ok(PostMessagePayload::Blocking(
                    BlockingJob::SpawnWithModuleAndMemory {
                        module,
                        memory,
                        spawn_wasm,
                        subprocess_stdio,
                        fork_pipes,
                    },
                ))
            }
            consts::TYPE_HOST_EXEC_START_RESPONSE => {
                let request_id = de.serde(consts::REQUEST_ID)?;
                let result = if let Ok(session_id) = de.serde::<u64>(consts::SESSION_ID) {
                    Ok(session_id)
                } else {
                    let error: String = de.serde(consts::ERROR)?;
                    Err(error)
                };
                Ok(PostMessagePayload::HostExecStartResponse { request_id, result })
            }
            consts::TYPE_HOST_EXEC_READ_RESPONSE => {
                let request_id = de.serde(consts::REQUEST_ID)?;
                let result = if let Ok(msg_type) = de.serde::<u32>(consts::MSG_TYPE) {
                    let data_array: Uint8Array = de.js(consts::DATA)?;
                    let data = data_array.to_vec();
                    Ok((msg_type, data))
                } else {
                    let error: String = de.serde(consts::ERROR)?;
                    Err(error)
                };
                Ok(PostMessagePayload::HostExecReadResponse { request_id, result })
            }
            other => Err(anyhow::anyhow!("Unknown message type: {other}").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use futures::channel::oneshot;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasmer::js::AsJs;
    use wasmer_wasix::{runtime::task_manager::TaskWasm, WasiEnvBuilder};

    use crate::{runtime::Runtime, tasks::SchedulerMessage};

    use super::*;

    #[wasm_bindgen_test]
    async fn round_trip_spawn_blocking() {
        let flag = Arc::new(AtomicBool::new(false));
        let msg = PostMessagePayload::Blocking(BlockingJob::Thunk({
            let flag = Arc::clone(&flag);
            Box::new(move || {
                flag.store(true, Ordering::SeqCst);
            })
        }));

        let js = msg.into_js().unwrap();
        let round_tripped = unsafe { PostMessagePayload::try_from_js(js).unwrap() };

        match round_tripped {
            PostMessagePayload::Blocking(BlockingJob::Thunk(task)) => {
                task();
                assert!(flag.load(Ordering::SeqCst));
            }
            _ => unreachable!(),
        }
    }

    #[wasm_bindgen_test]
    async fn round_trip_spawn_async() {
        let flag = Arc::new(AtomicBool::new(false));
        let msg = PostMessagePayload::Async(AsyncJob::Thunk({
            let flag = Arc::clone(&flag);
            Box::new(move || {
                Box::pin(async move {
                    flag.store(true, Ordering::SeqCst);
                })
            })
        }));

        let js = msg.into_js().unwrap();
        let round_tripped = unsafe { PostMessagePayload::try_from_js(js).unwrap() };

        match round_tripped {
            PostMessagePayload::Async(AsyncJob::Thunk(task)) => {
                task().await;
                assert!(flag.load(Ordering::SeqCst));
            }
            _ => unreachable!(),
        }
    }

    #[wasm_bindgen_test]
    async fn round_trip_spawn_with_module() {
        let wasm: &[u8] = include_bytes!("../../tests/envvar.wasm");
        let engine = wasmer::Engine::default();
        let module = wasmer::Module::new(&engine, wasm).unwrap();
        let (sender, receiver) = oneshot::channel();
        let msg = PostMessagePayload::Blocking(BlockingJob::SpawnWithModule {
            module: JsValue::from(module).dyn_into().unwrap(),
            task: Box::new(|m| {
                sender
                    .send(
                        m.exports()
                            .map(|e| e.name().to_string())
                            .collect::<Vec<String>>(),
                    )
                    .unwrap();
            }),
        });

        let js = msg.into_js().unwrap();
        let round_tripped = unsafe { PostMessagePayload::try_from_js(js).unwrap() };

        let (module, task) = match round_tripped {
            PostMessagePayload::Blocking(BlockingJob::SpawnWithModule { module, task }) => {
                (module, task)
            }
            _ => unreachable!(),
        };
        task(module.into());
        let name = receiver.await.unwrap();
        assert_eq!(
            name,
            vec![
                "memory".to_string(),
                "__heap_base".to_string(),
                "__data_end".to_string(),
                "_start".to_string(),
                "main".to_string()
            ]
        );
    }

    #[wasm_bindgen_test]
    async fn round_trip_cache_module() {
        let wasm: &[u8] = include_bytes!("../../tests/envvar.wasm");
        let engine = wasmer::Engine::default();
        let module = wasmer::Module::new(&engine, wasm).unwrap();
        let msg = PostMessagePayload::Notification(Notification::CacheModule {
            hash: ModuleHash::xxhash(wasm),
            module: module.into(),
        });

        let js = msg.into_js().unwrap();
        let round_tripped = unsafe { PostMessagePayload::try_from_js(js).unwrap() };

        match round_tripped {
            PostMessagePayload::Notification(Notification::CacheModule { hash, module: _ }) => {
                assert_eq!(hash, ModuleHash::xxhash(wasm));
            }
            _ => unreachable!(),
        };
    }

    #[wasm_bindgen_test]
    async fn round_trip_spawn_with_module_and_memory() {
        let wasm: &[u8] = include_bytes!("../../tests/envvar.wasm");
        let engine = wasmer::Engine::default();
        let module = wasmer::Module::new(&engine, wasm).unwrap();
        let flag = Arc::new(AtomicBool::new(false));
        let runtime = Runtime::new().with_default_pool();
        let env = WasiEnvBuilder::new("program")
            .runtime(Arc::new(runtime))
            .build()
            .unwrap();
        let msg = crate::tasks::task_wasm::to_scheduler_message(TaskWasm::new(
            Box::new({
                let flag = Arc::clone(&flag);
                move |_| {
                    flag.store(true, Ordering::SeqCst);
                }
            }),
            env,
            module,
            false,
        ))
        .unwrap();
        let msg = match msg {
            SchedulerMessage::SpawnWithModuleAndMemory {
                module,
                memory,
                spawn_wasm,
                subprocess_stdio,
                fork_pipes,
            } => PostMessagePayload::Blocking(BlockingJob::SpawnWithModuleAndMemory {
                module: module.into(),
                memory: memory.map(|m| m.as_jsvalue(&wasmer::Store::default()).dyn_into().unwrap()),
                spawn_wasm,
                subprocess_stdio,
                fork_pipes,
            }),
            _ => unreachable!(),
        };

        let js = msg.into_js().unwrap();
        let round_tripped = unsafe { PostMessagePayload::try_from_js(js).unwrap() };

        let (module, memory, spawn_wasm) = match round_tripped {
            PostMessagePayload::Blocking(BlockingJob::SpawnWithModuleAndMemory {
                module,
                memory,
                spawn_wasm,
                subprocess_stdio: _,
                fork_pipes: _,
            }) => (module, memory, spawn_wasm),
            _ => unreachable!(),
        };
        spawn_wasm
            .begin()
            .await
            .execute(module, memory.into())
            .await
            .unwrap();
        assert!(flag.load(Ordering::SeqCst));
    }
}
