use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, Weak,
};

use futures::future::BoxFuture;
use js_sys::Atomics;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use virtual_net::VirtualNetworking;
use wasmer_config::package::PackageSource;
use wasmer_wasix::{
    http::{HttpClient, WebHttpClient as DefaultHttpClient}, // reqwest::ReqwestHttpClient
    os::{TtyBridge, TtyOptions},
    runtime::{
        module_cache::ThreadLocalCache,
        package_loader::PackageLoader,
        resolver::{BackendSource, PackageSummary, QueryError, Source},
        DynHostExecRuntime, HostExecOutput, HostExecRequest, HostExecRuntime, HostExecSession,
        Signal,
    },
    VirtualTaskManager,
    WasiTtyState,
};

use crate::tasks::{
    thread_pool_worker::{CURRENT_WORKER_ID, HOST_EXEC_INT32_VIEW},
    SchedulerMessage, WorkerMessage,
};
use serde_json;

lazy_static! {
    /// We initialize the ThreadPool lazily
    static ref DEFAULT_THREAD_POOL: Arc<ThreadPool> = Arc::new(ThreadPool::new());
}

use crate::{tasks::ThreadPool, utils::Error};

/// A weak reference to the global [`Runtime`].
static GLOBAL_RUNTIME: Lazy<Mutex<Weak<Runtime>>> = Lazy::new(Mutex::default);

/// Host execution runtime implementation.
#[derive(Default, Debug)]
pub struct HostExecImpl {
    /// Counter for unique session IDs.
    next_session_id: AtomicU64,
}

/// Runtime components used when running WebAssembly programs.
#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
pub struct Runtime {
    task_manager: Option<Arc<dyn VirtualTaskManager>>,
    networking: Arc<dyn VirtualNetworking>,
    source: Option<Arc<BackendSource>>,
    http_client: Arc<dyn HttpClient + Send + Sync>,
    package_loader: Arc<crate::package_loader::PackageLoader>,
    module_cache: Arc<ThreadLocalCache>,
    tty: TtyOptions,
    connected_to_tty: Arc<AtomicBool>,
    #[derivative(Debug = "ignore")]
    host_exec: Arc<HostExecImpl>,
}

impl Runtime {
    /// Get a reference to the global runtime, if it has already been
    /// initialized.
    pub(crate) fn global() -> Option<Arc<Runtime>> {
        GLOBAL_RUNTIME.lock().ok()?.upgrade()
    }

    /// Get a reference to the global runtime, initializing it if it hasn't
    /// already been.
    pub(crate) fn lazily_initialized() -> Result<Arc<Self>, Error> {
        match GLOBAL_RUNTIME.lock() {
            Ok(mut guard) => match guard.upgrade() {
                Some(rt) => Ok(rt),
                None => {
                    tracing::debug!("Initializing the global runtime");
                    let rt = Arc::new(Runtime::with_defaults()?);
                    *guard = Arc::downgrade(&rt);

                    Ok(rt)
                }
            },
            Err(mut e) => {
                tracing::warn!("The global runtime lock was poisoned. Reinitializing.");

                let rt = Arc::new(Runtime::with_defaults()?);
                **e.get_mut() = Arc::downgrade(&rt);

                // FIXME: Use this when it becomes stable
                // GLOBAL_RUNTIME.clear_poison();

                Ok(rt)
            }
        }
    }

    pub(crate) fn with_defaults() -> Result<Self, Error> {
        let mut rt = Runtime::new();

        rt.set_registry(crate::DEFAULT_REGISTRY, None)?;

        Ok(rt)
    }

    pub(crate) fn with_task_manager(&self, task_manager: Arc<ThreadPool>) -> Self {
        let mut runtime = self.clone();
        // Update the http client
        let http_client = DefaultHttpClient::default();
        // http_client
        //     .with_default_header(
        //         reqwest::header::USER_AGENT,
        //         HeaderValue::from_static(crate::USER_AGENT),
        //     )
        //     .with_task_manager(task_manager.clone());
        runtime.http_client = Arc::new(http_client);

        runtime.task_manager = Some(task_manager);
        runtime
    }

    pub(crate) fn with_default_pool(&self) -> Self {
        // let pool = ThreadPool::new();
        self.with_task_manager(DEFAULT_THREAD_POOL.clone())
    }

    pub(crate) fn new() -> Self {
        let http_client = DefaultHttpClient::default();
        // http_client.with_default_header(
        //     reqwest::header::USER_AGENT,
        //     HeaderValue::from_static(crate::USER_AGENT),
        // );
        let http_client = Arc::new(http_client);

        let module_cache = ThreadLocalCache::default();
        let package_loader = crate::package_loader::PackageLoader::new(http_client.clone());

        Runtime {
            task_manager: None,
            networking: Arc::new(virtual_net::UnsupportedVirtualNetworking::default()),
            source: None,
            http_client: Arc::new(http_client),
            package_loader: Arc::new(package_loader),
            module_cache: Arc::new(module_cache),
            tty: TtyOptions::default(),
            connected_to_tty: Arc::new(AtomicBool::new(false)),
            host_exec: Arc::new(HostExecImpl::default()),
        }
    }

    /// Set the host_exec handler function.
    pub fn set_host_exec_handler(&self, handler: js_sys::Function) {
        // Store the handler in the scheduler's thread-local storage
        // (the scheduler runs on the main thread where this is called)
        crate::tasks::scheduler::HOST_EXEC_HANDLER.with(|h| {
            *h.borrow_mut() = Some(handler);
        });
    }

    /// Get a reference to the host_exec implementation.
    pub fn host_exec_impl(&self) -> &Arc<HostExecImpl> {
        &self.host_exec
    }

    /// Set the registry that packages will be fetched from.
    pub fn set_registry(&mut self, url: &str, token: Option<&str>) -> Result<(), Error> {
        let url = url.parse().map_err(Error::from)?;

        let mut source = BackendSource::new(url, self.http_client.clone());
        if let Some(token) = token {
            source = source.with_auth_token(token);
        }
        self.source = Some(Arc::new(source));

        Ok(())
    }

    /// Enable networking (i.e. TCP and UDP) via a gateway server.
    pub fn set_network_gateway(&mut self, gateway_url: String) {
        let networking = crate::net::connect_networking(gateway_url);
        self.networking = Arc::new(networking);
    }
}

impl Runtime {
    pub(crate) fn tty_options(&self) -> &TtyOptions {
        &self.tty
    }

    pub(crate) fn set_connected_to_tty(&self, state: bool) {
        self.connected_to_tty
            .store(state, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        tracing::debug!("Dropping Runtime");
    }
}

impl wasmer_wasix::runtime::Runtime for Runtime {
    fn networking(&self) -> &Arc<dyn VirtualNetworking> {
        &self.networking
    }

    fn task_manager(&self) -> &Arc<dyn VirtualTaskManager> {
        self.task_manager.as_ref().expect("Task manager not found")
    }

    fn source(&self) -> Arc<dyn wasmer_wasix::runtime::resolver::Source + Send + Sync> {
        match &self.source {
            Some(wapm) => Arc::clone(wapm) as _,
            None => Arc::new(UnsupportedSource),
        }
    }

    fn http_client(&self) -> Option<&wasmer_wasix::http::DynHttpClient> {
        Some(&self.http_client)
    }

    fn package_loader(&self) -> Arc<dyn PackageLoader + Send + Sync> {
        self.package_loader.clone()
    }

    fn module_cache(
        &self,
    ) -> Arc<dyn wasmer_wasix::runtime::module_cache::ModuleCache + Send + Sync> {
        self.module_cache.clone()
    }

    fn load_module_sync(&self, wasm: &[u8]) -> Result<wasmer::Module, wasmer_wasix::SpawnError> {
        let wasm = unsafe { js_sys::Uint8Array::view(wasm) };
        let module = js_sys::WebAssembly::Module::new(&wasm)
            .map_err(|x| wasmer_wasix::SpawnError::Other(crate::utils::js_error(x).into()))?;

        Ok(wasmer::Module::from((module, wasm.to_vec())))
    }

    fn tty(&self) -> Option<&(dyn wasmer_wasix::os::TtyBridge + Send + Sync)> {
        Some(self)
    }

    fn host_exec(&self) -> DynHostExecRuntime {
        self.host_exec.clone()
    }

    fn create_pipe(&self) -> wasmer_wasix::runtime::CreatedPipe {
        // Use SharedArrayBuffer-based pipes for cross-Worker IPC
        let pipe = crate::pipes::SharedPipe::new();
        let (tx, rx) = pipe.split();
        wasmer_wasix::runtime::CreatedPipe::Virtual {
            tx: Box::new(tx),
            rx: Box::new(rx),
        }
    }
}

impl TtyBridge for Runtime {
    #[tracing::instrument(level = "debug", skip_all)]
    fn reset(&self) {
        self.tty.set_echo(true);
        self.tty.set_line_buffering(true);
        self.tty.set_line_feeds(true);
        self.set_connected_to_tty(false);
    }

    #[tracing::instrument(level = "debug", skip(self), ret)]
    fn tty_get(&self) -> WasiTtyState {
        let connected_to_tty = self
            .connected_to_tty
            .load(std::sync::atomic::Ordering::SeqCst);

        WasiTtyState {
            cols: self.tty.cols(),
            rows: self.tty.rows(),
            width: 800,
            height: 600,
            stdin_tty: connected_to_tty,
            stdout_tty: connected_to_tty,
            stderr_tty: connected_to_tty,
            echo: self.tty.echo(),
            line_buffered: self.tty.line_buffering(),
            line_feeds: self.tty.line_feeds(),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn tty_set(&self, tty_state: WasiTtyState) {
        self.tty.set_cols(tty_state.cols);
        self.tty.set_rows(tty_state.rows);
        self.tty.set_echo(tty_state.echo);
        self.tty.set_line_buffering(tty_state.line_buffered);
        self.tty.set_line_feeds(tty_state.line_feeds);
        self.set_connected_to_tty(
            tty_state.stdin_tty || tty_state.stdout_tty || tty_state.stderr_tty,
        );
    }
}

impl HostExecRuntime for HostExecImpl {
    fn host_exec_start(
        &self,
        request: HostExecRequest,
    ) -> BoxFuture<'_, Result<HostExecSession, anyhow::Error>> {
        // Generate a unique session ID
        let session_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Get the current worker ID from thread-local storage
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_start called outside of worker thread"))
                });
            }
        };

        // Serialize the request
        let request_json = match serde_json::to_vec(&request) {
            Ok(json) => json,
            Err(e) => {
                let err_msg = format!("Failed to serialize request: {}", e);
                return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
            }
        };

        // Send the request to the scheduler (main thread)
        // We use session_id as request_id for simplicity
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecStart {
            worker_id,
            request_id: session_id,
            request_json,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec request: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Return session_id immediately without waiting
        // The actual result will be fetched by host_exec_read
        Box::pin(async move { Ok(session_id) })
    }

    fn host_exec_read(
        &self,
        session: HostExecSession,
    ) -> BoxFuture<'_, Result<HostExecOutput, anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_read called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Send read request to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecRead {
            worker_id,
            request_id,
            session_id: session,
        });

        web_sys::console::warn_1(&format!("[worker] sending HostExecRead request={} session={}", request_id, session).into());

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_read: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Now block using Atomics.wait() until the main thread writes the response
        // Buffer layout (Int32Array indices):
        //   [0]: status flag (0 = waiting, 1 = response ready)
        //   [1]: msg_type (1 = stdout, 2 = stderr, 3 = exit)
        //   [2]: data_len or exit_code
        //   [3]: session_id (for verification)
        //   [4+]: data bytes (packed into i32s)

        Box::pin(async move {
            HOST_EXEC_INT32_VIEW.with(|view_cell| {
                let view_opt = view_cell.borrow();
                let view = view_opt.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("host_exec buffer not initialized"))?;

                // Reset status to 0 (waiting) before blocking
                Atomics::store(view, 0, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                web_sys::console::warn_1(&format!("[worker] calling Atomics.wait for session={}", session).into());

                // Block until status becomes non-zero (response ready)
                // This is the key: Atomics.wait() blocks the thread but can be woken by Atomics.notify()
                let wait_result = Atomics::wait(view, 0, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::wait failed: {:?}", e))?;

                web_sys::console::warn_1(&format!("[worker] Atomics.wait returned: {:?}", wait_result).into());

                // Read the response from the buffer
                let msg_type = Atomics::load(view, 1)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))? as u32;
                let data_len_or_exit_code = Atomics::load(view, 2)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))?;
                let response_session = Atomics::load(view, 3)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))? as u64;

                web_sys::console::warn_1(&format!(
                    "[worker] read response: msg_type={}, data_len_or_exit={}, session={}",
                    msg_type, data_len_or_exit_code, response_session
                ).into());

                // Verify session matches
                if response_session != session {
                    return Err(anyhow::anyhow!(
                        "session mismatch: expected {}, got {}",
                        session, response_session
                    ));
                }

                // Convert to HostExecOutput based on msg_type
                match msg_type {
                    3 => {
                        // MSG_TYPE_EXIT
                        Ok(HostExecOutput::Exit(data_len_or_exit_code))
                    }
                    1 | 2 => {
                        // MSG_TYPE_STDOUT or MSG_TYPE_STDERR
                        let data_len = data_len_or_exit_code as usize;
                        let mut data = vec![0u8; data_len];

                        // Read data bytes from buffer starting at index 4 (byte offset 16)
                        // Use Uint8Array view for byte-level access
                        let buffer = view.buffer();
                        let uint8_view = js_sys::Uint8Array::new(&buffer);

                        // Data starts at byte offset 16 (4 * sizeof(i32))
                        for i in 0..data_len {
                            data[i] = uint8_view.get_index((16 + i) as u32) as u8;
                        }

                        if msg_type == 1 {
                            Ok(HostExecOutput::Stdout(data))
                        } else {
                            Ok(HostExecOutput::Stderr(data))
                        }
                    }
                    _ => {
                        // Unknown message type - pass through as Raw for WASM to handle
                        // This includes SPAWN_REQUEST (10), SPAWN_STDIN (11), etc.
                        let data_len = data_len_or_exit_code as usize;
                        let mut data = vec![0u8; data_len];

                        // Read data bytes from buffer starting at index 4 (byte offset 16)
                        let buffer = view.buffer();
                        let uint8_view = js_sys::Uint8Array::new(&buffer);

                        for i in 0..data_len {
                            data[i] = uint8_view.get_index((16 + i) as u32) as u8;
                        }

                        Ok(HostExecOutput::Raw { msg_type, data })
                    }
                }
            })
        })
    }

    fn host_exec_write(
        &self,
        session: HostExecSession,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_write called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Send write request to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecWrite {
            worker_id,
            request_id,
            session_id: session,
            data,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_write: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Return success immediately - write is fire-and-forget
        Box::pin(async move { Ok(()) })
    }

    fn host_exec_close_stdin(
        &self,
        session: HostExecSession,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_close_stdin called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Send close stdin request to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecCloseStdin {
            worker_id,
            request_id,
            session_id: session,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_close_stdin: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Return success immediately
        Box::pin(async move { Ok(()) })
    }

    fn host_exec_try_read(
        &self,
        session: HostExecSession,
    ) -> BoxFuture<'_, Result<Option<HostExecOutput>, anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_try_read called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Send poll request to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecTryRead {
            worker_id,
            request_id,
            session_id: session,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_try_read: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Block using Atomics.wait() until the main thread writes the response
        Box::pin(async move {
            HOST_EXEC_INT32_VIEW.with(|view_cell| {
                let view_opt = view_cell.borrow();
                let view = view_opt.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("host_exec buffer not initialized"))?;

                // Reset status to 0 (waiting) before blocking
                Atomics::store(view, 0, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Block until status becomes non-zero (response ready)
                let wait_result = Atomics::wait(view, 0, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::wait failed: {:?}", e))?;

                web_sys::console::warn_1(&format!("[worker] host_exec_try_read wait returned: {:?}", wait_result).into());

                // Read the response from the buffer
                // Status: 0 = waiting, 1 = data available, 2 = no data (EAGAIN)
                let status = Atomics::load(view, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))?;

                if status == 2 {
                    // No data available
                    return Ok(None);
                }

                let msg_type = Atomics::load(view, 1)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))? as u32;
                let data_len_or_exit_code = Atomics::load(view, 2)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))?;

                // Convert to HostExecOutput based on msg_type
                match msg_type {
                    3 => {
                        // MSG_TYPE_EXIT
                        Ok(Some(HostExecOutput::Exit(data_len_or_exit_code)))
                    }
                    1 | 2 => {
                        // MSG_TYPE_STDOUT or MSG_TYPE_STDERR
                        let data_len = data_len_or_exit_code as usize;
                        let mut data = vec![0u8; data_len];

                        // Read data bytes from buffer starting at index 4 (byte offset 16)
                        let buffer = view.buffer();
                        let uint8_view = js_sys::Uint8Array::new(&buffer);

                        for i in 0..data_len {
                            data[i] = uint8_view.get_index((16 + i) as u32) as u8;
                        }

                        if msg_type == 1 {
                            Ok(Some(HostExecOutput::Stdout(data)))
                        } else {
                            Ok(Some(HostExecOutput::Stderr(data)))
                        }
                    }
                    _ => {
                        // Unknown message type - pass through as Raw for WASM to handle
                        // This includes SPAWN_REQUEST (10), SPAWN_STDIN (11), etc.
                        let data_len = data_len_or_exit_code as usize;
                        let mut data = vec![0u8; data_len];

                        // Read data bytes from buffer starting at index 4 (byte offset 16)
                        let buffer = view.buffer();
                        let uint8_view = js_sys::Uint8Array::new(&buffer);

                        for i in 0..data_len {
                            data[i] = uint8_view.get_index((16 + i) as u32) as u8;
                        }

                        Ok(Some(HostExecOutput::Raw { msg_type, data }))
                    }
                }
            })
        })
    }

    fn host_exec_poll(
        &self,
        session: HostExecSession,
    ) -> BoxFuture<'_, Result<bool, anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_poll called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Send poll request to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecPoll {
            worker_id,
            request_id,
            session_id: session,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_poll: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Block using Atomics.wait() until the main thread writes the response
        Box::pin(async move {
            HOST_EXEC_INT32_VIEW.with(|view_cell| {
                let view_opt = view_cell.borrow();
                let view = view_opt.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("host_exec buffer not initialized"))?;

                // Reset status to 0 (waiting) before blocking
                Atomics::store(view, 0, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Block until status becomes non-zero (response ready)
                let _wait_result = Atomics::wait(view, 0, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::wait failed: {:?}", e))?;

                // Read the result from status field: 1 = data available, 2 = no data
                let status = Atomics::load(view, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::load failed: {:?}", e))?;

                // Status 1 means ready, status 2 means not ready
                Ok(status == 1)
            })
        })
    }

    fn host_exec_signal(
        &self,
        session: HostExecSession,
        sig: Signal,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_signal called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Convert Signal enum to u32
        let signal = sig as u32;

        // Send signal request to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecSignal {
            worker_id,
            request_id,
            session_id: session,
            signal,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_signal: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Return success immediately - signal is fire-and-forget
        Box::pin(async move { Ok(()) })
    }

    fn host_exec_child_output(
        &self,
        session: HostExecSession,
        child_id: u64,
        msg_type: u32,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        // Get current worker ID
        let worker_id = match CURRENT_WORKER_ID.get() {
            Some(id) => id,
            None => {
                return Box::pin(async move {
                    Err(anyhow::anyhow!("host_exec_child_output called outside of worker thread"))
                });
            }
        };

        let request_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Send child output to scheduler via postMessage
        let msg = WorkerMessage::Scheduler(SchedulerMessage::HostExecChildOutput {
            worker_id,
            request_id,
            session_id: session,
            child_id,
            msg_type,
            data,
        });

        if let Err(e) = msg.emit() {
            let err_msg = format!("Failed to send host_exec_child_output: {:?}", e);
            return Box::pin(async move { Err(anyhow::anyhow!(err_msg)) });
        }

        // Return success immediately - child output is fire-and-forget
        Box::pin(async move { Ok(()) })
    }
}

/// A [`Source`] that will always error out with [`QueryError::Unsupported`].
#[derive(Debug, Clone)]
struct UnsupportedSource;

#[async_trait::async_trait]
impl Source for UnsupportedSource {
    async fn query(&self, package: &PackageSource) -> Result<Vec<PackageSummary>, QueryError> {
        Err(QueryError::Unsupported {
            query: package.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasmer::Module;
    use wasmer_wasix::{Runtime as _, WasiEnvBuilder};

    use super::*;

    pub(crate) const TRIVIAL_WAT: &[u8] = br#"(
        module
            (memory $memory 0)
            (export "memory" (memory $memory))
            (func (export "_start") nop)
        )"#;

    #[wasm_bindgen_test]
    async fn execute_a_trivial_module() {
        let runtime = Runtime::with_defaults().unwrap().with_default_pool();
        // let module = runtime.load_module(TRIVIAL_WAT).await.unwrap();

        let module = Module::new(&runtime.engine(), TRIVIAL_WAT).unwrap();

        WasiEnvBuilder::new("trivial")
            .runtime(Arc::new(runtime))
            .run(module)
            .unwrap();
    }
}
