use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};

use futures::future::BoxFuture;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use tokio::sync::mpsc;
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
    },
    VirtualTaskManager, WasiTtyState,
};

lazy_static! {
    /// We initialize the ThreadPool lazily
    static ref DEFAULT_THREAD_POOL: Arc<ThreadPool> = Arc::new(ThreadPool::new());
}

use crate::{tasks::ThreadPool, utils::Error};

/// A weak reference to the global [`Runtime`].
static GLOBAL_RUNTIME: Lazy<Mutex<Weak<Runtime>>> = Lazy::new(Mutex::default);

// Thread-local storage for the host_exec handler (JS function cannot be Send+Sync)
thread_local! {
    static HOST_EXEC_HANDLER: std::cell::RefCell<Option<js_sys::Function>> = std::cell::RefCell::new(None);
}

/// State for a host execution session.
#[derive(Debug)]
struct HostExecSessionState {
    /// Channel to send stdin data to JS handler.
    stdin_tx: mpsc::Sender<Vec<u8>>,
    /// Channel to receive output from JS handler.
    output_rx: tokio::sync::Mutex<mpsc::Receiver<HostExecOutput>>,
}

/// Host execution runtime implementation.
#[derive(Default, Debug)]
pub struct HostExecImpl {
    /// Active sessions.
    sessions: Mutex<HashMap<HostExecSession, Arc<HostExecSessionState>>>,
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
        let mut http_client = DefaultHttpClient::default();
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
        let mut http_client = DefaultHttpClient::default();
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
        HOST_EXEC_HANDLER.with(|h| {
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
        &self.task_manager.as_ref().expect("Task manager not found")
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
        // Get handler from thread-local
        let handler = HOST_EXEC_HANDLER.with(|h| h.borrow().clone());

        // Check handler exists first
        let handler = match handler {
            Some(h) => h,
            None => {
                return Box::pin(async { Err(anyhow::anyhow!("host_exec handler not registered")) });
            }
        };

        // Allocate session ID
        let session_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);

        // Create channels for I/O
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(32);
        let (output_tx, output_rx) = mpsc::channel::<HostExecOutput>(32);

        // Store session state
        let session_state = Arc::new(HostExecSessionState {
            stdin_tx,
            output_rx: tokio::sync::Mutex::new(output_rx),
        });
        self.sessions.lock().unwrap().insert(session_id, session_state);

        // Create the context object for JS
        let context = create_host_exec_context(&request, stdin_rx, output_tx);

        // Call the JS handler synchronously (it may return a promise)
        let this = wasm_bindgen::JsValue::NULL;
        let result = handler.call1(&this, &context);

        match result {
            Ok(_promise) => {
                // The handler returned - it may be a promise that resolves to exit code
                // For now, the exit code handling is done in the JS side
                Box::pin(async move { Ok(session_id) })
            }
            Err(e) => {
                // Clean up session on error
                self.sessions.lock().unwrap().remove(&session_id);
                // Format error message here to avoid capturing JsValue in async block
                let err_msg = format!("host_exec handler failed: {:?}", e);
                Box::pin(async move { Err(anyhow::anyhow!(err_msg)) })
            }
        }
    }

    fn host_exec_read(
        &self,
        session: HostExecSession,
    ) -> BoxFuture<'_, Result<HostExecOutput, anyhow::Error>> {
        // Get session state before entering async block
        let session_state = self
            .sessions
            .lock()
            .unwrap()
            .get(&session)
            .cloned();

        Box::pin(async move {
            let session_state = session_state
                .ok_or_else(|| anyhow::anyhow!("invalid session: {}", session))?;

            let mut rx = session_state.output_rx.lock().await;
            let output = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("session closed unexpectedly"))?;

            Ok(output)
        })
    }

    fn host_exec_write(
        &self,
        session: HostExecSession,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        // Get session state before entering async block
        let session_state = self
            .sessions
            .lock()
            .unwrap()
            .get(&session)
            .cloned();

        Box::pin(async move {
            let session_state = session_state
                .ok_or_else(|| anyhow::anyhow!("invalid session: {}", session))?;

            session_state
                .stdin_tx
                .send(data)
                .await
                .map_err(|_| anyhow::anyhow!("stdin channel closed"))?;

            Ok(())
        })
    }

    fn host_exec_close_stdin(
        &self,
        session: HostExecSession,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        // Get session state before entering async block
        let session_state = self
            .sessions
            .lock()
            .unwrap()
            .get(&session)
            .cloned();

        Box::pin(async move {
            // Verify the session exists
            let _session_state = session_state
                .ok_or_else(|| anyhow::anyhow!("invalid session: {}", session))?;

            // The stdin channel will be closed when all senders are dropped
            // For explicit close, we'd need to track this separately
            Ok(())
        })
    }
}

/// Create the HostExecContext object for the JS handler.
fn create_host_exec_context(
    request: &HostExecRequest,
    _stdin_rx: mpsc::Receiver<Vec<u8>>,
    _output_tx: mpsc::Sender<HostExecOutput>,
) -> wasm_bindgen::JsValue {
    // Create a JS object with the context
    let obj = js_sys::Object::new();

    // Set command
    js_sys::Reflect::set(
        &obj,
        &wasm_bindgen::JsValue::from_str("command"),
        &wasm_bindgen::JsValue::from_str(&request.command),
    )
    .ok();

    // Set args
    let args = js_sys::Array::new();
    for arg in &request.args {
        args.push(&wasm_bindgen::JsValue::from_str(arg));
    }
    js_sys::Reflect::set(&obj, &wasm_bindgen::JsValue::from_str("args"), &args).ok();

    // Set env
    let env = js_sys::Object::new();
    for (key, value) in &request.env {
        js_sys::Reflect::set(
            &env,
            &wasm_bindgen::JsValue::from_str(key),
            &wasm_bindgen::JsValue::from_str(value),
        )
        .ok();
    }
    js_sys::Reflect::set(&obj, &wasm_bindgen::JsValue::from_str("env"), &env).ok();

    // Set cwd
    js_sys::Reflect::set(
        &obj,
        &wasm_bindgen::JsValue::from_str("cwd"),
        &wasm_bindgen::JsValue::from_str(&request.cwd),
    )
    .ok();

    // TODO: Create Web Streams for stdin/stdout/stderr
    // For now, we'll set these to null and handle them later
    js_sys::Reflect::set(
        &obj,
        &wasm_bindgen::JsValue::from_str("stdin"),
        &wasm_bindgen::JsValue::NULL,
    )
    .ok();
    js_sys::Reflect::set(
        &obj,
        &wasm_bindgen::JsValue::from_str("stdout"),
        &wasm_bindgen::JsValue::NULL,
    )
    .ok();
    js_sys::Reflect::set(
        &obj,
        &wasm_bindgen::JsValue::from_str("stderr"),
        &wasm_bindgen::JsValue::NULL,
    )
    .ok();

    obj.into()
}

/// A [`Source`] that will always error out with [`QueryError::Unsupported`].
#[derive(Debug, Clone)]
struct UnsupportedSource;

#[async_trait::async_trait]
impl Source for UnsupportedSource {
    async fn query(&self, package: &PackageSource) -> Result<Vec<PackageSummary>, QueryError> {
        Err(QueryError::Unsupported { query: package.clone()})
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
