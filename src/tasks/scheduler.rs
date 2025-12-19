use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    sync::{atomic::{AtomicU32, Ordering}, Mutex},
};

use anyhow::{Context, Error};
use js_sys::{Atomics, Int32Array, Uint8Array};
use once_cell::sync::Lazy;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::{self};
use tracing::Instrument;
use wasm_bindgen::{JsCast, JsValue};
use wasmer::AsJs;
use wasmer_wasix::runtime::HostExecRequest;
use wasmer_types::ModuleHash;
use serde_json;

use crate::tasks::{
    AsyncJob, BlockingJob, Notification, PostMessagePayload, SchedulerMessage, WorkerHandle,
    WorkerMessage,
};

// Thread-local storage for the host_exec handler (JS function cannot be Send+Sync)
// This is accessed from the main thread where the scheduler runs.
thread_local! {
    pub(crate) static HOST_EXEC_HANDLER: std::cell::RefCell<Option<js_sys::Function>> = std::cell::RefCell::new(None);
}

/// Message type constants for host_exec_read responses.
const MSG_TYPE_STDOUT: u32 = 1;
const MSG_TYPE_STDERR: u32 = 2;
const MSG_TYPE_EXIT: u32 = 3;

/// Message type constants for child process IPC (Node -> WASM via OUTPUT_QUEUES).
const MSG_TYPE_SPAWN_REQUEST: u32 = 10;
const MSG_TYPE_SPAWN_STDIN: u32 = 11;
const MSG_TYPE_SPAWN_CLOSE_STDIN: u32 = 12;
const MSG_TYPE_SPAWN_KILL: u32 = 13;

/// Message type constants for child process output (WASM -> Node via callbacks).
const MSG_TYPE_CHILD_STDOUT: u32 = 20;
const MSG_TYPE_CHILD_STDERR: u32 = 21;
const MSG_TYPE_CHILD_EXIT: u32 = 22;

/// Stores the exit code for completed host_exec sessions.
/// Key is session_id, value is exit code.
static HOST_EXEC_RESULTS: Lazy<Mutex<HashMap<u64, i32>>> = Lazy::new(|| Mutex::new(HashMap::new()));

/// Stores pending read requests waiting for session results.
/// Key is session_id, value is (worker_id, request_id).
static PENDING_READS: Lazy<Mutex<HashMap<u64, (u32, u64)>>> = Lazy::new(|| Mutex::new(HashMap::new()));

/// Stores output queues for streaming stdout/stderr.
/// Key is session_id, value is queue of (msg_type, data).
static OUTPUT_QUEUES: Lazy<Mutex<HashMap<u64, VecDeque<(u32, Vec<u8>)>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

/// Stores stdin writers for each session (thread-local since JS functions aren't Send+Sync).
/// Key is session_id, value is (writer_fn, closer_fn).
thread_local! {
    static STDIN_WRITERS: std::cell::RefCell<HashMap<u64, (js_sys::Function, js_sys::Function)>> = std::cell::RefCell::new(HashMap::new());
}

/// Stores kill/signal functions for each session (thread-local since JS functions aren't Send+Sync).
/// Key is session_id, value is kill_fn that takes a signal number.
thread_local! {
    static SIGNAL_HANDLERS: std::cell::RefCell<HashMap<u64, js_sys::Function>> = std::cell::RefCell::new(HashMap::new());
}

/// Stores child process output callbacks for each session (thread-local since JS functions aren't Send+Sync).
/// Key is session_id, value is (onChildStdout, onChildStderr, onChildExit).
thread_local! {
    static CHILD_OUTPUT_HANDLERS: std::cell::RefCell<HashMap<u64, (js_sys::Function, js_sys::Function, js_sys::Function)>> = std::cell::RefCell::new(HashMap::new());
}

/// Counter for generating unique child IDs within a session.
static NEXT_CHILD_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A handle for interacting with the threadpool's scheduler.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    scheduler_thread_id: u32,
    channel: UnboundedSender<SchedulerMessage>,
}

impl Scheduler {
    /// Spin up a scheduler on the current thread and get a channel that can be
    /// used to communicate with it.
    pub(crate) fn spawn() -> Scheduler {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let thread_id = wasmer::current_thread_id();
        // Safety: we just got the thread ID.
        let sender = unsafe { Scheduler::new(sender, thread_id) };

        let mut scheduler = SchedulerState::new(sender.clone());

        tracing::debug!(thread_id, "Spinning up the scheduler");
        wasm_bindgen_futures::spawn_local(
            async move {
                while let Some(msg) = receiver.recv().await {
                    tracing::trace!(?msg, "Executing a message");
                    if let SchedulerMessage::Close = msg {
                        break;
                    }
                    if let Err(e) = scheduler.execute(msg) {
                        tracing::error!(error = &*e, "An error occurred while handling a message");
                    }
                }

                tracing::debug!("Shutting down the scheduler");
                drop(scheduler);
            }
            .in_current_span()
            .instrument(tracing::debug_span!("scheduler", thread_id = thread_id)),
        );

        sender
    }

    /// # Safety
    ///
    /// The [`SchedulerMessage`] type is marked as `!Send` because
    /// [`wasmer::Module`] and friends are `!Send` when compiled for the
    /// browser.
    ///
    /// The `scheduler_thread_id` must match the [`wasmer::current_thread_id()`]
    /// otherwise these `!Send` values will be sent between threads.
    unsafe fn new(channel: UnboundedSender<SchedulerMessage>, scheduler_thread_id: u32) -> Self {
        debug_assert_eq!(scheduler_thread_id, wasmer::current_thread_id());
        tracing::debug!(current_thread = wasmer::current_thread_id(), "Creating Scheduler");
        Scheduler {
            channel,
            scheduler_thread_id,
        }
    }

    pub fn send(&self, msg: SchedulerMessage) -> Result<(), Error> {
        if wasmer::current_thread_id() == self.scheduler_thread_id {
            tracing::debug!(
                current_thread = wasmer::current_thread_id(),
                ?msg,
                "Sending message to scheduler"
            );
            // It's safe to send the message to the scheduler.
            self.channel
                .send(msg)
                .map_err(|_| Error::msg("Scheduler is dead"))?;
            Ok(())
        } else {
            // We are in a child worker so we need to emit the message via
            // postMessage() and let the WorkerHandle forward it to the
            // scheduler.
            WorkerMessage::Scheduler(msg)
                .emit()
                .map_err(|e| e.into_anyhow())?;
            Ok(())
        }
    }

    pub fn close(&self) {
        self.channel.send(SchedulerMessage::Close).unwrap();
    }

    pub fn is_closed(&self) -> bool {
        self.channel.is_closed()
    }
}

// Safety: The only way our !Send messages will be sent to the scheduler is if
// they are on the same thread. This is enforced via Scheduler::new()'s
// invariants.
unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

impl Drop for Scheduler {
    fn drop(&mut self) {
        tracing::debug!("Dropping Scheduler");
        // self.close();
    }
}

/// The state for the actor in charge of the threadpool.
#[derive(Debug)]
struct SchedulerState {
    /// Workers that are able to receive work.
    idle: VecDeque<WorkerHandle>,
    /// Workers that are currently blocked on synchronous operations and can't
    /// receive work at this time.
    busy: VecDeque<WorkerHandle>,
    /// A channel that can be used to send messages to this scheduler.
    mailbox: Scheduler,
    cached_modules: BTreeMap<ModuleHash, js_sys::WebAssembly::Module>,
}

impl SchedulerState {
    fn new(mailbox: Scheduler) -> Self {
        SchedulerState {
            idle: VecDeque::new(),
            busy: VecDeque::new(),
            mailbox,
            cached_modules: BTreeMap::new(),
        }
    }

    fn execute(&mut self, message: SchedulerMessage) -> Result<(), Error> {
        match message {
            SchedulerMessage::Close => {
                tracing::debug!("Scheduler received Close message");
                self.idle.clear();
                // self.busy.clear();
                Ok(())
            }
            SchedulerMessage::SpawnAsync(task) => {
                self.post_message(PostMessagePayload::Async(AsyncJob::Thunk(task)))
            }
            SchedulerMessage::SpawnBlocking(task) => {
                self.post_message(PostMessagePayload::Blocking(BlockingJob::Thunk(task)))
            }
            SchedulerMessage::CacheModule { hash, module } => {
                let module: js_sys::WebAssembly::Module = JsValue::from(module).unchecked_into();
                self.cached_modules.insert(hash, module.clone());

                for worker in self.idle.iter().chain(self.busy.iter()) {
                    worker.send(PostMessagePayload::Notification(
                        Notification::CacheModule {
                            hash,
                            module: module.clone(),
                        },
                    ))?;
                }

                Ok(())
            }
            SchedulerMessage::SpawnWithModule { module, task } => {
                self.post_message(PostMessagePayload::Blocking(BlockingJob::SpawnWithModule {
                    module: JsValue::from(module).unchecked_into(),
                    task,
                }))
            }
            SchedulerMessage::SpawnWithModuleAndMemory {
                module,
                memory,
                spawn_wasm,
            } => {
                let temp_store = wasmer::Store::default();
                let memory = memory.map(|m| m.as_jsvalue(&temp_store).dyn_into().unwrap());
                let module = JsValue::from(module).dyn_into().unwrap();

                self.post_message(PostMessagePayload::Blocking(
                    BlockingJob::SpawnWithModuleAndMemory {
                        module,
                        memory,
                        spawn_wasm,
                    },
                ))
            }
            SchedulerMessage::WorkerBusy { worker_id } => {
                move_worker(worker_id, &mut self.idle, &mut self.busy);
                tracing::trace!(
                    worker.id=worker_id,
                    idle_workers=?self.idle.iter().map(|w| w.id()).collect::<Vec<_>>(),
                    busy_workers=?self.busy.iter().map(|w| w.id()).collect::<Vec<_>>(),
                    "Worker marked as busy",
                );
                Ok(())
            }
            SchedulerMessage::WorkerIdle { worker_id } => {
                move_worker(worker_id, &mut self.busy, &mut self.idle);
                tracing::trace!(
                    worker.id=worker_id,
                    idle_workers=?self.idle.iter().map(|w| w.id()).collect::<Vec<_>>(),
                    busy_workers=?self.busy.iter().map(|w| w.id()).collect::<Vec<_>>(),
                    "Worker marked as idle",
                );
                Ok(())
            }
            SchedulerMessage::HostExecStart { worker_id, request_id, request_json } => {
                tracing::debug!(worker_id, request_id, "Received host_exec_start request");

                // Parse the request
                let request: HostExecRequest = match serde_json::from_slice(&request_json) {
                    Ok(r) => r,
                    Err(e) => {
                        let error = format!("Failed to parse request: {}", e);
                        return self.send_host_exec_start_response(worker_id, request_id, Err(error));
                    }
                };

                // Get the handler from thread-local
                let handler = HOST_EXEC_HANDLER.with(|h| h.borrow().clone());
                let handler = match handler {
                    Some(h) => h,
                    None => {
                        let error = "host_exec handler not registered".to_string();
                        return self.send_host_exec_start_response(worker_id, request_id, Err(error));
                    }
                };

                // Use request_id as session_id
                let session_id = request_id;

                // Create the context object for JS with streaming callbacks
                let context = create_host_exec_context(&request, session_id, self.mailbox.clone());

                // Call the JS handler
                let this = JsValue::NULL;
                let result = handler.call1(&this, &context);

                match result {
                    Ok(promise_value) => {
                        // The handler returned a Promise, convert and spawn async task
                        tracing::debug!(request_id, session_id, "Handler called successfully, spawning async task");

                        // Convert JsValue to Promise and spawn async task to await it
                        let promise: js_sys::Promise = match promise_value.dyn_into() {
                            Ok(p) => p,
                            Err(_) => {
                                let error = "Handler did not return a Promise".to_string();
                                return self.send_host_exec_start_response(worker_id, request_id, Err(error));
                            }
                        };

                        let mailbox = self.mailbox.clone();
                        web_sys::console::warn_1(&format!("[scheduler] spawning async task for session {}", session_id).into());
                        wasm_bindgen_futures::spawn_local(async move {
                            web_sys::console::warn_1(&format!("[scheduler] async task started for session {}", session_id).into());
                            let future = wasm_bindgen_futures::JsFuture::from(promise);
                            match future.await {
                                Ok(exit_code_value) => {
                                    let exit_code = exit_code_value.as_f64().unwrap_or(0.0) as i32;
                                    web_sys::console::warn_1(&format!("[scheduler] Promise resolved with exit_code={} for session {}", exit_code, session_id).into());

                                    // Store the result
                                    HOST_EXEC_RESULTS.lock().unwrap().insert(session_id, exit_code);

                                    // Check if there's a pending read request for this session
                                    let pending = PENDING_READS.lock().unwrap().remove(&session_id);
                                    web_sys::console::warn_1(&format!("[scheduler] pending read for session {}: {:?}", session_id, pending).into());
                                    if let Some((read_worker_id, read_session_id)) = pending {
                                        // Send the exit code to the waiting worker via SchedulerMessage
                                        // (will be handled by execute() which has access to workers)
                                        let exit_data = exit_code.to_le_bytes().to_vec();
                                        let msg = SchedulerMessage::HostExecReadComplete {
                                            worker_id: read_worker_id,
                                            request_id: read_session_id,  // Pass session_id through request_id
                                            msg_type: MSG_TYPE_EXIT,
                                            data: exit_data,
                                        };
                                        if let Err(e) = mailbox.send(msg) {
                                            web_sys::console::error_1(&format!("[scheduler] Failed to send read complete: {}", e).into());
                                        } else {
                                            web_sys::console::warn_1(&"[scheduler] sent HostExecReadComplete".into());
                                        }
                                    }
                                }
                                Err(e) => {
                                    web_sys::console::warn_1(&format!("[scheduler] Promise rejected for session {}: {:?}", session_id, e).into());
                                    // Store error as exit code 1
                                    HOST_EXEC_RESULTS.lock().unwrap().insert(session_id, 1);

                                    // Check if there's a pending read request
                                    if let Some((read_worker_id, read_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
                                        let exit_data = 1i32.to_le_bytes().to_vec();
                                        let msg = SchedulerMessage::HostExecReadComplete {
                                            worker_id: read_worker_id,
                                            request_id: read_session_id,  // Pass session_id through request_id
                                            msg_type: MSG_TYPE_EXIT,
                                            data: exit_data,
                                        };
                                        let _ = mailbox.send(msg);
                                    }
                                }
                            }
                        });

                        // Return session_id immediately
                        self.send_host_exec_start_response(worker_id, request_id, Ok(session_id))
                    }
                    Err(e) => {
                        let error = format!("Handler failed: {:?}", e);
                        self.send_host_exec_start_response(worker_id, request_id, Err(error))
                    }
                }
            }
            SchedulerMessage::HostExecRead { worker_id, request_id: _, session_id } => {
                web_sys::console::warn_1(&format!("[scheduler] HostExecRead worker={} session={}", worker_id, session_id).into());

                // First check if there's queued output data
                let queued_data = OUTPUT_QUEUES.lock().unwrap()
                    .get_mut(&session_id)
                    .and_then(|q| q.pop_front());

                if let Some((msg_type, data)) = queued_data {
                    // Send queued stdout/stderr data
                    web_sys::console::warn_1(&format!("[scheduler] Sending queued data for session {}: type={} len={}", session_id, msg_type, data.len()).into());
                    self.send_host_exec_read_response(worker_id, session_id, msg_type, data.len() as i32, Some(&data))
                } else if let Some(exit_code) = HOST_EXEC_RESULTS.lock().unwrap().remove(&session_id) {
                    // No more data, but process has exited - send exit code
                    web_sys::console::warn_1(&format!("[scheduler] Result ready for session {}: exit_code={}", session_id, exit_code).into());
                    // Clean up output queue
                    OUTPUT_QUEUES.lock().unwrap().remove(&session_id);
                    self.send_host_exec_read_response(worker_id, session_id, MSG_TYPE_EXIT, exit_code, None)
                } else {
                    // No data yet and process still running - store pending read request
                    web_sys::console::warn_1(&format!("[scheduler] Storing pending read for session {}", session_id).into());
                    PENDING_READS.lock().unwrap().insert(session_id, (worker_id, session_id));
                    Ok(())
                }
            }
            SchedulerMessage::HostExecReadComplete { worker_id, request_id: session_id, msg_type, data } => {
                // This is sent when an async Promise completes and a read was pending
                // request_id actually contains the session_id
                web_sys::console::warn_1(&format!("[scheduler] HostExecReadComplete worker={} session={} msg_type={}", worker_id, session_id, msg_type).into());
                // For exit, data contains the exit code as bytes; for stdout/stderr it's the data
                if msg_type == MSG_TYPE_EXIT && data.len() >= 4 {
                    let exit_code = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                    self.send_host_exec_read_response(worker_id, session_id, MSG_TYPE_EXIT, exit_code, None)
                } else {
                    self.send_host_exec_read_response(worker_id, session_id, msg_type, data.len() as i32, Some(&data))
                }
            }
            SchedulerMessage::HostExecWrite { worker_id, request_id, session_id, data } => {
                tracing::debug!(worker_id, request_id, session_id, data_len = data.len(), "Received host_exec_write request");

                // Get the stdin writer for this session and write the data
                STDIN_WRITERS.with(|writers| {
                    if let Some((writer_fn, _)) = writers.borrow().get(&session_id) {
                        // Convert data to Uint8Array and call the writer function
                        let uint8_array = js_sys::Uint8Array::from(data.as_slice());
                        let _ = writer_fn.call1(&JsValue::NULL, &uint8_array);
                    } else {
                        tracing::warn!(session_id, "No stdin writer found for session");
                    }
                });
                Ok(())
            }
            SchedulerMessage::HostExecCloseStdin { worker_id, request_id, session_id } => {
                tracing::debug!(worker_id, request_id, session_id, "Received host_exec_close_stdin request");

                // Get the stdin closer for this session, call it, and remove the entry
                STDIN_WRITERS.with(|writers| {
                    if let Some((_, closer_fn)) = writers.borrow_mut().remove(&session_id) {
                        let _ = closer_fn.call0(&JsValue::NULL);
                    } else {
                        tracing::warn!(session_id, "No stdin closer found for session");
                    }
                });
                Ok(())
            }
            SchedulerMessage::HostExecTryRead { worker_id, request_id: _, session_id } => {
                web_sys::console::warn_1(&format!("[scheduler] HostExecTryRead worker={} session={}", worker_id, session_id).into());

                // Check for queued output data (non-blocking)
                let queued_data = OUTPUT_QUEUES.lock().unwrap()
                    .get_mut(&session_id)
                    .and_then(|q| q.pop_front());

                if let Some((msg_type, data)) = queued_data {
                    // Send queued stdout/stderr data (status=1)
                    web_sys::console::warn_1(&format!("[scheduler] TryRead: sending queued data for session {}: type={} len={}", session_id, msg_type, data.len()).into());
                    self.send_host_exec_read_response(worker_id, session_id, msg_type, data.len() as i32, Some(&data))
                } else if let Some(exit_code) = HOST_EXEC_RESULTS.lock().unwrap().remove(&session_id) {
                    // No more data, but process has exited - send exit code (status=1)
                    web_sys::console::warn_1(&format!("[scheduler] TryRead: exit ready for session {}: exit_code={}", session_id, exit_code).into());
                    OUTPUT_QUEUES.lock().unwrap().remove(&session_id);
                    self.send_host_exec_read_response(worker_id, session_id, MSG_TYPE_EXIT, exit_code, None)
                } else {
                    // No data available - send status=2 (EAGAIN)
                    web_sys::console::warn_1(&format!("[scheduler] TryRead: no data for session {}", session_id).into());
                    self.send_host_exec_try_read_no_data(worker_id, session_id)
                }
            }
            SchedulerMessage::HostExecPoll { worker_id, request_id: _, session_id } => {
                web_sys::console::warn_1(&format!("[scheduler] HostExecPoll worker={} session={}", worker_id, session_id).into());

                // Check if there's data available (without consuming it)
                let has_queued_data = OUTPUT_QUEUES.lock().unwrap()
                    .get(&session_id)
                    .map(|q| !q.is_empty())
                    .unwrap_or(false);

                let has_exit_result = HOST_EXEC_RESULTS.lock().unwrap().contains_key(&session_id);

                let is_ready = has_queued_data || has_exit_result;
                web_sys::console::warn_1(&format!("[scheduler] Poll: session {} ready={}", session_id, is_ready).into());

                self.send_host_exec_poll_response(worker_id, session_id, is_ready)
            }
            SchedulerMessage::HostExecSignal { worker_id, request_id: _, session_id, signal } => {
                tracing::debug!(worker_id, session_id, signal, "Received host_exec_signal request");

                // Get the kill function for this session and call it with the signal
                SIGNAL_HANDLERS.with(|handlers| {
                    if let Some(kill_fn) = handlers.borrow().get(&session_id) {
                        let signal_val = JsValue::from_f64(signal as f64);
                        if let Err(e) = kill_fn.call1(&JsValue::NULL, &signal_val) {
                            tracing::warn!(session_id, ?e, "Failed to call kill function");
                        }
                    } else {
                        tracing::warn!(session_id, "No kill function found for session");
                    }
                });

                Ok(())
            }
            SchedulerMessage::HostExecChildOutput { worker_id: _, request_id: _, session_id: _, child_id, msg_type, data } => {
                web_sys::console::warn_1(&format!(
                    "[scheduler] HostExecChildOutput child_id={} msg_type={} data_len={}",
                    child_id, msg_type, data.len()
                ).into());

                // Look up the child output handlers by child_id and call appropriate callback
                // Callbacks expect (data) not (child_id, data) - child_id is implicit
                CHILD_OUTPUT_HANDLERS.with(|handlers| {
                    if let Some((stdout_fn, stderr_fn, exit_fn)) = handlers.borrow().get(&child_id) {
                        match msg_type {
                            MSG_TYPE_CHILD_STDOUT => {
                                let data_array = js_sys::Uint8Array::from(data.as_slice());
                                if let Err(e) = stdout_fn.call1(&JsValue::NULL, &data_array) {
                                    web_sys::console::warn_1(&format!(
                                        "[scheduler] Failed to call onChildStdout for child {}: {:?}",
                                        child_id, e
                                    ).into());
                                }
                            }
                            MSG_TYPE_CHILD_STDERR => {
                                let data_array = js_sys::Uint8Array::from(data.as_slice());
                                if let Err(e) = stderr_fn.call1(&JsValue::NULL, &data_array) {
                                    web_sys::console::warn_1(&format!(
                                        "[scheduler] Failed to call onChildStderr for child {}: {:?}",
                                        child_id, e
                                    ).into());
                                }
                            }
                            MSG_TYPE_CHILD_EXIT => {
                                // data contains exit code as 4 bytes
                                let exit_code = if data.len() >= 4 {
                                    i32::from_le_bytes([data[0], data[1], data[2], data[3]])
                                } else {
                                    0
                                };
                                web_sys::console::warn_1(&format!(
                                    "[scheduler] Calling onChildExit for child {} with code {}",
                                    child_id, exit_code
                                ).into());
                                let exit_code_val = JsValue::from_f64(exit_code as f64);
                                if let Err(e) = exit_fn.call1(&JsValue::NULL, &exit_code_val) {
                                    web_sys::console::warn_1(&format!(
                                        "[scheduler] Failed to call onChildExit for child {}: {:?}",
                                        child_id, e
                                    ).into());
                                }
                            }
                            _ => {
                                web_sys::console::warn_1(&format!(
                                    "[scheduler] Unknown child output message type {} for child {}",
                                    msg_type, child_id
                                ).into());
                            }
                        }
                    } else {
                        web_sys::console::warn_1(&format!(
                            "[scheduler] No child output handlers found for child_id {}",
                            child_id
                        ).into());
                    }
                });

                Ok(())
            }
            SchedulerMessage::Markers { uninhabited, .. } => match uninhabited {},
        }
    }

    fn send_host_exec_start_response(&mut self, worker_id: u32, request_id: u64, result: Result<u64, String>) -> Result<(), Error> {
        web_sys::console::warn_1(&format!("[scheduler] send_host_exec_start_response worker={} request={} result={:?}", worker_id, request_id, result).into());
        let msg = PostMessagePayload::HostExecStartResponse { request_id, result };
        let res = self.send_to_worker(worker_id, msg);
        web_sys::console::warn_1(&format!("[scheduler] send_to_worker result: {:?}", res.as_ref().map(|_| "ok").map_err(|e| e.to_string())).into());
        res
    }

    fn send_host_exec_read_response(&mut self, worker_id: u32, session_id: u64, msg_type: u32, data_or_exit_code: i32, data: Option<&[u8]>) -> Result<(), Error> {
        // Find the worker's SharedArrayBuffer
        for worker in self.idle.iter().chain(self.busy.iter()) {
            if worker.id() == worker_id {
                let int32_view = worker.host_exec_int32_view();

                // Buffer layout (Int32Array indices):
                //   [0]: status flag (0 = waiting, 1 = response ready)
                //   [1]: msg_type (1 = stdout, 2 = stderr, 3 = exit)
                //   [2]: data_len or exit_code
                //   [3]: session_id (for verification)
                //   [4+]: data bytes (if any)

                // Write msg_type
                Atomics::store(&int32_view, 1, msg_type as i32)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Write data_len or exit_code
                Atomics::store(&int32_view, 2, data_or_exit_code)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Write session_id (lower 32 bits for now)
                Atomics::store(&int32_view, 3, session_id as i32)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Write data bytes if present (starting at byte offset 16)
                if let Some(data) = data {
                    let buffer = int32_view.buffer();
                    let uint8_view = js_sys::Uint8Array::new(&buffer);
                    for (i, byte) in data.iter().enumerate() {
                        uint8_view.set_index((16 + i) as u32, *byte);
                    }
                }

                // Set status to 1 (response ready) - this must be done AFTER writing all data
                Atomics::store(&int32_view, 0, 1)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Wake up the worker
                web_sys::console::warn_1(&format!(
                    "[scheduler] calling Atomics.notify for worker {} session {}",
                    worker_id, session_id
                ).into());

                let notified = Atomics::notify(&int32_view, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::notify failed: {:?}", e))?;

                web_sys::console::warn_1(&format!(
                    "[scheduler] Atomics.notify woke {} waiters",
                    notified
                ).into());

                return Ok(());
            }
        }
        Err(anyhow::anyhow!("Worker {} not found", worker_id))
    }

    fn send_host_exec_try_read_no_data(&mut self, worker_id: u32, session_id: u64) -> Result<(), Error> {
        // Send status=2 (EAGAIN) to indicate no data available
        for worker in self.idle.iter().chain(self.busy.iter()) {
            if worker.id() == worker_id {
                let int32_view = worker.host_exec_int32_view();

                // Buffer layout:
                //   [0]: status flag (0 = waiting, 1 = response ready, 2 = no data / EAGAIN)
                //   [1]: msg_type (unused for EAGAIN)
                //   [2]: data_len (unused for EAGAIN)
                //   [3]: session_id

                // Write session_id
                Atomics::store(&int32_view, 3, session_id as i32)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Set status to 2 (EAGAIN)
                Atomics::store(&int32_view, 0, 2)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Wake up the worker
                Atomics::notify(&int32_view, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::notify failed: {:?}", e))?;

                return Ok(());
            }
        }
        Err(anyhow::anyhow!("Worker {} not found", worker_id))
    }

    fn send_host_exec_poll_response(&mut self, worker_id: u32, session_id: u64, is_ready: bool) -> Result<(), Error> {
        // Send poll response using status field
        for worker in self.idle.iter().chain(self.busy.iter()) {
            if worker.id() == worker_id {
                let int32_view = worker.host_exec_int32_view();

                // Buffer layout for poll:
                //   [0]: status flag (0 = waiting, 1 = ready, 2 = not ready)
                //   [3]: session_id

                // Write session_id
                Atomics::store(&int32_view, 3, session_id as i32)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Set status: 1 = ready, 2 = not ready
                let status = if is_ready { 1 } else { 2 };
                Atomics::store(&int32_view, 0, status)
                    .map_err(|e| anyhow::anyhow!("Atomics::store failed: {:?}", e))?;

                // Wake up the worker
                Atomics::notify(&int32_view, 0)
                    .map_err(|e| anyhow::anyhow!("Atomics::notify failed: {:?}", e))?;

                return Ok(());
            }
        }
        Err(anyhow::anyhow!("Worker {} not found", worker_id))
    }

    fn send_to_worker(&mut self, worker_id: u32, msg: PostMessagePayload) -> Result<(), Error> {
        // Find the worker in either idle or busy queue
        for worker in self.idle.iter().chain(self.busy.iter()) {
            if worker.id() == worker_id {
                return worker.send(msg);
            }
        }
        Err(anyhow::anyhow!("Worker {} not found", worker_id))
    }

    /// Send a task to one of the worker threads, preferring workers that aren't
    /// running synchronous work.
    fn post_message(&mut self, msg: PostMessagePayload) -> Result<(), Error> {
        let worker = self.next_available_worker()?;

        let would_block = msg.would_block();
        worker
            .send(msg)
            .with_context(|| format!("Unable to send a message to worker {}", worker.id()))?;

        if would_block {
            self.busy.push_back(worker);
        } else {
            self.idle.push_back(worker);
        }

        Ok(())
    }

    fn next_available_worker(&mut self) -> Result<WorkerHandle, Error> {
        // First, try to send the message to an idle worker
        if let Some(worker) = self.idle.pop_front() {
            tracing::trace!(
                worker.id = worker.id(),
                "Sending the message to an idle worker"
            );
            return Ok(worker);
        }

        // Rather than sending the task to one of the blocking workers,
        // let's spawn a new worker

        let worker = self.start_worker()?;
        tracing::trace!(
            worker.id = worker.id(),
            "Sending the message to a new worker"
        );
        Ok(worker)
    }

    fn start_worker(&mut self) -> Result<WorkerHandle, Error> {
        // Note: By using a monotonically incrementing counter, we can make sure
        // every single worker created with this shared linear memory will get a
        // unique ID.
        static NEXT_ID: AtomicU32 = AtomicU32::new(1);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let handle = WorkerHandle::spawn(id, self.mailbox.clone())?;

        // Prime the worker's module cache
        for (&hash, module) in &self.cached_modules {
            let msg = PostMessagePayload::Notification(Notification::CacheModule {
                hash,
                module: module.clone(),
            });
            handle.send(msg)?;
        }

        Ok(handle)
    }
}

fn move_worker(worker_id: u32, from: &mut VecDeque<WorkerHandle>, to: &mut VecDeque<WorkerHandle>) {
    if let Some(ix) = from.iter().position(|w| w.id() == worker_id) {
        let worker = from.remove(ix).unwrap();
        to.push_back(worker);
    }
}

/// Create the HostExecContext object for the JS handler.
/// Includes onStdout and onStderr callbacks for streaming output.
fn create_host_exec_context(request: &HostExecRequest, session_id: u64, scheduler: Scheduler) -> JsValue {
    use wasm_bindgen::prelude::Closure;

    // Create a JS object with the context
    let obj = js_sys::Object::new();

    // Set command
    js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("command"),
        &JsValue::from_str(&request.command),
    )
    .ok();

    // Set args
    let args = js_sys::Array::new();
    for arg in &request.args {
        args.push(&JsValue::from_str(arg));
    }
    js_sys::Reflect::set(&obj, &JsValue::from_str("args"), &args).ok();

    // Set env
    let env = js_sys::Object::new();
    for (key, value) in &request.env {
        js_sys::Reflect::set(
            &env,
            &JsValue::from_str(key),
            &JsValue::from_str(value),
        )
        .ok();
    }
    js_sys::Reflect::set(&obj, &JsValue::from_str("env"), &env).ok();

    // Set cwd
    js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("cwd"),
        &JsValue::from_str(&request.cwd),
    )
    .ok();

    // Set stdin to null for now
    js_sys::Reflect::set(&obj, &JsValue::from_str("stdin"), &JsValue::NULL).ok();
    js_sys::Reflect::set(&obj, &JsValue::from_str("stdout"), &JsValue::NULL).ok();
    js_sys::Reflect::set(&obj, &JsValue::from_str("stderr"), &JsValue::NULL).ok();

    // Create onStdout callback
    let stdout_scheduler = scheduler.clone();
    let on_stdout: Closure<dyn Fn(JsValue)> = Closure::new(move |data: JsValue| {
        if let Ok(array) = data.dyn_into::<js_sys::Uint8Array>() {
            let bytes = array.to_vec();

            // Check if there's a pending read request - if so, send immediately
            if let Some((worker_id, req_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
                // Send via message to trigger response (will be handled by execute())
                let msg = SchedulerMessage::HostExecReadComplete {
                    worker_id,
                    request_id: req_session_id,
                    msg_type: MSG_TYPE_STDOUT,
                    data: bytes,
                };
                let _ = stdout_scheduler.send(msg);
            } else {
                // No pending read, queue the data for later
                OUTPUT_QUEUES.lock().unwrap()
                    .entry(session_id)
                    .or_default()
                    .push_back((MSG_TYPE_STDOUT, bytes));
            }
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("onStdout"), &on_stdout.into_js_value()).ok();

    // Create onStderr callback
    let stderr_scheduler = scheduler.clone();
    let on_stderr: Closure<dyn Fn(JsValue)> = Closure::new(move |data: JsValue| {
        if let Ok(array) = data.dyn_into::<js_sys::Uint8Array>() {
            let bytes = array.to_vec();

            // Check if there's a pending read request - if so, send immediately
            if let Some((worker_id, req_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
                // Send via message to trigger response
                let msg = SchedulerMessage::HostExecReadComplete {
                    worker_id,
                    request_id: req_session_id,
                    msg_type: MSG_TYPE_STDERR,
                    data: bytes,
                };
                let _ = stderr_scheduler.send(msg);
            } else {
                // No pending read, queue the data for later
                OUTPUT_QUEUES.lock().unwrap()
                    .entry(session_id)
                    .or_default()
                    .push_back((MSG_TYPE_STDERR, bytes));
            }
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("onStderr"), &on_stderr.into_js_value()).ok();

    // Create setStdinWriter callback - handler calls this to register stdin writer functions
    let set_stdin_writer: Closure<dyn Fn(JsValue, JsValue)> = Closure::new(move |writer: JsValue, closer: JsValue| {
        if let (Ok(writer_fn), Ok(closer_fn)) = (writer.dyn_into::<js_sys::Function>(), closer.dyn_into::<js_sys::Function>()) {
            STDIN_WRITERS.with(|writers| {
                writers.borrow_mut().insert(session_id, (writer_fn, closer_fn));
            });
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("setStdinWriter"), &set_stdin_writer.into_js_value()).ok();

    // Create setKillFunction callback - handler calls this to register kill/signal function
    let set_kill_function: Closure<dyn Fn(JsValue)> = Closure::new(move |kill_fn: JsValue| {
        if let Ok(fn_obj) = kill_fn.dyn_into::<js_sys::Function>() {
            SIGNAL_HANDLERS.with(|handlers| {
                handlers.borrow_mut().insert(session_id, fn_obj);
            });
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("setKillFunction"), &set_kill_function.into_js_value()).ok();

    // Set terminal options if present
    if let Some(ref terminal) = request.terminal {
        let term_obj = js_sys::Object::new();
        js_sys::Reflect::set(&term_obj, &JsValue::from_str("term"), &JsValue::from_str(&terminal.term)).ok();
        js_sys::Reflect::set(&term_obj, &JsValue::from_str("cols"), &JsValue::from_f64(terminal.cols as f64)).ok();
        js_sys::Reflect::set(&term_obj, &JsValue::from_str("rows"), &JsValue::from_f64(terminal.rows as f64)).ok();
        js_sys::Reflect::set(&obj, &JsValue::from_str("terminal"), &term_obj).ok();
    }

    // Create requestSpawn callback - Node calls this to spawn a child process inside WASM
    // Parameters: childId, command, argsJson, envJson, cwd, onStdout, onStderr, onExit
    let spawn_scheduler = scheduler.clone();
    let request_spawn: Closure<dyn Fn(JsValue, JsValue, JsValue, JsValue, JsValue, JsValue, JsValue, JsValue)> = Closure::new(move |child_id_val: JsValue, command: JsValue, args_json: JsValue, env_json: JsValue, cwd: JsValue, on_stdout: JsValue, on_stderr: JsValue, on_exit: JsValue| {
        // Get child ID from JavaScript (already generated there)
        let child_id = child_id_val.as_f64().unwrap_or(0.0) as u64;

        web_sys::console::warn_1(&format!(
            "[scheduler] requestSpawn child_id={} command={:?}",
            child_id, command.as_string()
        ).into());

        // Store the output callbacks for this child, keyed by child_id
        // Each child has its own set of callbacks
        if let (Ok(stdout_fn), Ok(stderr_fn), Ok(exit_fn)) = (
            on_stdout.dyn_into::<js_sys::Function>(),
            on_stderr.dyn_into::<js_sys::Function>(),
            on_exit.dyn_into::<js_sys::Function>(),
        ) {
            CHILD_OUTPUT_HANDLERS.with(|handlers| {
                handlers.borrow_mut().insert(child_id, (stdout_fn, stderr_fn, exit_fn));
                web_sys::console::warn_1(&format!(
                    "[scheduler] Stored callbacks for child_id {}",
                    child_id
                ).into());
            });
        } else {
            web_sys::console::warn_1(&"[scheduler] requestSpawn: failed to get callback functions".into());
        }

        // Build spawn request JSON - parse args and env from JSON strings
        let spawn_request = js_sys::Object::new();
        js_sys::Reflect::set(&spawn_request, &JsValue::from_str("child_id"), &JsValue::from_f64(child_id as f64)).ok();
        js_sys::Reflect::set(&spawn_request, &JsValue::from_str("command"), &command).ok();
        // args_json is already a JSON string, parse it to get the array
        if let Ok(args_array) = js_sys::JSON::parse(&args_json.as_string().unwrap_or_default()) {
            js_sys::Reflect::set(&spawn_request, &JsValue::from_str("args"), &args_array).ok();
        }
        // env_json is already a JSON string, parse it to get the object
        if let Ok(env_obj) = js_sys::JSON::parse(&env_json.as_string().unwrap_or_default()) {
            js_sys::Reflect::set(&spawn_request, &JsValue::from_str("env"), &env_obj).ok();
        }
        js_sys::Reflect::set(&spawn_request, &JsValue::from_str("cwd"), &cwd).ok();

        // Serialize to JSON bytes
        let json_str = js_sys::JSON::stringify(&spawn_request)
            .map(|s| s.as_string().unwrap_or_default())
            .unwrap_or_default();
        let json_bytes = json_str.into_bytes();

        web_sys::console::warn_1(&format!(
            "[scheduler] requestSpawn queuing SPAWN_REQUEST len={}",
            json_bytes.len()
        ).into());

        // Queue spawn request for WASM to read
        OUTPUT_QUEUES.lock().unwrap()
            .entry(session_id)
            .or_default()
            .push_back((MSG_TYPE_SPAWN_REQUEST, json_bytes));

        // Check if there's a pending read - if so, send immediately
        if let Some((worker_id, req_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
            if let Some((msg_type, data)) = OUTPUT_QUEUES.lock().unwrap()
                .get_mut(&session_id)
                .and_then(|q| q.pop_front())
            {
                let msg = SchedulerMessage::HostExecReadComplete {
                    worker_id,
                    request_id: req_session_id,
                    msg_type,
                    data,
                };
                let _ = spawn_scheduler.send(msg);
            }
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("requestSpawn"), &request_spawn.into_js_value()).ok();

    // Create spawnWriteStdin callback - write data to a child's stdin
    let stdin_scheduler = scheduler.clone();
    let spawn_write_stdin: Closure<dyn Fn(JsValue, JsValue)> = Closure::new(move |child_id: JsValue, data: JsValue| {
        let child_id = child_id.as_f64().unwrap_or(0.0) as u64;

        if let Ok(array) = data.dyn_into::<js_sys::Uint8Array>() {
            // Build message: child_id (8 bytes) + data
            let mut msg_data = Vec::with_capacity(8 + array.length() as usize);
            msg_data.extend_from_slice(&child_id.to_le_bytes());
            msg_data.extend_from_slice(&array.to_vec());

            OUTPUT_QUEUES.lock().unwrap()
                .entry(session_id)
                .or_default()
                .push_back((MSG_TYPE_SPAWN_STDIN, msg_data));

            // Wake pending read if any
            if let Some((worker_id, req_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
                if let Some((msg_type, data)) = OUTPUT_QUEUES.lock().unwrap()
                    .get_mut(&session_id)
                    .and_then(|q| q.pop_front())
                {
                    let msg = SchedulerMessage::HostExecReadComplete {
                        worker_id,
                        request_id: req_session_id,
                        msg_type,
                        data,
                    };
                    let _ = stdin_scheduler.send(msg);
                }
            }
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("spawnWriteStdin"), &spawn_write_stdin.into_js_value()).ok();

    // Create spawnCloseStdin callback - close a child's stdin
    let close_scheduler = scheduler.clone();
    let spawn_close_stdin: Closure<dyn Fn(JsValue)> = Closure::new(move |child_id: JsValue| {
        let child_id = child_id.as_f64().unwrap_or(0.0) as u64;

        // Build message: just child_id (8 bytes)
        let msg_data = child_id.to_le_bytes().to_vec();

        OUTPUT_QUEUES.lock().unwrap()
            .entry(session_id)
            .or_default()
            .push_back((MSG_TYPE_SPAWN_CLOSE_STDIN, msg_data));

        // Wake pending read if any
        if let Some((worker_id, req_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
            if let Some((msg_type, data)) = OUTPUT_QUEUES.lock().unwrap()
                .get_mut(&session_id)
                .and_then(|q| q.pop_front())
            {
                let msg = SchedulerMessage::HostExecReadComplete {
                    worker_id,
                    request_id: req_session_id,
                    msg_type,
                    data,
                };
                let _ = close_scheduler.send(msg);
            }
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("spawnCloseStdin"), &spawn_close_stdin.into_js_value()).ok();

    // Create spawnKill callback - send signal to a child
    let kill_scheduler = scheduler.clone();
    let spawn_kill: Closure<dyn Fn(JsValue, JsValue)> = Closure::new(move |child_id: JsValue, signal: JsValue| {
        let child_id = child_id.as_f64().unwrap_or(0.0) as u64;
        let signal = signal.as_f64().unwrap_or(15.0) as u32; // Default SIGTERM

        // Build message: child_id (8 bytes) + signal (4 bytes)
        let mut msg_data = Vec::with_capacity(12);
        msg_data.extend_from_slice(&child_id.to_le_bytes());
        msg_data.extend_from_slice(&signal.to_le_bytes());

        OUTPUT_QUEUES.lock().unwrap()
            .entry(session_id)
            .or_default()
            .push_back((MSG_TYPE_SPAWN_KILL, msg_data));

        // Wake pending read if any
        if let Some((worker_id, req_session_id)) = PENDING_READS.lock().unwrap().remove(&session_id) {
            if let Some((msg_type, data)) = OUTPUT_QUEUES.lock().unwrap()
                .get_mut(&session_id)
                .and_then(|q| q.pop_front())
            {
                let msg = SchedulerMessage::HostExecReadComplete {
                    worker_id,
                    request_id: req_session_id,
                    msg_type,
                    data,
                };
                let _ = kill_scheduler.send(msg);
            }
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("spawnKill"), &spawn_kill.into_js_value()).ok();

    // Create setChildOutputHandlers callback - Node registers callbacks for child output
    let set_child_handlers: Closure<dyn Fn(JsValue, JsValue, JsValue)> = Closure::new(move |on_stdout: JsValue, on_stderr: JsValue, on_exit: JsValue| {
        if let (Ok(stdout_fn), Ok(stderr_fn), Ok(exit_fn)) = (
            on_stdout.dyn_into::<js_sys::Function>(),
            on_stderr.dyn_into::<js_sys::Function>(),
            on_exit.dyn_into::<js_sys::Function>(),
        ) {
            CHILD_OUTPUT_HANDLERS.with(|handlers| {
                handlers.borrow_mut().insert(session_id, (stdout_fn, stderr_fn, exit_fn));
            });
        }
    });
    js_sys::Reflect::set(&obj, &JsValue::from_str("setChildOutputHandlers"), &set_child_handlers.into_js_value()).ok();

    obj.into()
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::*;

    #[wasm_bindgen_test]
    async fn spawn_an_async_function() {
        let (sender, receiver) = oneshot::channel();
        let (tx, _) = mpsc::unbounded_channel();
        let tx = unsafe { Scheduler::new(tx, wasmer::current_thread_id()) };
        let mut scheduler = SchedulerState::new(tx);
        let message = SchedulerMessage::SpawnAsync(Box::new(move || {
            Box::pin(async move {
                let _ = sender.send(42);
            })
        }));

        // we start off with no workers
        assert_eq!(scheduler.idle.len(), 0);
        assert_eq!(scheduler.busy.len(), 0);

        // then we run the message, which should start up a worker and send it
        // the job
        scheduler.execute(message).unwrap();

        // One worker should have been created and added to the "ready" queue
        // because it's just handling async workloads.
        assert_eq!(scheduler.idle.len(), 1);
        assert_eq!(scheduler.busy.len(), 0);

        // Make sure the background thread actually ran something and sent us
        // back a result
        assert_eq!(receiver.await.unwrap(), 42);
    }
}
