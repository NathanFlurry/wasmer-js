//! Scheduler-routed pipes for cross-Worker IPC.
//!
//! All pipe I/O is routed through the main scheduler thread via postMessage.
//! This trades some latency for simplicity and correctness.

use std::io::{self, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use js_sys::Atomics;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use virtual_fs::VirtualFile;

use crate::tasks::thread_pool_worker::{CURRENT_WORKER_ID, HOST_EXEC_INT32_VIEW};
use crate::tasks::{SchedulerMessage, WorkerMessage};

/// Global counter for unique pipe IDs
static NEXT_PIPE_ID: AtomicU64 = AtomicU64::new(1);

/// A pipe that routes all I/O through the scheduler.
#[derive(Debug)]
pub struct SimplePipe {
    pipe_id: u64,
}

/// Transmit (write) end of a SimplePipe.
#[derive(Debug, Clone)]
pub struct SimplePipeTx {
    pipe_id: u64,
}

/// Receive (read) end of a SimplePipe.
#[derive(Debug, Clone)]
pub struct SimplePipeRx {
    pipe_id: u64,
}

unsafe impl Send for SimplePipe {}
unsafe impl Sync for SimplePipe {}
unsafe impl Send for SimplePipeTx {}
unsafe impl Sync for SimplePipeTx {}
unsafe impl Send for SimplePipeRx {}
unsafe impl Sync for SimplePipeRx {}

impl SimplePipe {
    /// Create a new pipe with a unique ID.
    pub fn new() -> Self {
        let pipe_id = NEXT_PIPE_ID.fetch_add(1, Ordering::SeqCst);

        // Register pipe with scheduler
        let worker_id = CURRENT_WORKER_ID.get().unwrap_or(0);
        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeCreate {
            pipe_id,
            worker_id,
        });
        let _ = msg.emit();

        SimplePipe { pipe_id }
    }

    /// Create a pipe handle from an existing ID (for child side after fork).
    pub fn from_id(pipe_id: u64) -> Self {
        SimplePipe { pipe_id }
    }

    pub fn id(&self) -> u64 {
        self.pipe_id
    }

    pub fn split(self) -> (SimplePipeTx, SimplePipeRx) {
        (
            SimplePipeTx { pipe_id: self.pipe_id },
            SimplePipeRx { pipe_id: self.pipe_id },
        )
    }

    /// Create TX handle without consuming self
    pub fn tx(&self) -> SimplePipeTx {
        SimplePipeTx { pipe_id: self.pipe_id }
    }

    /// Create RX handle without consuming self
    pub fn rx(&self) -> SimplePipeRx {
        SimplePipeRx { pipe_id: self.pipe_id }
    }
}

impl Default for SimplePipe {
    fn default() -> Self {
        Self::new()
    }
}

impl SimplePipeTx {
    pub fn id(&self) -> u64 {
        self.pipe_id
    }

    pub fn from_id(pipe_id: u64) -> Self {
        SimplePipeTx { pipe_id }
    }

    pub fn write_data(&self, data: &[u8]) -> io::Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        let worker_id = CURRENT_WORKER_ID.get().unwrap_or(0);
        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeWrite {
            pipe_id: self.pipe_id,
            worker_id,
            data: data.to_vec(),
        });

        msg.emit().map_err(|e| {
            io::Error::new(io::ErrorKind::BrokenPipe, format!("{:?}", e))
        })?;

        Ok(data.len())
    }

    pub fn close(&self) {
        let worker_id = CURRENT_WORKER_ID.get().unwrap_or(0);
        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeClose {
            pipe_id: self.pipe_id,
            worker_id,
        });
        let _ = msg.emit();
    }
}

impl SimplePipeRx {
    pub fn id(&self) -> u64 {
        self.pipe_id
    }

    pub fn from_id(pipe_id: u64) -> Self {
        SimplePipeRx { pipe_id }
    }

    /// Read data, blocking via Atomics.wait until data available or EOF.
    pub fn read_blocking(&self, buf: &mut [u8]) -> io::Result<usize> {
        let worker_id = CURRENT_WORKER_ID.get().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Worker ID not set")
        })?;

        let msg = WorkerMessage::Scheduler(SchedulerMessage::PipeRead {
            pipe_id: self.pipe_id,
            worker_id,
            max_len: buf.len() as u32,
        });

        msg.emit().map_err(|e| {
            io::Error::new(io::ErrorKind::BrokenPipe, format!("{:?}", e))
        })?;

        // Block using Atomics.wait until scheduler responds
        HOST_EXEC_INT32_VIEW.with(|view_cell| {
            let view_opt = view_cell.borrow();
            let view = view_opt.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "Host exec buffer not initialized")
            })?;

            // Reset status and wait for scheduler to notify us
            Atomics::store(view, 0, 0).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
            })?;
            let _ = Atomics::wait(view, 0, 0);

            // Read response: [0]=status, [1]=len, [16..]=data
            let status = Atomics::load(view, 0).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
            })?;
            let data_len = Atomics::load(view, 1).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
            })? as usize;

            match status {
                1 => {
                    // Data available
                    let sab = view.buffer();
                    let uint8_view = js_sys::Uint8Array::new(&sab);
                    let to_copy = data_len.min(buf.len());
                    for i in 0..to_copy {
                        buf[i] = uint8_view.get_index((64 + i) as u32);
                    }
                    Ok(to_copy)
                }
                2 => Ok(0), // EOF
                _ => Err(io::Error::new(io::ErrorKind::BrokenPipe, "Pipe error")),
            }
        })
    }
}

impl Drop for SimplePipeTx {
    fn drop(&mut self) {
        self.close();
    }
}

// std::io traits
impl Read for SimplePipeRx {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_blocking(buf)
    }
}

impl std::io::Write for SimplePipeTx {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_data(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for SimplePipeRx {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Ok(0)
    }
}

impl Seek for SimplePipeTx {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Ok(0)
    }
}

// Tokio async traits
impl AsyncRead for SimplePipeRx {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let temp_buf = buf.initialize_unfilled();
        match self.read_blocking(temp_buf) {
            Ok(n) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for SimplePipeTx {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.write_data(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.close();
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for SimplePipeRx {
    fn start_seek(self: Pin<&mut Self>, _: SeekFrom) -> io::Result<()> {
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(0))
    }
}

impl AsyncSeek for SimplePipeTx {
    fn start_seek(self: Pin<&mut Self>, _: SeekFrom) -> io::Result<()> {
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(0))
    }
}

// Wrong-direction stubs
impl AsyncRead for SimplePipeTx {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Write-only pipe",
        )))
    }
}

impl AsyncWrite for SimplePipeRx {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Read-only pipe",
        )))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// VirtualFile traits
impl VirtualFile for SimplePipeTx {
    fn last_accessed(&self) -> u64 {
        0
    }
    fn last_modified(&self) -> u64 {
        0
    }
    fn created_time(&self) -> u64 {
        0
    }
    fn size(&self) -> u64 {
        0
    }
    fn set_len(&mut self, _: u64) -> virtual_fs::Result<()> {
        Ok(())
    }
    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> {
        Ok(())
    }
    fn is_open(&self) -> bool {
        true
    }
    fn poll_read_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Write-only",
        )))
    }
    fn poll_write_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(65536))
    }
}

impl VirtualFile for SimplePipeRx {
    fn last_accessed(&self) -> u64 {
        0
    }
    fn last_modified(&self) -> u64 {
        0
    }
    fn created_time(&self) -> u64 {
        0
    }
    fn size(&self) -> u64 {
        0
    }
    fn set_len(&mut self, _: u64) -> virtual_fs::Result<()> {
        Ok(())
    }
    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> {
        Ok(())
    }
    fn is_open(&self) -> bool {
        true
    }
    fn poll_read_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        // Claim ready, actual read will block via Atomics.wait
        Poll::Ready(Ok(1))
    }
    fn poll_write_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Read-only",
        )))
    }
}
