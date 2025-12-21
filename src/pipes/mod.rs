//! SharedArrayBuffer-based pipes for cross-Worker IPC.
//!
//! This module provides `SharedPipe` which uses SharedArrayBuffer + Atomics
//! for communication between Web Workers. This is necessary because tokio
//! channels cannot work across Web Workers (isolated memory spaces).
//!
//! # Design
//!
//! A SharedPipe uses a ring buffer in a shared pipe pool (a SharedArrayBuffer
//! created on the main thread and shared with all workers). This ensures pipes
//! work even when processes fork (which copies WASM memory but not the shared pool).
//!
//! ```text
//! Offset  Size   Field
//! ──────  ────   ─────
//! 0       4      write_pos (Atomics) - next write position
//! 4       4      read_pos (Atomics) - next read position
//! 8       4      closed flag (Atomics) - 0=open, 1=closed
//! 12      4      notify flag (Atomics.wait/notify)
//! 16      N      ring buffer data
//! ```
//!
//! The ring buffer uses producer-consumer semantics:
//! - Writer writes at write_pos, advances write_pos
//! - Reader reads at read_pos, advances read_pos
//! - Buffer full when (write_pos + 1) % capacity == read_pos
//! - Buffer empty when write_pos == read_pos
//!
//! # Cross-Worker Transfer
//!
//! Pipes are allocated from a shared pool that exists outside of WASM memory.
//! This means fork() doesn't affect the pipes - all workers access the same
//! underlying SharedArrayBuffer.

mod extract;
pub mod pool;
pub use extract::{extract_pipe_buffers, reconnect_pipe_buffers, PipeBufferMap};
pub use pool::{create_pipe_pool, init_pipe_pool, is_pipe_pool_initialized, get_pipe_pool, PIPE_POOL_SIZE};

use js_sys::{Atomics, Int32Array, SharedArrayBuffer, Uint8Array};
use std::io::{self, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

use virtual_fs::VirtualFile;

use pool::{allocate_from_pool, get_pool_int32_view, get_pool_uint8_view};

/// Header size in bytes (4 fields * 4 bytes each)
const HEADER_SIZE: usize = 16;

/// Default buffer size (64KB - header = ~64KB data)
pub const DEFAULT_PIPE_BUFFER_SIZE: u32 = 65536;

/// Offset for write position (in i32 units, not bytes)
const OFFSET_WRITE_POS: u32 = 0;
/// Offset for read position (in i32 units)
const OFFSET_READ_POS: u32 = 1;
/// Offset for closed flag (in i32 units)
const OFFSET_CLOSED: u32 = 2;
/// Offset for notify flag (in i32 units, used with Atomics.wait/notify)
const OFFSET_NOTIFY: u32 = 3;

/// A SharedArrayBuffer-based pipe for cross-Worker communication.
///
/// Note: SharedArrayBuffer can be shared across Workers, but the typed array views
/// (Int32Array, Uint8Array) must be created per-worker. This struct only stores
/// the pool offset and creates views on-demand.
#[derive(Debug)]
pub struct SharedPipe {
    tx: SharedPipeTx,
    rx: SharedPipeRx,
}

/// Transmit side of a SharedPipe.
///
/// Uses a shared pipe pool (SharedArrayBuffer) for the buffer, which is shared
/// across all workers regardless of fork() creating copied WASM memory.
#[derive(Debug)]
pub struct SharedPipeTx {
    /// Byte offset into the shared pipe pool.
    pool_offset: u32,
    /// Total size of the buffer including header.
    buffer_size: u32,
    /// Usable data capacity (buffer_size - HEADER_SIZE).
    data_capacity: u32,
}

/// Receive side of a SharedPipe.
///
/// Uses a shared pipe pool (SharedArrayBuffer) for the buffer.
#[derive(Debug)]
pub struct SharedPipeRx {
    /// Byte offset into the shared pipe pool.
    pool_offset: u32,
    /// Total size of the buffer including header.
    buffer_size: u32,
    /// Usable data capacity (buffer_size - HEADER_SIZE).
    data_capacity: u32,
}

// Safety: In WASM, each Worker runs single-threaded. The buffer lives in WASM
// linear memory which is shared between all workers. We use Atomics for
// synchronization of the header fields, and the ring buffer protocol ensures
// writers and readers don't access the same locations simultaneously.
unsafe impl Send for SharedPipe {}
unsafe impl Sync for SharedPipe {}
unsafe impl Send for SharedPipeTx {}
unsafe impl Sync for SharedPipeTx {}
unsafe impl Send for SharedPipeRx {}
unsafe impl Sync for SharedPipeRx {}

impl SharedPipeTx {
    /// Get the pool offset for this buffer.
    pub fn pool_offset(&self) -> u32 {
        self.pool_offset
    }

    /// Get the buffer size (including header).
    pub fn buffer_size(&self) -> u32 {
        self.buffer_size
    }

    /// Create a SharedPipeTx from a pool offset and size.
    /// Used when reconstructing from fork_pipes on the child worker.
    pub fn from_pool_offset(offset: u32, size: u32) -> Self {
        SharedPipeTx {
            pool_offset: offset,
            buffer_size: size,
            data_capacity: size - HEADER_SIZE as u32,
        }
    }

    /// Create an Int32Array view of the header for atomic operations.
    fn int32_view(&self) -> Int32Array {
        get_pool_int32_view(self.pool_offset, 4).expect("Pipe pool not initialized")
    }

    /// Create a Uint8Array view of the data portion.
    fn uint8_view(&self) -> Uint8Array {
        get_pool_uint8_view(
            self.pool_offset + HEADER_SIZE as u32,
            self.data_capacity,
        ).expect("Pipe pool not initialized")
    }
}

impl SharedPipeRx {
    /// Get the pool offset for this buffer.
    pub fn pool_offset(&self) -> u32 {
        self.pool_offset
    }

    /// Get the buffer size (including header).
    pub fn buffer_size(&self) -> u32 {
        self.buffer_size
    }

    /// Create a SharedPipeRx from a pool offset and size.
    /// Used when reconstructing from fork_pipes on the child worker.
    pub fn from_pool_offset(offset: u32, size: u32) -> Self {
        SharedPipeRx {
            pool_offset: offset,
            buffer_size: size,
            data_capacity: size - HEADER_SIZE as u32,
        }
    }

    /// Create an Int32Array view of the header for atomic operations.
    pub(crate) fn int32_view(&self) -> Int32Array {
        get_pool_int32_view(self.pool_offset, 4).expect("Pipe pool not initialized")
    }

    /// Create a Uint8Array view of the data portion.
    fn uint8_view(&self) -> Uint8Array {
        get_pool_uint8_view(
            self.pool_offset + HEADER_SIZE as u32,
            self.data_capacity,
        ).expect("Pipe pool not initialized")
    }
}

// Old methods removed - now using the int32_view/uint8_view methods defined above
impl SharedPipe {
    /// Create a new SharedPipe with the default buffer size.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_PIPE_BUFFER_SIZE)
    }

    /// Create a new SharedPipe with a custom buffer size.
    pub fn with_capacity(size: u32) -> Self {
        let data_capacity = size - HEADER_SIZE as u32;

        // Allocate buffer from the shared pipe pool.
        let pool_offset = allocate_from_pool(size)
            .expect("Failed to allocate from pipe pool - pool exhausted");

        // Create an Int32Array view for atomic initialization
        let int32_view = get_pool_int32_view(pool_offset, 4)
            .expect("Pipe pool not initialized");

        // Initialize header to zeros
        let _ = Atomics::store(&int32_view, OFFSET_WRITE_POS, 0);
        let _ = Atomics::store(&int32_view, OFFSET_READ_POS, 0);
        let _ = Atomics::store(&int32_view, OFFSET_CLOSED, 0);
        let _ = Atomics::store(&int32_view, OFFSET_NOTIFY, 0);

        let tx = SharedPipeTx {
            pool_offset,
            buffer_size: size,
            data_capacity,
        };

        let rx = SharedPipeRx {
            pool_offset,
            buffer_size: size,
            data_capacity,
        };

        SharedPipe { tx, rx }
    }

    /// Create a SharedPipe from a pool offset and size.
    ///
    /// This is used on the child worker side to connect to the parent's pipe.
    /// The offset points to an existing allocation in the shared pipe pool.
    pub fn from_pool_offset(offset: u32, size: u32) -> Self {
        let data_capacity = size - HEADER_SIZE as u32;

        let tx = SharedPipeTx {
            pool_offset: offset,
            buffer_size: size,
            data_capacity,
        };

        let rx = SharedPipeRx {
            pool_offset: offset,
            buffer_size: size,
            data_capacity,
        };

        SharedPipe { tx, rx }
    }

    /// Get the pool offset for this pipe's buffer.
    ///
    /// This offset can be passed to another Worker to establish communication.
    pub fn pool_offset(&self) -> u32 {
        self.tx.pool_offset()
    }

    /// Get the buffer size.
    pub fn buffer_size(&self) -> u32 {
        self.tx.buffer_size()
    }

    /// Split the pipe into separate transmit and receive ends.
    pub fn split(self) -> (SharedPipeTx, SharedPipeRx) {
        (self.tx, self.rx)
    }

    /// Combine separate tx and rx ends into a single pipe.
    pub fn combine(tx: SharedPipeTx, rx: SharedPipeRx) -> Self {
        SharedPipe { tx, rx }
    }

    /// Close the pipe.
    pub fn close(&mut self) {
        self.tx.close();
    }

    /// Check if the pipe is closed.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Try to read data without blocking.
    pub fn try_read(&mut self, buf: &mut [u8]) -> Option<usize> {
        self.rx.try_read(buf)
    }
}

impl Default for SharedPipe {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedPipeTx {
    /// Close the transmit end of the pipe.
    pub fn close(&mut self) {
        let int32_view = self.int32_view();
        let _ = Atomics::store(&int32_view, OFFSET_CLOSED, 1);
        // Notify any waiting readers
        let _ = Atomics::notify(&int32_view, OFFSET_NOTIFY);
    }

    /// Check if the pipe is closed.
    pub fn is_closed(&self) -> bool {
        let int32_view = self.int32_view();
        Atomics::load(&int32_view, OFFSET_CLOSED).unwrap_or(1) != 0
    }

    /// Get the amount of free space in the buffer.
    fn free_space(&self) -> usize {
        let int32_view = self.int32_view();
        let write_pos = Atomics::load(&int32_view, OFFSET_WRITE_POS).unwrap_or(0) as usize;
        let read_pos = Atomics::load(&int32_view, OFFSET_READ_POS).unwrap_or(0) as usize;
        let capacity = self.data_capacity as usize;

        if write_pos >= read_pos {
            capacity - (write_pos - read_pos) - 1
        } else {
            read_pos - write_pos - 1
        }
    }

    /// Write data to the pipe, returning the number of bytes written.
    pub fn write_data(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.is_closed() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Pipe is closed"));
        }

        if data.is_empty() {
            return Ok(0);
        }

        let free = self.free_space();
        if free == 0 {
            // Buffer is full, would need to wait
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Pipe buffer is full",
            ));
        }

        let int32_view = self.int32_view();
        let uint8_view = self.uint8_view();

        let to_write = data.len().min(free);
        let write_pos = Atomics::load(&int32_view, OFFSET_WRITE_POS).unwrap_or(0) as usize;
        let capacity = self.data_capacity as usize;

        // Write data in up to two chunks (for wrap-around)
        let first_chunk_size = (capacity - write_pos).min(to_write);
        let second_chunk_size = to_write - first_chunk_size;

        // First chunk: from write_pos to end of buffer (or end of data)
        // Note: uint8_view already starts at the data area (after HEADER_SIZE)
        for i in 0..first_chunk_size {
            uint8_view.set_index((write_pos + i) as u32, data[i]);
        }

        // Second chunk: from start of buffer (wrap-around)
        for i in 0..second_chunk_size {
            uint8_view.set_index(i as u32, data[first_chunk_size + i]);
        }

        // Update write position
        let new_write_pos = (write_pos + to_write) % capacity;
        let _ = Atomics::store(&int32_view, OFFSET_WRITE_POS, new_write_pos as i32);

        // Notify any waiting readers
        let _ = Atomics::notify(&int32_view, OFFSET_NOTIFY);

        Ok(to_write)
    }

}

impl SharedPipeRx {
    /// Close the receive end of the pipe.
    pub fn close(&mut self) {
        let int32_view = self.int32_view();
        let _ = Atomics::store(&int32_view, OFFSET_CLOSED, 1);
    }

    /// Check if the pipe is closed.
    pub fn is_closed(&self) -> bool {
        let int32_view = self.int32_view();
        Atomics::load(&int32_view, OFFSET_CLOSED).unwrap_or(1) != 0
    }

    /// Get the amount of data available in the buffer.
    fn data_available(&self) -> usize {
        let int32_view = self.int32_view();
        let write_pos = Atomics::load(&int32_view, OFFSET_WRITE_POS).unwrap_or(0) as usize;
        let read_pos = Atomics::load(&int32_view, OFFSET_READ_POS).unwrap_or(0) as usize;
        let capacity = self.data_capacity as usize;

        if write_pos >= read_pos {
            write_pos - read_pos
        } else {
            capacity - read_pos + write_pos
        }
    }

    /// Try to read data without blocking.
    pub fn try_read(&mut self, buf: &mut [u8]) -> Option<usize> {
        let available = self.data_available();
        if available == 0 {
            if self.is_closed() {
                return Some(0); // EOF
            }
            return None; // Would block
        }

        let int32_view = self.int32_view();
        let uint8_view = self.uint8_view();

        let to_read = buf.len().min(available);
        let read_pos = Atomics::load(&int32_view, OFFSET_READ_POS).unwrap_or(0) as usize;
        let capacity = self.data_capacity as usize;

        // Read data in up to two chunks (for wrap-around)
        let first_chunk_size = (capacity - read_pos).min(to_read);
        let second_chunk_size = to_read - first_chunk_size;

        // First chunk: from read_pos to end of buffer (or end of request)
        // Note: uint8_view already starts at the data area (after HEADER_SIZE)
        for i in 0..first_chunk_size {
            buf[i] = uint8_view.get_index((read_pos + i) as u32);
        }

        // Second chunk: from start of buffer (wrap-around)
        for i in 0..second_chunk_size {
            buf[first_chunk_size + i] = uint8_view.get_index(i as u32);
        }

        // Update read position
        let new_read_pos = (read_pos + to_read) % capacity;
        let _ = Atomics::store(&int32_view, OFFSET_READ_POS, new_read_pos as i32);

        Some(to_read)
    }

    /// Read data, blocking until data is available or pipe is closed.
    ///
    /// Uses Atomics.wait() for efficient blocking.
    pub fn read_blocking(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // Try to read non-blocking first
            if let Some(n) = self.try_read(buf) {
                return Ok(n);
            }

            // Check if closed
            if self.is_closed() {
                return Ok(0); // EOF
            }

            // Wait for notification using Atomics.wait
            let int32_view = self.int32_view();
            let current_notify = Atomics::load(&int32_view, OFFSET_NOTIFY).unwrap_or(0);

            // The writer will call Atomics.notify when data is written
            // We use wait with a small timeout to avoid missing notifications
            let _ = Atomics::wait_with_timeout(&int32_view, OFFSET_NOTIFY, current_notify, 100.0);

            // Loop back and try to read again
        }
    }

}

// Implement std::io traits for SharedPipe

impl Read for SharedPipe {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.rx.read(buf)
    }
}

impl Read for SharedPipeRx {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_blocking(buf)
    }
}

impl std::io::Write for SharedPipe {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tx.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl std::io::Write for SharedPipeTx {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_data(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for SharedPipe {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Ok(0)
    }
}

impl Seek for SharedPipeRx {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Ok(0)
    }
}

impl Seek for SharedPipeTx {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Ok(0)
    }
}

// Implement Tokio async traits

impl AsyncRead for SharedPipe {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.rx).poll_read(cx, buf)
    }
}

impl AsyncRead for SharedPipeRx {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Loop with Atomics.wait until data arrives or pipe closes
        let max_wait_iterations = 200; // 200 * 50ms = 10s max wait

        for _ in 0..max_wait_iterations {
            // Try to read non-blocking
            let temp_buf = buf.initialize_unfilled();
            if let Some(n) = self.try_read(temp_buf) {
                buf.advance(n);
                return Poll::Ready(Ok(()));
            }

            if self.is_closed() {
                return Poll::Ready(Ok(())); // EOF
            }

            // No data available - wait for notification
            let int32_view = self.int32_view();
            let current_notify = Atomics::load(&int32_view, OFFSET_NOTIFY).unwrap_or(0);

            // Wait up to 50ms for notification
            let _ = Atomics::wait_with_timeout(&int32_view, OFFSET_NOTIFY, current_notify, 50.0);
        }

        // Timeout - return Pending
        Poll::Pending
    }
}

impl AsyncWrite for SharedPipe {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.tx).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tx).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tx).poll_shutdown(cx)
    }
}

impl AsyncWrite for SharedPipeTx {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.write_data(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.close();
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for SharedPipe {
    fn start_seek(self: Pin<&mut Self>, _position: SeekFrom) -> io::Result<()> {
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(0))
    }
}

impl AsyncSeek for SharedPipeRx {
    fn start_seek(self: Pin<&mut Self>, _position: SeekFrom) -> io::Result<()> {
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(0))
    }
}

impl AsyncSeek for SharedPipeTx {
    fn start_seek(self: Pin<&mut Self>, _position: SeekFrom) -> io::Result<()> {
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(0))
    }
}

// Dummy AsyncRead for SharedPipeTx (write-only pipe cannot be read)
impl AsyncRead for SharedPipeTx {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Write-only pipe: return error
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Write-only pipe cannot be read")))
    }
}

// Dummy AsyncWrite for SharedPipeRx (read-only pipe cannot be written)
impl AsyncWrite for SharedPipeRx {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Read-only pipe: return error
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Read-only pipe cannot be written")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// Implement VirtualFile for SharedPipeTx (write-only)
impl VirtualFile for SharedPipeTx {
    fn last_accessed(&self) -> u64 { 0 }
    fn last_modified(&self) -> u64 { 0 }
    fn created_time(&self) -> u64 { 0 }
    fn size(&self) -> u64 { 0 }
    fn set_len(&mut self, _new_size: u64) -> virtual_fs::Result<()> { Ok(()) }
    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> { Ok(()) }
    fn is_open(&self) -> bool { !self.is_closed() }

    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // Write-only pipe cannot be read
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Write-only pipe")))
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let free = self.free_space();
        if free > 0 {
            Poll::Ready(Ok(free))
        } else if self.is_closed() {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "Pipe is closed")))
        } else {
            Poll::Pending
        }
    }
}

// Implement VirtualFile for SharedPipeRx (read-only)
impl VirtualFile for SharedPipeRx {
    fn last_accessed(&self) -> u64 { 0 }
    fn last_modified(&self) -> u64 { 0 }
    fn created_time(&self) -> u64 { 0 }
    fn size(&self) -> u64 { 0 }
    fn set_len(&mut self, _new_size: u64) -> virtual_fs::Result<()> { Ok(()) }
    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> { Ok(()) }
    fn is_open(&self) -> bool { !self.is_closed() }

    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // Since we can't easily register a cross-worker waker, we loop internally
        // with Atomics.wait until data arrives or the pipe is closed.
        // This makes poll_read_ready quasi-blocking but ensures pipes work cross-worker.
        let max_wait_iterations = 200; // 200 * 50ms = 10s max wait

        for _ in 0..max_wait_iterations {
            // Check if data is available
            let available = self.data_available();
            if available > 0 {
                return Poll::Ready(Ok(available));
            }

            if self.is_closed() {
                return Poll::Ready(Ok(0));
            }

            // No data available - wait for notification
            let int32_view = self.int32_view();
            let current_notify = Atomics::load(&int32_view, OFFSET_NOTIFY).unwrap_or(0);

            // Wait up to 50ms for notification
            let _ = Atomics::wait_with_timeout(&int32_view, OFFSET_NOTIFY, current_notify, 50.0);
        }

        // Timeout after max_wait_iterations - return Pending
        // (though in practice this means the pipe is stuck)
        Poll::Pending
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // Read-only pipe cannot be written
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Read-only pipe")))
    }
}

// Implement VirtualFile for SharedPipe

impl VirtualFile for SharedPipe {
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

    fn set_len(&mut self, _new_size: u64) -> virtual_fs::Result<()> {
        Ok(())
    }

    fn unlink(&mut self) -> Result<(), virtual_fs::FsError> {
        Ok(())
    }

    fn is_open(&self) -> bool {
        !self.is_closed()
    }

    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // Loop with Atomics.wait until data arrives or pipe closes
        let max_wait_iterations = 200; // 200 * 50ms = 10s max wait

        for _ in 0..max_wait_iterations {
            let available = self.rx.data_available();
            if available > 0 {
                return Poll::Ready(Ok(available));
            }

            if self.is_closed() {
                return Poll::Ready(Ok(0));
            }

            // No data available - wait for notification
            let int32_view = self.rx.int32_view();
            let current_notify = Atomics::load(&int32_view, OFFSET_NOTIFY).unwrap_or(0);

            // Wait up to 50ms for notification
            let _ = Atomics::wait_with_timeout(&int32_view, OFFSET_NOTIFY, current_notify, 50.0);
        }

        // Timeout - return Pending
        Poll::Pending
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let free = self.tx.free_space();
        if free > 0 {
            Poll::Ready(Ok(free))
        } else if self.is_closed() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Pipe is closed",
            )))
        } else {
            Poll::Pending
        }
    }
}

/// Stdio pipes for a subprocess, using SharedArrayBuffer for cross-Worker IPC.
#[derive(Debug)]
pub struct SharedStdioPipes {
    /// Stdin pipe (parent writes, child reads)
    pub stdin: SharedPipe,
    /// Stdout pipe (child writes, parent reads)
    pub stdout: SharedPipe,
    /// Stderr pipe (child writes, parent reads)
    pub stderr: SharedPipe,
}

impl SharedStdioPipes {
    /// Create new stdio pipes for a subprocess.
    pub fn new() -> Self {
        Self {
            stdin: SharedPipe::new(),
            stdout: SharedPipe::new(),
            stderr: SharedPipe::new(),
        }
    }

    /// Get the pool offsets and sizes for the child side.
    ///
    /// Returns ((stdin_offset, stdin_size), (stdout_offset, stdout_size), (stderr_offset, stderr_size)).
    /// The child will:
    /// - Read from stdin buffer at stdin_offset
    /// - Write to stdout buffer at stdout_offset
    /// - Write to stderr buffer at stderr_offset
    pub fn child_offsets(&self) -> ((u32, u32), (u32, u32), (u32, u32)) {
        (
            (self.stdin.pool_offset(), self.stdin.buffer_size()),
            (self.stdout.pool_offset(), self.stdout.buffer_size()),
            (self.stderr.pool_offset(), self.stderr.buffer_size()),
        )
    }

    /// Split into parent-side handles (for the spawning worker).
    ///
    /// Returns handles for:
    /// - Writing to child's stdin
    /// - Reading from child's stdout
    /// - Reading from child's stderr
    pub fn parent_handles(self) -> (SharedPipeTx, SharedPipeRx, SharedPipeRx) {
        let (stdin_tx, _stdin_rx) = self.stdin.split();
        let (_stdout_tx, stdout_rx) = self.stdout.split();
        let (_stderr_tx, stderr_rx) = self.stderr.split();
        (stdin_tx, stdout_rx, stderr_rx)
    }

    /// Create child-side pipes from pool offsets.
    ///
    /// This is used on the child worker to connect to the parent's pipes.
    pub fn child_from_offsets(
        stdin_offset: u32, stdin_size: u32,
        stdout_offset: u32, stdout_size: u32,
        stderr_offset: u32, stderr_size: u32,
    ) -> Self {
        Self {
            stdin: SharedPipe::from_pool_offset(stdin_offset, stdin_size),
            stdout: SharedPipe::from_pool_offset(stdout_offset, stdout_size),
            stderr: SharedPipe::from_pool_offset(stderr_offset, stderr_size),
        }
    }

    /// Split into child-side handles (for the spawned worker).
    ///
    /// Returns handles for:
    /// - Reading from stdin
    /// - Writing to stdout
    /// - Writing to stderr
    pub fn child_handles(self) -> (SharedPipeRx, SharedPipeTx, SharedPipeTx) {
        let (_stdin_tx, stdin_rx) = self.stdin.split();
        let (stdout_tx, _stdout_rx) = self.stdout.split();
        let (stderr_tx, _stderr_rx) = self.stderr.split();
        (stdin_rx, stdout_tx, stderr_tx)
    }
}

impl Default for SharedStdioPipes {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_pipe_basic_write_read() {
        let mut pipe = SharedPipe::new();

        // Write some data
        let data = b"hello world";
        let written = pipe.tx.write_data(data).unwrap();
        assert_eq!(written, data.len());

        // Read it back
        let mut buf = [0u8; 20];
        let read = pipe.rx.try_read(&mut buf).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf[..read], data);
    }

    #[test]
    fn test_shared_pipe_empty_read() {
        let mut pipe = SharedPipe::new();

        // Try to read from empty pipe
        let mut buf = [0u8; 20];
        let result = pipe.rx.try_read(&mut buf);
        assert!(result.is_none()); // Would block
    }

    #[test]
    fn test_shared_pipe_closed() {
        let mut pipe = SharedPipe::new();
        pipe.close();

        // Reading from closed pipe should return EOF
        let mut buf = [0u8; 20];
        let read = pipe.rx.try_read(&mut buf).unwrap();
        assert_eq!(read, 0);

        // Writing to closed pipe should fail
        let result = pipe.tx.write_data(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_shared_pipe_wrap_around() {
        let mut pipe = SharedPipe::with_capacity(32); // Small buffer for testing

        // Write data close to capacity
        let data1 = b"hello";
        pipe.tx.write_data(data1).unwrap();

        // Read it
        let mut buf = [0u8; 10];
        pipe.rx.try_read(&mut buf).unwrap();

        // Write more data that wraps around
        let data2 = b"world12345";
        let written = pipe.tx.write_data(data2).unwrap();
        assert!(written > 0);

        // Read the wrapped data
        let read = pipe.rx.try_read(&mut buf).unwrap();
        assert_eq!(&buf[..read], &data2[..read]);
    }
}
