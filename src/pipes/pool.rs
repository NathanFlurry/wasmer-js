//! Shared pipe buffer pool for cross-Worker communication.
//!
//! This module provides a SharedArrayBuffer-based pool for allocating pipe buffers.
//! The pool is created on the main thread (scheduler) and shared with all workers,
//! enabling true cross-worker communication even when processes fork.
//!
//! # Design
//!
//! The pool uses a simple bump allocator with atomic offset tracking:
//! - First 4 bytes: atomic allocation offset
//! - Remaining bytes: available for pipe buffers
//!
//! When a pipe is created, it atomically increments the offset and gets exclusive
//! access to that region of the buffer.

use js_sys::{Atomics, Int32Array, SharedArrayBuffer, Uint8Array};
use std::cell::RefCell;

/// Size of the shared pipe pool (16MB).
/// This supports many concurrent pipes.
pub const PIPE_POOL_SIZE: u32 = 16 * 1024 * 1024;

/// Header size at the start of the pool (allocation offset).
const POOL_HEADER_SIZE: u32 = 4;

/// Thread-local reference to the shared pipe pool.
/// This is set when the worker is initialized with the pool from the scheduler.
thread_local! {
    static PIPE_POOL: RefCell<Option<SharedArrayBuffer>> = RefCell::new(None);
    static PIPE_POOL_INT32_VIEW: RefCell<Option<Int32Array>> = RefCell::new(None);
}

/// Initialize the pipe pool for this thread.
/// Called by workers when they receive the pool from the scheduler.
pub fn init_pipe_pool(pool: SharedArrayBuffer) {
    let int32_view = Int32Array::new(&pool);
    PIPE_POOL.with(|p| {
        *p.borrow_mut() = Some(pool);
    });
    PIPE_POOL_INT32_VIEW.with(|v| {
        *v.borrow_mut() = Some(int32_view);
    });
}

/// Check if the pipe pool has been initialized.
pub fn is_pipe_pool_initialized() -> bool {
    PIPE_POOL.with(|p| p.borrow().is_some())
}

/// Get the SharedArrayBuffer for the pipe pool.
/// Panics if the pool hasn't been initialized.
pub fn get_pipe_pool() -> SharedArrayBuffer {
    PIPE_POOL.with(|p| {
        let pool = p.borrow();
        match pool.clone() {
            Some(p) => p,
            None => panic!("[get_pipe_pool] FATAL: Pipe pool not initialized - this is a bug!"),
        }
    })
}

/// Allocate a buffer from the pipe pool.
/// Returns the byte offset into the pool where the buffer starts.
/// Returns None if there's not enough space.
pub fn allocate_from_pool(size: u32) -> Option<u32> {
    PIPE_POOL_INT32_VIEW.with(|view_ref| {
        let view = view_ref.borrow();
        let view = view.as_ref()?;

        // Atomically try to allocate
        loop {
            let current_offset = Atomics::load(view, 0).unwrap_or(POOL_HEADER_SIZE as i32) as u32;

            // Check if we have enough space
            let new_offset = current_offset.checked_add(size)?;
            if new_offset > PIPE_POOL_SIZE {
                return None;
            }

            // Try to atomically claim this space
            let result = Atomics::compare_exchange(
                view,
                0,
                current_offset as i32,
                new_offset as i32,
            );

            match result {
                Ok(old_value) if old_value == current_offset as i32 => {
                    return Some(current_offset);
                }
                _ => {
                    // Someone else allocated, retry
                    continue;
                }
            }
        }
    })
}

/// Get a Uint8Array view into the pipe pool at the given offset.
/// Panics if the pool isn't initialized.
pub fn get_pool_uint8_view(offset: u32, length: u32) -> Option<Uint8Array> {
    PIPE_POOL.with(|pool_ref| {
        let pool = pool_ref.borrow();
        let pool = pool.as_ref().expect("[get_pool_uint8_view] FATAL: Pipe pool not initialized!");
        Some(Uint8Array::new_with_byte_offset_and_length(pool, offset, length))
    })
}

/// Get an Int32Array view into the pipe pool at the given offset.
/// The offset must be 4-byte aligned.
/// Panics if the pool isn't initialized.
pub fn get_pool_int32_view(offset: u32, length: u32) -> Option<Int32Array> {
    PIPE_POOL.with(|pool_ref| {
        let pool = pool_ref.borrow();
        let pool = pool.as_ref().expect("[get_pool_int32_view] FATAL: Pipe pool not initialized!");
        Some(Int32Array::new_with_byte_offset_and_length(pool, offset, length))
    })
}

/// Create the initial pipe pool on the main thread (scheduler).
/// This should be called once when the scheduler starts.
pub fn create_pipe_pool() -> SharedArrayBuffer {
    let pool = SharedArrayBuffer::new(PIPE_POOL_SIZE);

    // Initialize the allocation offset to start after the header
    let int32_view = Int32Array::new(&pool);
    let _ = Atomics::store(&int32_view, 0, POOL_HEADER_SIZE as i32);

    pool
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests would require browser/WASM environment
}
