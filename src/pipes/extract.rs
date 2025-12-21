//! Extract pool offsets from WasiEnv for cross-Worker transfer.
//!
//! When a WasiEnv is forked and spawned in a new Worker, any SharedPipe file
//! descriptors need their pool offsets passed to the child. Pipes use a shared
//! pool (SharedArrayBuffer) that is not affected by fork() copying WASM memory.

use std::collections::HashMap;

use wasmer_wasix::WasiEnv;
use wasmer_wasix::fs::Kind;

/// File descriptor type alias
type WasiFd = u32;

use super::{SharedPipeTx, SharedPipeRx};

/// A mapping of file descriptor to pool offset for cross-Worker transfer.
#[derive(Debug, Default)]
pub struct PipeBufferMap {
    /// Map from FD to (is_tx, pool_offset, buffer_size)
    /// is_tx = true for write end, false for read end
    pub buffers: HashMap<WasiFd, (bool, u32, u32)>,
    /// Debug info: all fds and their kinds (for logging on main thread)
    pub all_fds_debug: String,
}

impl PipeBufferMap {
    /// Create an empty buffer map.
    pub fn new() -> Self {
        Self { buffers: HashMap::new(), all_fds_debug: String::new() }
    }

    /// Check if there are any buffers to transfer.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }

    /// Get the buffers as a Vec for iteration.
    pub fn into_vec(self) -> Vec<(WasiFd, bool, u32, u32)> {
        self.buffers.into_iter()
            .map(|(fd, (is_tx, offset, size))| (fd, is_tx, offset, size))
            .collect()
    }
}

/// Extract pool offsets from VirtualPipe file descriptors in a WasiEnv.
///
/// This is needed when forking a process - the child needs the same pool
/// offsets as the parent since they all share the same pipe pool.
pub fn extract_pipe_buffers(env: &WasiEnv) -> PipeBufferMap {
    let mut result = PipeBufferMap::new();

    // Access the file descriptor map via the public fs() accessor
    let fs = env.fs();
    let fd_map = match fs.fd_map.read() {
        Ok(map) => map,
        Err(_) => return result,
    };

    for (fd, fd_entry) in fd_map.iter() {
        let kind = match fd_entry.inode.kind.read() {
            Ok(k) => k,
            Err(_) => continue,
        };

        match &*kind {
            Kind::VirtualPipeTx { tx } => {
                if let Ok(pipe_lock) = tx.read() {
                    let any_ref = pipe_lock.upcast_any_ref();
                    if let Some(shared_pipe) = any_ref.downcast_ref::<SharedPipeTx>() {
                        result.buffers.insert(fd, (true, shared_pipe.pool_offset(), shared_pipe.buffer_size()));
                    }
                }
            }
            Kind::VirtualPipeRx { rx } => {
                if let Ok(pipe_lock) = rx.read() {
                    let any_ref = pipe_lock.upcast_any_ref();
                    if let Some(shared_pipe) = any_ref.downcast_ref::<SharedPipeRx>() {
                        result.buffers.insert(fd, (false, shared_pipe.pool_offset(), shared_pipe.buffer_size()));
                    }
                }
            }
            _ => {}
        }
    }

    result
}

/// Reconnect pool offsets to VirtualPipe file descriptors after transfer.
///
/// This is called on the child Worker to reconnect the pipe file descriptors
/// using the same pool offsets as the parent (since the pipe pool is shared).
pub fn reconnect_pipe_buffers(env: &WasiEnv, buffers: PipeBufferMap) {
    // Check if pipe pool is initialized
    if !crate::pipes::is_pipe_pool_initialized() {
        panic!("reconnect_pipe_buffers: pipe pool not initialized");
    }

    let fs = env.fs();
    let fd_map = match fs.fd_map.read() {
        Ok(map) => map,
        Err(_) => panic!("reconnect_pipe_buffers: fd_map lock poisoned"),
    };

    for (fd, (is_tx, pool_offset, buffer_size)) in buffers.buffers {
        if let Some(fd_entry) = fd_map.get(fd) {
            let mut kind = match fd_entry.inode.kind.write() {
                Ok(k) => k,
                Err(_) => continue,
            };

            match &mut *kind {
                Kind::VirtualPipeTx { tx } if is_tx => {
                    if let Ok(mut pipe_lock) = tx.write() {
                        let new_pipe = SharedPipeTx::from_pool_offset(pool_offset, buffer_size);
                        *pipe_lock = Box::new(new_pipe);
                    }
                }
                Kind::VirtualPipeRx { rx } if !is_tx => {
                    if let Ok(mut pipe_lock) = rx.write() {
                        let new_pipe = SharedPipeRx::from_pool_offset(pool_offset, buffer_size);
                        *pipe_lock = Box::new(new_pipe);
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_env() {
        // Can't easily test without a full WasiEnv, but this at least compiles
    }
}
