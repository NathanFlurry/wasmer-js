//! Extract SharedArrayBuffers from WasiEnv for cross-Worker transfer.
//!
//! When a WasiEnv is forked and spawned in a new Worker, any SharedPipe file
//! descriptors need their SharedArrayBuffers explicitly transferred via postMessage.

use std::any::Any;
use std::collections::HashMap;

use js_sys::SharedArrayBuffer;
use wasmer_wasix::WasiEnv;
use wasmer_wasix::fs::Kind;

/// File descriptor type alias
type WasiFd = u32;

use super::{SharedPipeTx, SharedPipeRx};

/// A mapping of file descriptor to SharedArrayBuffer for cross-Worker transfer.
#[derive(Debug, Default)]
pub struct PipeBufferMap {
    /// Map from FD to (is_tx, SharedArrayBuffer)
    /// is_tx = true for write end, false for read end
    pub buffers: HashMap<WasiFd, (bool, SharedArrayBuffer)>,
}

impl PipeBufferMap {
    /// Create an empty buffer map.
    pub fn new() -> Self {
        Self { buffers: HashMap::new() }
    }

    /// Check if there are any buffers to transfer.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }

    /// Get the buffers as a Vec for iteration.
    pub fn into_vec(self) -> Vec<(WasiFd, bool, SharedArrayBuffer)> {
        self.buffers.into_iter()
            .map(|(fd, (is_tx, buf))| (fd, is_tx, buf))
            .collect()
    }
}

/// Extract SharedArrayBuffers from VirtualPipe file descriptors in a WasiEnv.
///
/// This is needed when forking a process - the child needs access to the same
/// SharedArrayBuffers as the parent for pipe communication.
pub fn extract_pipe_buffers(env: &WasiEnv) -> PipeBufferMap {
    let mut result = PipeBufferMap::new();

    // Access the file descriptor map via the public fs() accessor
    let fs = env.fs();
    let fd_map = match fs.fd_map.read() {
        Ok(map) => map,
        Err(_) => return result, // Lock poisoned, return empty
    };

    for (fd, fd_entry) in fd_map.iter() {
        // Get the inode kind
        let kind = match fd_entry.inode.kind.read() {
            Ok(k) => k,
            Err(_) => continue, // Skip if lock poisoned
        };

        match &*kind {
            Kind::VirtualPipeTx { tx } => {
                // Try to downcast to SharedPipeTx
                if let Ok(pipe_lock) = tx.read() {
                    let any_ref = pipe_lock.upcast_any_ref();
                    if let Some(shared_pipe) = any_ref.downcast_ref::<SharedPipeTx>() {
                        result.buffers.insert(fd, (true, shared_pipe.shared_buffer().clone()));
                    }
                }
            }
            Kind::VirtualPipeRx { rx } => {
                // Try to downcast to SharedPipeRx
                if let Ok(pipe_lock) = rx.read() {
                    let any_ref = pipe_lock.upcast_any_ref();
                    if let Some(shared_pipe) = any_ref.downcast_ref::<SharedPipeRx>() {
                        result.buffers.insert(fd, (false, shared_pipe.shared_buffer().clone()));
                    }
                }
            }
            _ => continue,
        }
    }

    result
}

/// Reconnect SharedArrayBuffers to VirtualPipe file descriptors after transfer.
///
/// This is called on the child Worker to reconnect the transferred SharedArrayBuffers
/// to the pipe file descriptors.
pub fn reconnect_pipe_buffers(env: &WasiEnv, buffers: PipeBufferMap) {
    let fs = env.fs();
    let fd_map = match fs.fd_map.read() {
        Ok(map) => map,
        Err(_) => return, // Lock poisoned
    };

    for (fd, (is_tx, buffer)) in buffers.buffers {
        if let Some(fd_entry) = fd_map.get(fd) {
            let mut kind = match fd_entry.inode.kind.write() {
                Ok(k) => k,
                Err(_) => continue,
            };

            match &mut *kind {
                Kind::VirtualPipeTx { tx } if is_tx => {
                    // Replace the inner SharedPipeTx with one using the transferred buffer
                    if let Ok(mut pipe_lock) = tx.write() {
                        // We need to replace the entire Box<dyn VirtualFile>
                        // Create a new SharedPipeTx from the buffer
                        let new_pipe = SharedPipeTx::from_buffer(buffer);
                        *pipe_lock = Box::new(new_pipe);
                    }
                }
                Kind::VirtualPipeRx { rx } if !is_tx => {
                    // Replace the inner SharedPipeRx with one using the transferred buffer
                    if let Ok(mut pipe_lock) = rx.write() {
                        let new_pipe = SharedPipeRx::from_buffer(buffer);
                        *pipe_lock = Box::new(new_pipe);
                    }
                }
                _ => continue,
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
