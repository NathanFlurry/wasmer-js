use anyhow::Context;
use futures::{channel::oneshot::Receiver, Stream, StreamExt, TryFutureExt};
use js_sys::Uint8Array;
use std::path::Path;
use virtual_fs::{AsyncReadExt, AsyncWriteExt, FileSystem, TmpFileSystem};
use wasm_bindgen::{closure::Closure, prelude::wasm_bindgen, JsCast, JsValue};
use wasmer_wasix::WasiRuntimeError;

use crate::utils::Error;

/// A handle connected to a running WASIX program.
#[derive(Debug)]
#[wasm_bindgen]
pub struct Instance {
    /// The standard input stream, if one wasn't provided when starting the
    /// instance.
    #[wasm_bindgen(getter_with_clone, readonly)]
    pub stdin: Option<web_sys::WritableStream>,
    /// The WASI program's standard output.
    #[wasm_bindgen(getter_with_clone, readonly)]
    pub stdout: web_sys::ReadableStream,
    /// The WASI program's standard error.
    #[wasm_bindgen(getter_with_clone, readonly)]
    pub stderr: web_sys::ReadableStream,
    pub(crate) exit: Receiver<ExitCondition>,
    /// The virtual filesystem for this instance.
    pub(crate) fs: TmpFileSystem,
}

#[wasm_bindgen]
impl Instance {
    /// Wait for the process to exit.
    #[wasm_bindgen(js_name = "wait")]
    pub async fn js_wait(self) -> Result<JsOutput, Error> {
        let output = self.wait().await?;
        Ok(output.into())
    }

    /// Read the contents of a file as binary data.
    #[wasm_bindgen(js_name = "vfsReadFile")]
    pub async fn vfs_read_file(&self, path: String) -> Result<Uint8Array, Error> {
        let buffer = self.read_file_internal(&path).await?;
        Ok(Uint8Array::from(&buffer[..]))
    }

    /// Read the contents of a file as UTF-8 text.
    #[wasm_bindgen(js_name = "vfsReadTextFile")]
    pub async fn vfs_read_text_file(&self, path: String) -> Result<js_sys::JsString, Error> {
        let buffer = self.read_file_internal(&path).await?;
        let string = String::from_utf8(buffer)
            .with_context(|| format!("File '{}' is not valid UTF-8", path))?;
        Ok(string.into())
    }

    /// Write binary data to a file, creating it if it doesn't exist, or truncating if it does.
    #[wasm_bindgen(js_name = "vfsWriteFile")]
    pub async fn vfs_write_file(&self, path: String, content: &[u8]) -> Result<(), Error> {
        let path_obj = Path::new(&path);

        // Create parent directories if they don't exist
        if let Some(parent) = path_obj.parent() {
            self.create_dir_all_internal(parent)
                .with_context(|| format!("Failed to create parent directories for '{}'", path))?;
        }

        let mut file = self
            .fs
            .new_open_options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path_obj)
            .with_context(|| format!("Failed to create '{}'", path))?;

        file.write_all(content)
            .await
            .with_context(|| format!("Failed to write '{}'", path))?;

        file.flush()
            .await
            .with_context(|| format!("Failed to flush '{}'", path))?;

        Ok(())
    }

    /// Write a UTF-8 string to a file.
    #[wasm_bindgen(js_name = "vfsWriteTextFile")]
    pub async fn vfs_write_text_file(&self, path: String, content: &str) -> Result<(), Error> {
        self.vfs_write_file(path, content.as_bytes()).await
    }

    /// Check if a path exists (file or directory).
    #[wasm_bindgen(js_name = "vfsExists")]
    pub fn vfs_exists(&self, path: &str) -> bool {
        self.fs.metadata(Path::new(path)).is_ok()
    }

    /// Get metadata about a file or directory.
    #[wasm_bindgen(js_name = "vfsStat")]
    pub fn vfs_stat(&self, path: &str) -> Result<JsValue, Error> {
        let path_obj = Path::new(path);
        let metadata = self
            .fs
            .metadata(path_obj)
            .with_context(|| format!("Failed to stat '{}'", path))?;

        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &"isFile".into(), &metadata.is_file().into()).unwrap();
        js_sys::Reflect::set(&obj, &"isDir".into(), &metadata.is_dir().into()).unwrap();
        js_sys::Reflect::set(
            &obj,
            &"size".into(),
            &JsValue::from_f64(metadata.len() as f64),
        )
        .unwrap();

        Ok(obj.into())
    }

    /// List contents of a directory.
    #[wasm_bindgen(js_name = "vfsReadDir")]
    pub fn vfs_read_dir(&self, path: &str) -> Result<js_sys::Array, Error> {
        let path_obj = Path::new(path);
        let entries = self
            .fs
            .read_dir(path_obj)
            .with_context(|| format!("Failed to read dir '{}'", path))?;

        let arr = js_sys::Array::new();
        for entry in entries {
            let entry = entry.context("Failed to read directory entry")?;

            let obj = js_sys::Object::new();
            let name = entry
                .path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            js_sys::Reflect::set(&obj, &"name".into(), &name.into()).unwrap();
            js_sys::Reflect::set(
                &obj,
                &"path".into(),
                &entry.path.to_string_lossy().into_owned().into(),
            )
            .unwrap();

            arr.push(&obj);
        }

        Ok(arr)
    }

    /// Create a directory and all parent directories.
    #[wasm_bindgen(js_name = "vfsMkdir")]
    pub fn vfs_mkdir(&self, path: &str) -> Result<(), Error> {
        self.create_dir_all_internal(Path::new(path))
            .with_context(|| format!("Failed to mkdir '{}'", path))?;
        Ok(())
    }

    /// Remove a file.
    #[wasm_bindgen(js_name = "vfsRemoveFile")]
    pub fn vfs_remove_file(&self, path: &str) -> Result<(), Error> {
        self.fs
            .remove_file(Path::new(path))
            .with_context(|| format!("Failed to remove '{}'", path))?;
        Ok(())
    }

    /// Remove an empty directory.
    #[wasm_bindgen(js_name = "vfsRemoveDir")]
    pub fn vfs_remove_dir(&self, path: &str) -> Result<(), Error> {
        self.fs
            .remove_dir(Path::new(path))
            .with_context(|| format!("Failed to rmdir '{}'", path))?;
        Ok(())
    }
}

impl Instance {
    #[tracing::instrument(skip_all)]
    async fn wait(self) -> Result<Output, Error> {
        let Instance {
            stdin,
            stdout,
            stderr,
            exit,
            fs: _,
        } = self;

        if let Some(stdin) = stdin {
            if stdin.locked() {
                // The caller has already acquired a writer so it's their
                // responsibility to close the stream.
            } else {
                match wasm_bindgen_futures::JsFuture::from(stdin.close()).await {
                    Ok(_) => {
                        tracing::debug!("Closed stdin");
                    }

                    Err(e) if e.has_type::<js_sys::TypeError>() => {
                        tracing::debug!("Stdin was already closed by the user");
                    }
                    Err(e) => {
                        return Err(Error::js(e));
                    }
                }
            }
        }

        tracing::debug!("Getting stdout");
        let mut stdout_buffer = Vec::new();
        let stdout_done = copy_to_buffer(crate::streams::read_to_end(stdout), &mut stdout_buffer);
        tracing::debug!("Getting stderr");
        let mut stderr_buffer = Vec::new();
        let stderr_done = copy_to_buffer(crate::streams::read_to_end(stderr), &mut stderr_buffer);

        // Note: this relies on the underlying instance closing stdout and
        // stderr when it exits. Failing to do this will block forever.
        let (_, _, ExitCondition(code)) =
            futures::try_join!(stdout_done, stderr_done, exit.map_err(Error::from))?;

        let output = Output {
            code,
            ok: code == 0,
            stdout: stdout_buffer,
            stderr: stderr_buffer,
        };

        Ok(output)
    }

    /// Internal helper to read a file.
    async fn read_file_internal(&self, path: &str) -> Result<Vec<u8>, Error> {
        let path_obj = Path::new(path);
        let mut file = self
            .fs
            .new_open_options()
            .read(true)
            .open(path_obj)
            .with_context(|| format!("Failed to open '{}'", path))?;

        let mut buffer = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buffer)
            .await
            .with_context(|| format!("Failed to read '{}'", path))?;

        Ok(buffer)
    }

    /// Internal helper to create directories recursively.
    fn create_dir_all_internal(&self, path: &Path) -> Result<(), virtual_fs::FsError> {
        if path.as_os_str().is_empty() || self.fs.metadata(path).is_ok() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            self.create_dir_all_internal(parent)?;
        }
        match self.fs.create_dir(path) {
            Ok(()) => Ok(()),
            Err(virtual_fs::FsError::AlreadyExists) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

async fn copy_to_buffer(
    stream: impl Stream<Item = Result<Vec<u8>, Error>>,
    buffer: &mut Vec<u8>,
) -> Result<(), Error> {
    futures::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        buffer.extend(chunk?);
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct ExitCondition(i32);

impl ExitCondition {
    pub(crate) fn from_result(result: Result<(), anyhow::Error>) -> Self {
        let err = match result {
            Ok(_) => return ExitCondition(0),
            Err(e) => e,
        };

        // looks like some sort of error occurred.
        let error_code = err
            .chain()
            .find_map(|e| e.downcast_ref::<WasiRuntimeError>())
            .and_then(|runtime_error| runtime_error.as_exit_code());

        match error_code {
            Some(code) => ExitCondition(code.raw()),
            None => {
                tracing::debug!(error = &*err, "Process exited unexpectedly");
                ExitCondition(1)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Output {
    code: i32,
    ok: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "Output")]
    pub type JsOutput;
}

impl From<Output> for JsOutput {
    fn from(value: Output) -> Self {
        let Output {
            code,
            ok,
            stdout,
            stderr,
        } = value;

        let output = js_sys::Object::new();
        let _ = js_sys::Reflect::set(&output, &JsValue::from_str("code"), &JsValue::from(code));
        let _ = js_sys::Reflect::set(&output, &JsValue::from_str("ok"), &JsValue::from(ok));
        let _ = js_sys::Reflect::set(
            &output,
            &JsValue::from_str("stdoutBytes"),
            &Uint8Array::from(stdout.as_slice()),
        );
        js_sys::Object::define_property(
            &output,
            &JsValue::from_str("stdout"),
            &lazily_decoded_string_property(stdout),
        );
        let _ = js_sys::Reflect::set(
            &output,
            &JsValue::from_str("stderrBytes"),
            &Uint8Array::from(stderr.as_slice()),
        );
        js_sys::Object::define_property(
            &output,
            &JsValue::from_str("stderr"),
            &lazily_decoded_string_property(stderr),
        );

        output.unchecked_into()
    }
}

fn lazily_decoded_string_property(binary: Vec<u8>) -> js_sys::Object {
    let contents: once_cell::unsync::Lazy<js_sys::JsString, _> =
        once_cell::unsync::Lazy::new(move || {
            let utf8 = String::from_utf8_lossy(&binary);
            js_sys::JsString::from(utf8.as_ref())
        });

    let property = js_sys::Object::new();
    let getter: Closure<dyn Fn() -> js_sys::JsString> =
        Closure::new(move || js_sys::JsString::clone(&contents));
    js_sys::Reflect::set(&property, &JsValue::from("get"), &getter.into_js_value()).unwrap();
    js_sys::Reflect::set(&property, &JsValue::from("enumerable"), &JsValue::TRUE).unwrap();

    property
}

#[wasm_bindgen(typescript_custom_section)]
const OUTPUT_TYPE_DEFINITION: &'static str = r#"
export type Output = {
    /* The program's exit code. */
    code: number;
    /* Did the program exit successfully? */
    ok: boolean;
    /* The contents of the program's stdout stream. */
    stdoutBytes: Uint8Array;
    /* The program's stdout stream, decoded as UTF-8. */
    readonly stdout: string;
    /* The contents of the program's stderr stream. */
    stderrBytes: Uint8Array;
    /* The program's stderr stream, decoded as UTF-8. */
    readonly stderr: string;
}
"#;

#[wasm_bindgen(typescript_custom_section)]
const VFS_TYPE_DEFINITIONS: &'static str = r#"
/**
 * Metadata about a file or directory in the VFS.
 */
export type VfsStat = {
    /** Is this path a file? */
    isFile: boolean;
    /** Is this path a directory? */
    isDir: boolean;
    /** Size of the file in bytes (0 for directories). */
    size: number;
}

/**
 * An entry in a directory listing.
 */
export type VfsDirEntry = {
    /** The name of the file or directory. */
    name: string;
    /** The full path to the entry. */
    path: string;
}
"#;

#[cfg(test)]
mod tests {
    use futures::channel::oneshot;
    use virtual_fs::AsyncReadExt;
    use virtual_fs::AsyncWriteExt;
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::*;

    #[wasm_bindgen_test]
    async fn read_stdout_and_stderr_when_waiting_for_completion() {
        let (mut stdin, stdin_stream) = crate::streams::input_pipe();
        let (mut stdout, stdout_stream) = crate::streams::output_pipe();
        let (mut stderr, stderr_stream) = crate::streams::output_pipe();
        let (sender, exit) = oneshot::channel();
        let instance = Instance {
            stdin: Some(stdin_stream),
            stdout: stdout_stream,
            stderr: stderr_stream,
            exit,
            fs: TmpFileSystem::new(),
        };
        dbg!(&instance);

        // First, let's pretend to be a WASIX process doing stuff in the background
        stdout.write_all(b"stdout").await.unwrap();
        stdout.flush().await.unwrap();
        stderr.write_all(b"stderr").await.unwrap();
        stderr.flush().await.unwrap();

        // Now, we pretend the WASIX process exited
        stdout.close();
        stderr.close();
        sender.send(ExitCondition(42)).unwrap();

        // and wait for the result
        let output = instance.wait().await.unwrap();

        assert_eq!(
            output,
            Output {
                code: 42,
                ok: false,
                stdout: b"stdout".to_vec(),
                stderr: b"stderr".to_vec()
            }
        );
        // Reading from stdin should now result in an EOF because it's closed
        let mut buffer = Vec::new();
        let bytes_read = stdin.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(bytes_read, 0);
    }
}
