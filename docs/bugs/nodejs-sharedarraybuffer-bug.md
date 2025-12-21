# Node.js SharedArrayBuffer Bug

## Status: FIXED

## Summary

The wasmer-js SDK failed to run in Node.js with the error:
```
TypeError: [object Int32Array] is not a shared typed array.
    at Atomics.waitAsync (<anonymous>)
```

## Root Cause

**The WASM linear memory was not declared as `shared`.**

The `wasm-bindgen-futures` crate uses `Atomics.waitAsync()` on the WASM linear memory buffer in `src/task/multithread.rs:177-179`:

```rust
let mem = wasm_bindgen::memory().unchecked_into::<js_sys::WebAssembly::Memory>();
let array = js_sys::Int32Array::new(&mem.buffer());
let result = Atomics::wait_async(&array, ...);
```

When inspecting the built WASM module, the memory was declared as:
```wasm
(memory $0 33)
```

But for `Atomics.waitAsync` to work, it must be:
```wasm
(memory $0 33 65536 shared)
```

## The Fix

Two changes were required:

### 1. Update `.cargo/config.toml` with shared memory linker flags

**Before (broken):**
```toml
rustflags = '-Ctarget-feature=+atomics,+bulk-memory -Clink-args=--no-check-features --cfg=web_sys_unstable_apis'
```

**After (fixed):**
```toml
rustflags = [
  "-C", "target-feature=+atomics,+bulk-memory,+mutable-globals",
  "-C", "link-arg=--shared-memory",
  "-C", "link-arg=--import-memory",
  "-C", "link-arg=--export-memory",
  "-C", "link-arg=--max-memory=4294967296",
  "--cfg=web_sys_unstable_apis",
]

[unstable]
build-std = ['std', 'panic_abort']
```

### 2. Use compatible nightly Rust version

The nightly toolchain must be compatible with wasm-bindgen's threading transform.
`nightly-2024-12-01` works correctly with the wasmer dependencies (which require rustc 1.84+).

**In `rust-toolchain.toml`:**
```toml
channel = "nightly-2024-12-01"
```

## Verification

After the fix, the WASM module correctly has shared memory:
```wasm
(import "wbg" "memory" (memory $mimport$0 35 65536 shared))
```

And Node.js tests pass:
```
$ node test.mjs
Initializing wasmer-js...
✓ init() succeeded - SharedArrayBuffer working!
✓ Package loaded
✓ Command output: hello from node
Exit code: 0
```

## References

- [wasm-bindgen-rayon docs](https://docs.rs/wasm-bindgen-rayon) - Example of correct shared memory setup
