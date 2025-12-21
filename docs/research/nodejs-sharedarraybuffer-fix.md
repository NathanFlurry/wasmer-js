# Node.js SharedArrayBuffer Fix Research

## Problem Statement

The wasmer-js SDK failed to run in Node.js with:
```
TypeError: [object Int32Array] is not a shared typed array.
    at Atomics.waitAsync (<anonymous>)
```

This error occurred during `init()` before any user code could run.

## Investigation

### Step 1: Identify the Error Source

The error originates from `Atomics.waitAsync()` which requires a SharedArrayBuffer-backed typed array. The call trace pointed to wasm-bindgen-futures internals.

### Step 2: Locate the Code Path

Found in `wasm-bindgen-futures` crate at `src/task/multithread.rs:177-179`:
```rust
let mem = wasm_bindgen::memory().unchecked_into::<js_sys::WebAssembly::Memory>();
let array = js_sys::Int32Array::new(&mem.buffer());
let result = Atomics::wait_async(&array, ptr.as_ptr() as u32 / 4, current_value);
```

This code creates an Int32Array from WASM linear memory and uses it for async waiting.

### Step 3: Inspect WASM Module

Disassembled the built WASM module to check memory declaration:
```bash
wasm2wat dist/wasmer_js_bg.wasm | head -20
```

Found:
```wasm
(memory $0 33)
```

This is a non-shared memory. For `Atomics.waitAsync` to work, it must be:
```wasm
(memory $0 33 65536 shared)
```

### Step 4: Research Shared Memory Requirements

Consulted wasm-bindgen-rayon documentation (known working example of shared memory WASM):

Required compiler/linker flags:
- `-C target-feature=+atomics,+bulk-memory,+mutable-globals` - Enable atomics
- `-C link-arg=--shared-memory` - Make memory shared
- `-C link-arg=--import-memory` - Import memory from JS
- `-C link-arg=--export-memory` - Export memory to JS
- `-C link-arg=--max-memory=4294967296` - Set max memory (required for shared)
- `build-std = ['std', 'panic_abort']` - Rebuild std with atomics support

### Step 5: Apply Fix and Debug Compatibility Issues

**Attempt 1: nightly-2024-08-02** (recommended by wasm-bindgen-rayon)
```
error: rustc 1.82.0-nightly is not supported, minimum 1.84
```
Failed because wasmer dependencies require newer rustc.

**Attempt 2: nightly-2025-07-05** (latest)
```
error: failed to prepare module for threading
Caused by: failed to find `__wasm_init_tls`
```
Failed due to wasm-bindgen threading transform incompatibility with latest nightly.

**Attempt 3: nightly-2024-12-01** (binary search for compatible version)
- rustc 1.85.0-nightly - satisfies wasmer's 1.84+ requirement
- Works with wasm-bindgen's threading transform
- Successfully builds with shared memory

## Solution

### File: `.cargo/config.toml`
```toml
[build]
target = "wasm32-unknown-unknown"

[target.wasm32-unknown-unknown]
runner = ["wasmer", "run"]
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

### File: `rust-toolchain.toml`
```toml
[toolchain]
channel = "nightly-2024-12-01"
targets = ["wasm32-unknown-unknown", "wasm32-wasi"]
components = ["rust-src", "rustfmt", "clippy"]
```

## Verification

After fix, WASM module has shared memory:
```wasm
(import "wbg" "memory" (memory $mimport$0 35 65536 shared))
```

Node.js test passes:
```javascript
import { init, Wasmer } from './dist/node.mjs';

await init();  // No longer throws SharedArrayBuffer error
const pkg = await Wasmer.fromRegistry("sharrattj/coreutils");
const instance = await pkg.commands["echo"].run({ args: ["hello"] });
const output = await instance.wait();
console.log(output.stdout);  // "hello"
```

## Key Insights

1. **WASM shared memory requires explicit linker flags** - The `+atomics` target feature alone is insufficient; `--shared-memory` linker arg is also required.

2. **`build-std` is essential** - Without rebuilding std with atomics, the standard library will use non-atomic implementations that break shared memory assumptions.

3. **Nightly version matters** - There's a narrow compatibility window between:
   - Minimum rustc version required by dependencies (1.84+)
   - Maximum version compatible with wasm-bindgen's threading transform
   - `nightly-2024-12-01` falls in this window

4. **The `__wasm_init_tls` error** indicates wasm-bindgen can't find thread-local storage initialization. This happens when the nightly Rust version generates TLS differently than wasm-bindgen expects.

## References

- [wasm-bindgen-rayon](https://docs.rs/wasm-bindgen-rayon) - Reference implementation for shared memory WASM
- [Rust WASM threading](https://rustwasm.github.io/wasm-bindgen/examples/rayon.html) - Official docs on WASM threading
- [SharedArrayBuffer MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer) - JS SharedArrayBuffer requirements
