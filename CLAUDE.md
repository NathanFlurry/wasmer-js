# wasmer-js Local Development

## Branches

- `v0.9.x` - base wasmer-js 0.9.0 release
- `rivet-patches` - patched version for local builds (use this)

## Dependencies

Requires local clones of:
- `../wasmer` - wasmer v4.4.0 on `rivet-patches` branch
- `../webc` - webc 6.1.0 from crates.io, patched for wasmer-config 0.9.0

## Build

Requires nightly-2024-09-23 for atomics/threading support:

```bash
RUSTUP_TOOLCHAIN=nightly-2024-09-23 npm run build
```

Or step by step:
```bash
RUSTUP_TOOLCHAIN=nightly-2024-09-23 wasm-pack build --release --target=web --weak-refs --no-pack
npm run build:rollup
```

## Dev Build

```bash
RUSTUP_TOOLCHAIN=nightly-2024-09-23 npm run build:dev
```
