# Wasmer-JS Scheduler Flakiness

## Status: Under Investigation

## Summary

When running wasmer-js SDK tests sequentially in Node.js, instances intermittently hang during `instance.wait()`. The issue appears after 2-3 successful runs, suggesting a resource exhaustion or scheduler issue.

## Reproduction

```javascript
import { init, Wasmer } from './dist/node.mjs';

await init();

// Run same test 3 times - third often hangs
for (let i = 0; i < 3; i++) {
  const pkg = await Wasmer.fromRegistry("saghul/quickjs@0.0.3");
  const instance = await pkg.commands["quickjs"].run({
    args: ["--eval", "console.log('hi')"],
  });
  const output = await instance.wait();  // <-- hangs on 3rd iteration
  console.log(`Run ${i+1}:`, output.stdout);
}
```

## Observations

1. **Individual tests pass**: Running single tests in isolation works fine
2. **Sequential tests fail intermittently**: After 2-3 runs, `instance.wait()` hangs
3. **Workers spawn correctly**: The deprecation warning shows 3 workers spawn each time
4. **Pattern is non-deterministic**: Sometimes fails on run 2, sometimes run 3

## Likely Causes

1. **Worker pool exhaustion**: The SDK spawns Web Workers for threading. If workers aren't properly cleaned up between runs, the pool may fill.

2. **SharedArrayBuffer memory leak**: With the new shared memory configuration, there may be buffer references that aren't released.

3. **Atomics deadlock**: The `Atomics.waitAsync()` used for scheduling could deadlock if the worker it's waiting on never responds.

4. **WASM instance cleanup**: The WASM instance may hold resources that prevent subsequent instances from executing.

## Environment

- Node.js v24.3.0
- wasmer-js v0.8.0 (rivet-patches branch)
- Rust nightly-2024-12-01
- SharedArrayBuffer enabled via new config

## Workarounds

For now, tests should be run with generous timeouts and potentially in separate processes.

## Related

- SharedArrayBuffer fix in `.cargo/config.toml`
- The `deprecated parameters` warning suggests SDK init is called multiple times
