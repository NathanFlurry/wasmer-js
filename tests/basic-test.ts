import { init, runWasix, Wasmer, wat2wasm } from "../dist/node.mjs";

async function main() {
    console.log("Initializing wasmer SDK...");
    await init();
    console.log("SDK initialized");

    // Test 1: Simple noop program
    console.log("\n=== Test 1: Noop program ===");
    const noop = `(
        module
            (memory $memory 0)
            (export "memory" (memory $memory))
            (func (export "_start") nop)
        )`;
    const wasm = wat2wasm(noop);
    const module = await WebAssembly.compile(wasm);
    const instance1 = await runWasix(module, { program: "noop" });
    const output1 = await instance1.wait();
    console.log("Exit code:", output1.code);
    console.log("OK:", output1.ok);

    if (!output1.ok || output1.code !== 0) {
        throw new Error("Test 1 failed: expected exit code 0");
    }
    console.log("Test 1 passed!");

    // Test 2: Fetch package from registry
    console.log("\n=== Test 2: Package from registry ===");
    const pkg = await Wasmer.fromRegistry("wasmer/cowsay");
    console.log("Package loaded:", pkg.manifest);

    const cowsay = pkg.commands["cowsay"];
    if (!cowsay) {
        console.log("Available commands:", Object.keys(pkg.commands));
        throw new Error("cowsay command not found");
    }

    const binary = cowsay.binary();
    const instance2 = await runWasix(binary, {
        program: "cowsay",
        args: ["hello", "from", "wasmer", "0.10!"],
    });

    const output2 = await instance2.wait();
    console.log("Exit code:", output2.code);
    console.log("Stdout:", output2.stdout);
    if (output2.stderr) {
        console.log("Stderr:", output2.stderr);
    }

    if (!output2.ok) {
        throw new Error("Test 2 failed: cowsay did not exit successfully");
    }
    console.log("Test 2 passed!");

    console.log("\n=== All tests passed! ===");
}

main().catch(err => {
    console.error("Test failed:", err);
    process.exit(1);
});
