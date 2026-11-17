//! Emits `cfg(jemalloc)` on the targets where jemalloc is the global allocator.
fn main() {
    // Must match the `tikv-jemallocator` target dependency in Cargo.toml: Cargo resolves
    // target dependencies before build scripts run, so the condition is spelled out there too.
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();

    println!("cargo::rustc-check-cfg=cfg(jemalloc)");
    if target_env != "msvc" && (target_arch == "x86_64" || target_arch == "aarch64") {
        println!("cargo::rustc-cfg=jemalloc");
    }
}
