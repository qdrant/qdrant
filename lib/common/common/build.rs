fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // Required for tango benchmarks, see:
    // https://github.com/bazhenov/tango/blob/v0.6.0/README.md#getting-started
    println!("cargo:rustc-link-arg-benches=-rdynamic");

    // Matches all platforms that have `nix::fcntl::posix_fadvise` function.
    // https://github.com/nix-rust/nix/blob/v0.29.0/src/fcntl.rs#L35-L42
    println!("cargo:rustc-check-cfg=cfg(posix_fadvise_supported)");
    if matches!(
        std::env::var("CARGO_CFG_TARGET_OS").unwrap().as_str(),
        "linux" | "freebsd" | "android" | "fuchsia" | "emscripten" | "wasi"
    ) || matches!(
        std::env::var("CARGO_CFG_TARGET_ENV").unwrap().as_str(),
        "uclibc"
    ) {
        println!("cargo:rustc-cfg=posix_fadvise_supported")
    }

    // Matches all platforms, that have `nix::sys::statfs::statfs` function.
    // https://github.com/nix-rust/nix/blob/v0.29.0/src/sys/mod.rs#L131
    println!("cargo:rustc-check-cfg=cfg(fs_type_check_supported)");
    if matches!(
        std::env::var("CARGO_CFG_TARGET_OS").unwrap().as_str(),
        "linux"
            | "freebsd"
            | "android"
            | "openbsd"
            | "ios"
            | "macos"
            | "watchos"
            | "tvos"
            | "visionos"
    ) {
        println!("cargo:rustc-cfg=fs_type_check_supported")
    }
}
