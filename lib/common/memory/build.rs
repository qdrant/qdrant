fn main() {
    println!("cargo:rustc-check-cfg=cfg(posix_fadvise_supported)");

    // Matches all platforms that have `nix::fcntl::posix_fadvise` function.
    // https://github.com/nix-rust/nix/blob/v0.29.0/src/fcntl.rs#L35-L42
    if matches!(
        std::env::var("CARGO_CFG_TARGET_OS").unwrap().as_str(),
        "linux" | "freebsd" | "android" | "fuchsia" | "emscripten" | "wasi"
    ) || matches!(
        std::env::var("CARGO_CFG_TARGET_ENV").unwrap().as_str(),
        "uclibc"
    ) {
        println!("cargo:rustc-cfg=posix_fadvise_supported")
    }
}
