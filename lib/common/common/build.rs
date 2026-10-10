/// Returns true if the target requires linking against libatomic for 64-bit
/// atomic operations.  32-bit ARM lacks native 8-byte atomic instructions, so
/// the compiler falls back to `__atomic_load_8` / `__atomic_fetch_add_8` etc.
/// which are supplied by GCC's libatomic.
fn needs_libatomic(target_arch: &str) -> bool {
    target_arch == "arm"
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // Required for tango benchmarks, see:
    // https://github.com/bazhenov/tango/blob/v0.6.0/README.md#getting-started
    println!("cargo:rustc-link-arg-benches=-rdynamic");

    // On 32-bit ARM targets, 64-bit atomic operations (e.g. AtomicU64) are not
    // natively supported by the hardware and must be emulated via GCC's libatomic.
    // Without this, cross-compiling for targets such as `arm-unknown-linux-gnueabi`
    // fails with undefined references to `__atomic_load_8`, `__atomic_fetch_add_8`,
    // etc.
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    if needs_libatomic(&target_arch) {
        println!("cargo:rustc-link-lib=atomic");
    }

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

#[cfg(test)]
mod tests {
    use super::needs_libatomic;

    #[test]
    fn test_arm32_requires_libatomic() {
        // 32-bit ARM lacks native 64-bit atomics; libatomic must be linked.
        assert!(
            needs_libatomic("arm"),
            "arm target should require libatomic for 64-bit atomic operations"
        );
    }

    #[test]
    fn test_aarch64_does_not_require_libatomic() {
        // 64-bit AArch64 has native 64-bit atomic instructions.
        assert!(
            !needs_libatomic("aarch64"),
            "aarch64 target should not require libatomic"
        );
    }

    #[test]
    fn test_x86_64_does_not_require_libatomic() {
        assert!(
            !needs_libatomic("x86_64"),
            "x86_64 target should not require libatomic"
        );
    }

    #[test]
    fn test_x86_does_not_require_libatomic() {
        // 32-bit x86 uses LOCK-prefixed instructions for 64-bit atomics via cmpxchg8b.
        assert!(
            !needs_libatomic("x86"),
            "x86 target should not require libatomic"
        );
    }
}
