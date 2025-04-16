use std::io;
use std::path::Path;

/// For given file path, clear disk cache with `posix_fadvise`
///
/// If `posix_fadvise` is not supported, this function does nothing.
pub fn clear_disk_cache(file_path: &Path) -> io::Result<()> {
    // https://github.com/nix-rust/nix/blob/v0.29.0/src/fcntl.rs#L35-L42
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "android",
        target_os = "fuchsia",
        target_os = "emscripten",
        target_os = "wasi",
        target_env = "uclibc",
    ))]
    {
        use std::fs::File;
        use std::os::fd::AsRawFd as _;

        use nix::fcntl;

        let file = File::open(file_path)?;
        let fd = file.as_raw_fd();

        fcntl::posix_fadvise(fd, 0, 0, fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED)
            .map_err(io::Error::from)?;
    }

    _ = file_path;

    Ok(())
}
