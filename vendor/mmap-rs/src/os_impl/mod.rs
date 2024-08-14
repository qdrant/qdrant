#[cfg(unix)]
pub mod unix;

#[cfg(windows)]
pub mod windows;

#[cfg(target_os = "freebsd")]
pub mod freebsd;

#[cfg(any(target_os = "android", target_os = "linux"))]
pub mod linux;

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub mod macos;
