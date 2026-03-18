pub mod simple_cosine;
pub mod simple_dot;
pub mod simple_euclid;
pub mod simple_manhattan;

#[cfg(target_arch = "x86_64")]
pub mod avx;

#[cfg(all(target_arch = "aarch64", not(windows)))]
pub mod neon;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub mod sse;
