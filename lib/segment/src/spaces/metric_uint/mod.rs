#[cfg(target_arch = "x86_64")]
mod avx_dot;

#[cfg(target_arch = "x86_64")]
mod avx_cosine;

#[cfg(target_arch = "x86_64")]
mod avx_simple_manhattan;

#[cfg(target_arch = "x86_64")]
mod avx_euclid;

#[cfg(target_arch = "aarch64")]
mod neon_dot;

#[cfg(target_arch = "aarch64")]
mod neon_cosine;

#[cfg(target_arch = "aarch64")]
mod neon_simple_manhattan;

#[cfg(target_arch = "aarch64")]
mod neon_euclid;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod sse_dot;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod sse_cosine;

#[cfg(target_arch = "x86_64")]
mod sse_simple_manhattan;

#[cfg(target_arch = "x86_64")]
mod sse_euclid;

mod simple_cosine;
mod simple_dot;
mod simple_euclid;
mod simple_manhattan;
