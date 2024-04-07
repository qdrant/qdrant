pub mod metric;
pub mod simple;
pub mod tools;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub mod simple_sse;

#[cfg(target_arch = "x86_64")]
pub mod simple_avx;

pub mod metric_uint;

#[cfg(target_arch = "aarch64")]
pub mod simple_neon;
