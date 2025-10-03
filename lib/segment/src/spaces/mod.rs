pub mod metric;
pub mod simple;
pub mod tools;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub mod simple_sse;

#[cfg(target_arch = "x86_64")]
pub mod simple_avx;
#[cfg(target_arch = "x86_64")]
pub mod simple_avx512;

pub mod metric_f16;
pub mod metric_uint;

#[cfg(target_arch = "aarch64")]
pub mod simple_neon;
