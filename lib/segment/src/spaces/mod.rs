pub mod metric;
pub mod simple;
pub mod tools;

#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "use_f32"))]
pub mod simple_sse;

#[cfg(all(target_arch = "x86_64", feature = "use_f32"))]
pub mod simple_avx;

#[cfg(all(target_arch = "aarch64", target_feature = "neon", feature = "use_f32"))]
pub mod simple_neon;
