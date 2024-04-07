#[cfg(target_arch = "aarch64")]
pub mod neon_dot;

#[cfg(target_arch = "aarch64")]
pub mod neon_cosine;

#[cfg(target_arch = "aarch64")]
pub mod neon_simple_manhattan;

#[cfg(target_arch = "aarch64")]
pub mod neon_euclid;
