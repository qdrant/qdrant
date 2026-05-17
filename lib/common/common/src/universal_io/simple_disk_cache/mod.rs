mod config;
mod file;

pub mod pipeline;
pub mod prefill;
#[cfg(test)]
mod tests;

pub use config::DiskCacheConfig;
pub use file::DiskCache;

/// Files are logically split into fixed-size blocks; the roaring bitmap
/// tracks population on a per-block basis.
///
/// Matches `disk_cache::BLOCK_SIZE` and is a small multiple of typical
/// filesystem block sizes (usually 4 KiB).
const BLOCK_SIZE: usize = 16 * 1024; // 16kB
