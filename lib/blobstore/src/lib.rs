pub mod blob;
mod blobstore;
pub mod config;
mod direct_io;
pub mod error;
pub mod fixtures;
mod pages;
mod tracker;

pub use blob::Blob;
// The bitmask belongs to the Gridstore variant, it is only public for the benchmarks
pub use blobstore::gridstore::bitmask;
pub use blobstore::{Blobstore, BlobstoreReader, BlobstoreView};

use crate::error::BlobstoreError;

pub(crate) type Result<T, E = BlobstoreError> = std::result::Result<T, E>;
