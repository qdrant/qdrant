pub mod blob;
mod blobstore;
pub mod config;
pub mod error;
pub mod fixtures;
mod tracker;

pub use blob::Blob;
// The append-only variant, exported because `Blobstore` cannot be named on a backend that only
// appends: its type is bound at `UniversalWrite + UniversalAppend` for the sake of the other
// variant. Its cross-crate surface is open, put and flush, nothing more.
pub use blobstore::Logstore;
// The bitmask belongs to the Gridstore variant, it is only public for the benchmarks
pub use blobstore::gridstore::bitmask;
pub use blobstore::{Blobstore, BlobstoreReader, BlobstoreView};

use crate::error::BlobstoreError;

pub(crate) type Result<T, E = BlobstoreError> = std::result::Result<T, E>;
