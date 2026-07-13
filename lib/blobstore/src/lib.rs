pub mod bitmask;
pub mod blob;
mod blobstore;
pub mod config;
mod direct_io;
pub mod error;
pub mod fixtures;
mod pages;
mod tracker;

pub use blob::Blob;
pub use blobstore::{Blobstore, BlobstoreReader, BlobstoreView};

use crate::error::GridstoreError;

pub(crate) type Result<T, E = GridstoreError> = std::result::Result<T, E>;
