pub mod bitmask;
pub mod blob;
pub mod config;
mod direct_io;
pub mod error;
pub mod fixtures;
mod gridstore;
mod pages;
mod tracker;

pub use blob::Blob;
pub use gridstore::{Gridstore, GridstoreReader, GridstoreView};

use crate::error::GridstoreError;

pub(crate) type Result<T, E = GridstoreError> = std::result::Result<T, E>;
