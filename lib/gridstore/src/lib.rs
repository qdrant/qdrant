pub mod bitmask;
pub mod blob;
pub mod config;
pub mod error;
pub mod fixtures;
mod gridstore;
mod pages;
mod tracker;

pub use blob::Blob;
use common::universal_io::MmapFile;
pub use gridstore::{Gridstore, GridstoreReader, GridstoreView};

use crate::error::GridstoreError;

pub(crate) type Result<T> = std::result::Result<T, GridstoreError>;

/// Concrete tracker type used by gridstore (universal io over mmap).
pub(crate) type Tracker = tracker::Tracker<MmapFile>;
