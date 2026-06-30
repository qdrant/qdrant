//! Object-store backends (AWS S3, GCS, Azure) for the [`io_bridge`] sync ↔ async
//! bridge.
//!
//! This crate plugs the `object_store` ecosystem into the backend-agnostic
//! [`io_bridge`] crate. It provides:
//!
//! - [`BlobBackend`]: a concrete `object_store::ObjectStore` type plus a typed
//!   config that builds it, and
//! - a blanket [`AsyncRead`] impl on `Arc<S>` for any [`BlobBackend`] `S`
//!   (see [`source`]), so the bridge stays free of `dyn`.
//!
//! Concrete backends live under [`backends`]: [`backends::aws`],
//! [`backends::gcp`], and [`backends::azure`]. Each module declares its own
//! `Config` struct and a manual [`BlobBackend`] impl.
//!
//! The sync wrappers [`BlobFile`] / [`BlobFs`] and the [`BridgeRuntime`] are
//! re-exported from [`io_bridge`] for convenience, so an object-store-backed
//! universal-IO handle can be built entirely from this crate:
//!
//! ```ignore
//! use std::sync::Arc;
//! use io_bridge_object_store::{BlobFile, BridgeRuntime};
//! use io_bridge_object_store::backends::aws::AwsConfig;
//! use object_store::aws::AmazonS3;
//!
//! let file = BlobFile::<Arc<AmazonS3>>::open(&config, BridgeRuntime::global(), "key")?;
//! ```

mod backend;
pub mod backends;
mod source;

#[cfg(test)]
mod tests;

pub use backend::BlobBackend;
// Re-export the bridge core so a complete object-store-backed `UniversalRead`
// stack can be built from this single crate.
pub use io_bridge::{AsyncRead, BlobFile, BlobFs, BlobReadPipeline, BridgeRuntime};
pub use source::ObjectStoreSource;
