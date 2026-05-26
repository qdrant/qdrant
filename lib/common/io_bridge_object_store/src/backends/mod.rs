//! Concrete object-store backend implementations.
//!
//! Each submodule declares its own `Config` struct + [`crate::BlobBackend`]
//! impl for one upstream `object_store` type.

pub mod aws;
pub mod azure;
pub mod gcp;

pub use aws::{AwsConfig, AwsCredentials};
pub use azure::{AzureConfig, AzureCredentials};
pub use gcp::{GcsConfig, GcsCredentials};
