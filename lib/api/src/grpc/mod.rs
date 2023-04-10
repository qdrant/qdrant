#![allow(deprecated)]

pub mod conversions;
pub mod models;
#[allow(clippy::all)]
#[rustfmt::skip] // tonic uses `prettyplease` to format its output
pub mod qdrant;
pub mod dynamic_channel_pool;
pub mod dynamic_pool;
pub mod transport_channel_pool;
pub mod validate;

pub const fn api_crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
