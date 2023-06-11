pub mod conversions;
pub mod models;
#[allow(clippy::all)]
#[rustfmt::skip] // tonic uses `prettyplease` to format its output
pub mod qdrant;
pub mod dynamic_channel_pool;
pub mod dynamic_pool;
pub mod transport_channel_pool;
pub mod validate;

// Include generated protobuf "well-known types".
#[path = "google.protobuf.rs"]
#[rustfmt::skip]
pub mod protobuf;

// `prost`'s generated code expects these types
// under `google::protobuf`, so we do a little module path hack here
pub mod google {
    pub use super::protobuf;
}

pub const fn api_crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
