pub mod conversions;
pub mod models;
#[allow(clippy::all)]
#[rustfmt::skip] // tonic uses `prettyplease` to format its output
pub mod qdrant;

use std::time::Duration;

use tonic::transport::{Channel, Error, Uri};
use tower::timeout::Timeout;

pub const fn api_crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub async fn timeout_channel(
    timeout: Duration,
    peer_address: Uri,
) -> Result<Timeout<Channel>, Error> {
    let channel = Channel::builder(peer_address).connect().await?;
    Ok(Timeout::new(channel, timeout))
}
