use std::collections::HashMap;
use std::sync::Arc;

use api::grpc::transport_channel_pool::TransportChannelPool;
use http::uri::Scheme;
use tonic::transport::Uri;
use url::Url;

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard::PeerId;

#[derive(Clone)]
pub struct ChannelService {
    // Shared with consensus_state
    pub id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
    pub channel_pool: Arc<TransportChannelPool>,
    /// Port at which the public REST API is exposed.
    pub rest_port: u16,
}

impl ChannelService {
    /// Construct a new channel service with the given REST port.
    pub fn new(rest_port: u16) -> Self {
        Self {
            id_to_address: Default::default(),
            channel_pool: Default::default(),
            rest_port,
        }
    }

    pub async fn remove_peer(&self, peer_id: PeerId) {
        let removed = self.id_to_address.write().remove(&peer_id);
        if let Some(uri) = removed {
            self.channel_pool.drop_pool(&uri).await;
        }
    }

    /// Get the REST address for a given peer.
    pub fn rest_address(&self, peer_id: PeerId) -> CollectionResult<Url> {
        let local_peer_uri = self
            .id_to_address
            .read()
            .get(&peer_id)
            .cloned()
            .ok_or_else(|| {
                CollectionError::service_error(format!(
                    "Cannot determine REST address, peer {peer_id} not found in cluster",
                ))
            })?;
        Ok(Url::parse(&format!(
            "{}://{}:{}",
            local_peer_uri.scheme().unwrap_or(&Scheme::HTTP),
            local_peer_uri.host().ok_or_else(|| {
                CollectionError::service_error(
                    "Cannot determine REST address, peer {peer_id} has unknown host",
                )
            })?,
            self.rest_port,
        ))
        .expect("Malformed URL"))
    }
}

#[cfg(test)]
impl Default for ChannelService {
    fn default() -> Self {
        Self {
            id_to_address: Default::default(),
            channel_pool: Default::default(),
            rest_port: 6333,
        }
    }
}
