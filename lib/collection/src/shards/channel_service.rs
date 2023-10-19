use std::collections::HashMap;
use std::sync::Arc;

use api::grpc::transport_channel_pool::TransportChannelPool;
use tonic::transport::Uri;
use url::Url;

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard::PeerId;

#[derive(Clone)]
pub struct ChannelService {
    // Shared with consensus_state
    pub id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
    pub channel_pool: Arc<TransportChannelPool>,
    /// Port at which the public REST API is exposed for the current peer.
    pub current_rest_port: u16,
}

impl ChannelService {
    /// Construct a new channel service with the given REST port.
    pub fn new(current_rest_port: u16) -> Self {
        Self {
            id_to_address: Default::default(),
            channel_pool: Default::default(),
            current_rest_port,
        }
    }

    pub async fn remove_peer(&self, peer_id: PeerId) {
        let removed = self.id_to_address.write().remove(&peer_id);
        if let Some(uri) = removed {
            self.channel_pool.drop_pool(&uri).await;
        }
    }

    /// Get the REST address for the current peer.
    pub fn current_rest_address(&self, this_peer_id: PeerId) -> CollectionResult<Url> {
        // Get local peer URI
        let local_peer_uri = self
            .id_to_address
            .read()
            .get(&this_peer_id)
            .cloned()
            .ok_or_else(|| {
                CollectionError::service_error(format!(
                    "Cannot determine REST address, this peer not found in cluster by ID {this_peer_id} ",
                ))
            })?;

        // Construct REST URL from URI
        let mut url = Url::parse(&local_peer_uri.to_string()).expect("Malformed URL");
        url.set_port(Some(self.current_rest_port))
            .map_err(|()| {
                CollectionError::service_error(format!(
                    "Cannot determine REST address, cannot specify port on address {url} for peer ID {this_peer_id}",
                ))
            })?;
        Ok(url)
    }
}

#[cfg(test)]
impl Default for ChannelService {
    fn default() -> Self {
        Self {
            id_to_address: Default::default(),
            channel_pool: Default::default(),
            current_rest_port: 6333,
        }
    }
}
