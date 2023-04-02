use std::collections::HashMap;
use std::sync::Arc;

use api::grpc::transport_channel_pool::{TransportChannelPool, ClientMutualTlsCertificates};
use tonic::transport::Uri;

use crate::shards::shard::PeerId;

#[derive(Clone, Default)]
pub struct ChannelService {
    // Shared with consensus_state
    pub id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
    pub channel_pool: Arc<TransportChannelPool>,
    pub tls_certificates: Option<ClientMutualTlsCertificates>
}

impl ChannelService {
    pub fn new(
        id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
        channel_pool: Arc<TransportChannelPool>,
        tls_certificates: &Option<ClientMutualTlsCertificates>
    ) -> Self {
        Self {
            id_to_address,
            channel_pool,
            tls_certificates: tls_certificates.clone()
        }
    }

    pub async fn remove_peer(&self, peer_id: PeerId) {
        let removed = self.id_to_address.write().remove(&peer_id);
        if let Some(uri) = removed {
            self.channel_pool.drop_pool(&uri).await;
        }
    }
}
