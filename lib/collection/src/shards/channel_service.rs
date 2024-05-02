use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::qdrant_internal_client::QdrantInternalClient;
use api::grpc::qdrant::WaitOnConsensusCommitRequest;
use api::grpc::transport_channel_pool::{AddTimeout, TransportChannelPool};
use futures::future::try_join_all;
use futures::Future;
use semver::Version;
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};
use url::Url;

use crate::operations::types::{CollectionError, CollectionResult, PeerMetadata};
use crate::shards::shard::PeerId;

#[derive(Clone)]
pub struct ChannelService {
    // Shared with consensus_state
    pub id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
    // Shared with consensus_state
    pub id_to_metadata: Arc<parking_lot::RwLock<HashMap<PeerId, PeerMetadata>>>,
    pub channel_pool: Arc<TransportChannelPool>,
    /// Port at which the public REST API is exposed for the current peer.
    pub current_rest_port: u16,
    /// Instance wide API key if configured, must be used with care.
    pub api_key: Option<String>,
}

impl ChannelService {
    /// Construct a new channel service with the given REST port.
    pub fn new(current_rest_port: u16, api_key: Option<String>) -> Self {
        Self {
            id_to_address: Default::default(),
            id_to_metadata: Default::default(),
            channel_pool: Default::default(),
            current_rest_port,
            api_key,
        }
    }

    pub async fn remove_peer(&self, peer_id: PeerId) {
        let removed = self.id_to_address.write().remove(&peer_id);
        if let Some(uri) = removed {
            self.channel_pool.drop_pool(&uri).await;
        }
    }

    /// Wait until all other known peers reach the given commit
    ///
    /// # Errors
    ///
    /// This errors if:
    /// - any of the peers is not on the same term
    /// - waiting takes longer than the specified timeout
    /// - any of the peers cannot be reached
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn await_commit_on_all_peers(
        &self,
        this_peer_id: PeerId,
        commit: u64,
        term: u64,
        timeout: Duration,
    ) -> Result<(), CollectionError> {
        let requests = self
            .id_to_address
            .read()
            .keys()
            .filter(|id| **id != this_peer_id)
            // The collective timeout at the bottom of this function handles actually timing out.
            // Since an explicit timeout must be given here as well, it is multiplied by two to
            // give the collective timeout some space.
            .map(|peer_id| self.await_commit_on_peer(*peer_id, commit, term, timeout * 2))
            .collect::<Vec<_>>();
        let responses = try_join_all(requests);

        // Handle requests with timeout
        tokio::time::timeout(timeout, responses)
            .await
            // Timeout error
            .map_err(|_elapsed| CollectionError::Timeout {
                description: "Failed to wait for consensus commit on all peers, timed out.".into(),
            })?
            // Await consensus error
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to wait for consensus commit on peer: {err}"
                ))
            })?;
        Ok(())
    }

    /// Wait until the given peer reaches the given commit
    ///
    /// # Errors
    ///
    /// This errors if the given peer is on a different term. Also errors if the peer cannot be reached.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn await_commit_on_peer(
        &self,
        peer_id: PeerId,
        commit: u64,
        term: u64,
        timeout: Duration,
    ) -> Result<(), CollectionError> {
        let response = self
            .with_qdrant_client(peer_id, |mut client| async move {
                let request = WaitOnConsensusCommitRequest {
                    commit: commit as i64,
                    term: term as i64,
                    timeout: timeout.as_secs() as i64,
                };
                client.wait_on_consensus_commit(Request::new(request)).await
            })
            .await
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to wait for consensus commit on peer {peer_id}: {err}"
                ))
            })?
            .into_inner();

        // Create error if wait request failed
        if !response.ok {
            return Err(CollectionError::service_error(format!(
                "Failed to wait for consensus commit on peer {peer_id}, has diverged commit/term or timed out."
            )));
        }
        Ok(())
    }

    async fn with_qdrant_client<T, O: Future<Output = Result<T, Status>>>(
        &self,
        peer_id: PeerId,
        f: impl Fn(QdrantInternalClient<InterceptedService<Channel, AddTimeout>>) -> O,
    ) -> Result<T, CollectionError> {
        let address = self
            .id_to_address
            .read()
            .get(&peer_id)
            .ok_or_else(|| CollectionError::service_error("Address for peer ID is not found."))?
            .clone();
        self.channel_pool
            .with_channel(&address, |channel| {
                let client = QdrantInternalClient::new(channel);
                let client = client.max_decoding_message_size(usize::MAX);
                f(client)
            })
            .await
            .map_err(Into::into)
    }

    /// Check whether all peers are running at least the given version
    ///
    /// If the version is not known for any peer, this returns `false`.
    /// Peer versions are known since 1.9 and up.
    pub fn all_peers_at_version(&self, version: Version) -> bool {
        let id_to_address = self.id_to_address.read();
        let id_to_metadata = self.id_to_metadata.read();

        // Ensure there aren't more peer addresses than metadata
        if id_to_address.len() > id_to_metadata.len() {
            return false;
        }

        id_to_metadata
            .values()
            .all(|metadata| metadata.version >= version)
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
            id_to_metadata: Default::default(),
            channel_pool: Default::default(),
            current_rest_port: 6333,
        }
    }
}
