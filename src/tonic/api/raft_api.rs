use api::grpc::qdrant::raft_server::Raft;
use api::grpc::qdrant::{
    AddPeerToKnownMessage, AllPeers, Peer, PeerId, RaftMessage as RaftMessageBytes, Uri as UriStr,
};
use itertools::Itertools;
use raft::eraftpb::Message as RaftMessage;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::consensus_ops::ConsensusOperations;
use tokio::sync::mpsc::Sender;
use tonic::transport::Uri;
use tonic::{async_trait, Request, Response, Status};

use super::validate;
use crate::consensus;

pub struct RaftService {
    message_sender: Sender<consensus::Message>,
    consensus_state: ConsensusStateRef,
}

impl RaftService {
    pub fn new(sender: Sender<consensus::Message>, consensus_state: ConsensusStateRef) -> Self {
        Self {
            message_sender: sender,
            consensus_state,
        }
    }
}

#[async_trait]
impl Raft for RaftService {
    async fn send(&self, mut request: Request<RaftMessageBytes>) -> Result<Response<()>, Status> {
        let message =
            <RaftMessage as prost_for_raft::Message>::decode(&request.get_mut().message[..])
                .map_err(|err| {
                    Status::invalid_argument(format!("Failed to parse raft message: {err}"))
                })?;
        self.message_sender
            .send(consensus::Message::FromPeer(Box::new(message)))
            .await
            .map_err(|_| Status::internal("Can't send Raft message over channel"))?;
        Ok(Response::new(()))
    }

    async fn who_is(
        &self,
        request: tonic::Request<PeerId>,
    ) -> Result<tonic::Response<UriStr>, tonic::Status> {
        let addresses = self.consensus_state.peer_address_by_id();
        let uri = addresses
            .get(&request.get_ref().id)
            .ok_or_else(|| Status::internal("Peer not found"))?;
        Ok(Response::new(UriStr {
            uri: uri.to_string(),
        }))
    }

    async fn add_peer_to_known(
        &self,
        request: tonic::Request<AddPeerToKnownMessage>,
    ) -> Result<tonic::Response<AllPeers>, tonic::Status> {
        validate(request.get_ref())?;
        let peer = request.get_ref();
        let uri_string = if let Some(uri) = &peer.uri {
            uri.clone()
        } else {
            let ip = request
                .remote_addr()
                .ok_or_else(|| {
                    Status::failed_precondition("Remote address unavailable due to the used IO")
                })?
                .ip();
            let port = peer
                .port
                .ok_or_else(|| Status::invalid_argument("URI or port should be supplied"))?;
            format!("http://{ip}:{port}")
        };
        let uri: Uri = uri_string
            .parse()
            .map_err(|err| Status::internal(format!("Failed to parse uri: {err}")))?;
        let peer = request.into_inner();

        // the consensus operation can take up to DEFAULT_META_OP_WAIT
        self.consensus_state
            .propose_consensus_op_with_await(
                ConsensusOperations::AddPeer {
                    peer_id: peer.id,
                    uri: uri.to_string(),
                },
                None,
            )
            .await
            .map_err(|err| Status::internal(format!("Failed to add peer: {err}")))?;

        let mut addresses = self.consensus_state.peer_address_by_id();

        // Make sure that the new peer is now present in the known addresses
        if !addresses.values().contains(&uri) {
            return Err(Status::internal(format!(
                "Failed to add peer after consensus: {uri}"
            )));
        }

        let first_peer_id = self.consensus_state.first_voter();

        // If `first_peer_id` is not present in the list of peers, it means it was removed from
        // cluster at some point.
        //
        // Before Qdrant version 1.11.6 origin peer was not committed to consensus, so if it was
        // removed from cluster, any node added to the cluster after this would not recognize it as
        // being part of the cluster in the past and will end up with a broken consensus state.
        //
        // To prevent this, we add `first_peer_id` (with a fake URI) to the list of peers.
        //
        // `add_peer_to_known` is used to add new peers to the cluster, and so `first_peer_id` (and
        // its fake URI) would be removed from new peer's state shortly, while it will be synchronizing
        // and applying past Raft log.
        addresses.entry(first_peer_id).or_default();

        Ok(Response::new(AllPeers {
            all_peers: addresses
                .into_iter()
                .map(|(id, uri)| Peer {
                    id,
                    uri: uri.to_string(),
                })
                .collect(),
            first_peer_id,
        }))
    }

    // Left for compatibility - does nothing
    async fn add_peer_as_participant(
        &self,
        _request: tonic::Request<PeerId>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(Response::new(()))
    }
}
