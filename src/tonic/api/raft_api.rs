use crate::consensus;
use api::grpc::qdrant::{raft_server::Raft, RaftMessage as RaftMessageBytes};
use raft::eraftpb::Message as RaftMessage;
use std::sync::{mpsc::SyncSender, Mutex};
use tonic::{async_trait, Request, Response, Status};

pub struct RaftService(Mutex<SyncSender<consensus::Message>>);

impl RaftService {
    pub fn new(sender: SyncSender<consensus::Message>) -> Self {
        Self(Mutex::new(sender))
    }
}

#[async_trait]
impl Raft for RaftService {
    async fn send(&self, mut request: Request<RaftMessageBytes>) -> Result<Response<()>, Status> {
        let message = <RaftMessage as prost::Message>::decode(&request.get_mut().message[..])
            .map_err(|err| {
                Status::invalid_argument(format!("Failed to parse raft message: {err}"))
            })?;
        self.0
            .lock()
            .map_err(|_| Status::internal("Can't capture the Raft message sender lock"))?
            .send(consensus::Message::FromPeer(Box::new(message)))
            .map_err(|_| Status::internal("Can't send Raft message over channel"))?;
        Ok(Response::new(()))
    }
}
