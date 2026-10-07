//! Collection operations must remain available while a sender waits for remote recovery or
//! consensus during a shard transfer.

use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::collections_internal_server::{
    CollectionsInternal, CollectionsInternalServer,
};
use api::grpc::qdrant::qdrant_server::{Qdrant, QdrantServer};
use api::grpc::qdrant::shard_snapshots_server::{ShardSnapshots, ShardSnapshotsServer};
use api::grpc::qdrant::{
    CollectionOperationResponse, CreateShardSnapshotRequest, CreateSnapshotResponse,
    DeleteShardSnapshotRequest, DeleteSnapshotResponse, GetCollectionInfoRequestInternal,
    GetCollectionInfoResponse, GetShardMemoryReportRequest, GetShardMemoryReportResponse,
    GetShardOptimizationsRequest, GetShardOptimizationsResponse, GetShardRecoveryPointRequest,
    GetShardRecoveryPointResponse, HealthCheckReply, HealthCheckRequest,
    InitiateShardTransferRequest, ListShardSnapshotsRequest, ListSnapshotsResponse,
    RecoverShardSnapshotRequest, RecoverSnapshotResponse, RecoveryPoint,
    UpdateShardCutoffPointRequest, WaitForShardStateRequest,
};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::types::{CollectionError, CollectionResult, PeerMetadata};
use collection::shards::CollectionId;
use collection::shards::channel_service::ChannelService;
use collection::shards::remote_shard::RemoteShard;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::driver::transfer_shard;
use collection::shards::transfer::transfer_tasks_pool::TransferTaskProgress;
use collection::shards::transfer::{
    ShardTransfer, ShardTransferConsensus, ShardTransferKey, ShardTransferMethod,
};
use common::counter::AmbientContext;
use common::counter::hw::HwFutureExt;
use parking_lot::Mutex;
use rstest::rstest;
use segment::types::StrictModeConfig;
use semver::Version;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_util::task::AbortOnDropHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::common::simple_collection_fixture;

const THIS_PEER: PeerId = 0;
const REMOTE_PEER: PeerId = 1;
const SHARD: ShardId = 0;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
enum Checkpoint {
    Recovery,
    Consensus,
}

#[derive(Default)]
struct TransferPause {
    entered: Notify,
    release: Notify,
}

impl TransferPause {
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}

#[rstest]
#[case::snapshot_file_recovery(ShardTransferMethod::Snapshot, false, Checkpoint::Recovery)]
#[case::snapshot_stream_recovery(ShardTransferMethod::Snapshot, true, Checkpoint::Recovery)]
#[case::snapshot_file_consensus(ShardTransferMethod::Snapshot, false, Checkpoint::Consensus)]
#[case::snapshot_stream_consensus(ShardTransferMethod::Snapshot, true, Checkpoint::Consensus)]
#[case::wal_delta_consensus(ShardTransferMethod::WalDelta, false, Checkpoint::Consensus)]
#[tokio::test]
async fn test_sender_releases_shard_holder(
    #[case] method: ShardTransferMethod,
    #[case] streaming: bool,
    #[case] checkpoint: Checkpoint,
) {
    let dir = TempDir::new().unwrap();
    let collection = simple_collection_fixture(dir.path(), 1).await;

    // WAL-delta requires a nonempty recovery point. Give the mock the current clocks
    // so the sender reaches consensus without needing to send any WAL updates.
    let point = PointStructPersisted {
        id: 1.into(),
        vector: VectorStructPersisted::Single(vec![1.0, 0.0, 0.0, 0.0]),
        payload: None,
    };
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));
    collection
        .update_from_client_simple(operation, true, None, WriteOrdering::default())
        .measured(AmbientContext::new())
        .await
        .unwrap();
    let recovery_point = collection.shard_recovery_point(SHARD).await.unwrap();
    assert!(!recovery_point.is_empty());

    let pause = Arc::new(TransferPause::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote_address = listener.local_addr().unwrap();
    let peer = MockPeer {
        recovery_pause: matches!(checkpoint, Checkpoint::Recovery).then(|| pause.clone()),
        recovery_point: recovery_point.into(),
    };
    let incoming = futures::stream::unfold(listener, |listener| async {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let server = Server::builder()
        .add_service(QdrantServer::new(peer.clone()))
        .add_service(CollectionsInternalServer::new(peer.clone()))
        .add_service(ShardSnapshotsServer::new(peer));
    let server_task = AbortOnDropHandle::new(tokio::spawn(server.serve_with_incoming(incoming)));

    let channel_service = ChannelService::new(6333, false, None, None);
    channel_service
        .id_to_address
        .write()
        .insert(THIS_PEER, "http://127.0.0.1:6335".parse().unwrap());
    channel_service.id_to_address.write().insert(
        REMOTE_PEER,
        format!("http://{remote_address}").parse().unwrap(),
    );
    let remote_version = if streaming {
        Version::new(1, 12, 0)
    } else {
        Version::new(1, 11, 0)
    };
    channel_service
        .id_to_metadata
        .write()
        .insert(REMOTE_PEER, PeerMetadata::new(remote_version));

    let transfer = ShardTransfer {
        shard_id: SHARD,
        to_shard_id: None,
        from: THIS_PEER,
        to: REMOTE_PEER,
        sync: true,
        method: Some(method),
        filter: None,
    };
    let consensus = TestConsensus {
        pause: matches!(checkpoint, Checkpoint::Consensus).then(|| pause.clone()),
    };
    let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
    let shard_holder = collection.shards_holder();
    let collection_path = dir.path().to_path_buf();
    let transfer_task = AbortOnDropHandle::new(tokio::spawn(async move {
        transfer_shard(
            transfer,
            progress,
            shard_holder,
            &consensus,
            "test".to_string(),
            channel_service,
            &collection_path.join("snapshots"),
            &collection_path,
            method,
        )
        .await
    }));

    // The mock signals only after the sender has reached the selected await point.
    tokio::time::timeout(TEST_TIMEOUT, pause.entered.notified())
        .await
        .expect("sender must reach the transfer checkpoint");

    let strict_mode_config = StrictModeConfig {
        enabled: Some(true),
        ..Default::default()
    };
    let (writer_result, _) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            collection.update_strict_mode_config(strict_mode_config),
            collection.state(),
        )
    })
    .await
    .expect("collection writer and reader must complete while the transfer is paused");

    writer_result.unwrap();
    assert!(
        !transfer_task.is_finished(),
        "transfer must still be paused"
    );

    // Stop at the checkpoint rather than performing consensus synchronization against a mock.
    pause.release.notify_one();
    let result = tokio::time::timeout(TEST_TIMEOUT, transfer_task)
        .await
        .expect("sender must return after releasing the checkpoint")
        .unwrap();
    let error = result.expect_err("mock consensus must stop the transfer");
    assert!(error.to_string().contains("test checkpoint released"));

    collection
        .shards_holder()
        .read()
        .await
        .get_shard(SHARD)
        .unwrap()
        .un_proxify_local()
        .await
        .unwrap();
    server_task.abort();
}

#[derive(Clone)]
struct MockPeer {
    recovery_pause: Option<Arc<TransferPause>>,
    recovery_point: RecoveryPoint,
}

#[tonic::async_trait]
impl Qdrant for MockPeer {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        Ok(Response::new(HealthCheckReply {
            title: "test".to_string(),
            version: "1.12.0".to_string(),
            commit: None,
        }))
    }
}

#[tonic::async_trait]
impl CollectionsInternal for MockPeer {
    async fn initiate(
        &self,
        _request: Request<InitiateShardTransferRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        Ok(Response::new(CollectionOperationResponse {
            result: true,
            time: 0.0,
        }))
    }

    async fn get_shard_recovery_point(
        &self,
        _request: Request<GetShardRecoveryPointRequest>,
    ) -> Result<Response<GetShardRecoveryPointResponse>, Status> {
        Ok(Response::new(GetShardRecoveryPointResponse {
            recovery_point: Some(self.recovery_point.clone()),
            time: 0.0,
        }))
    }

    async fn get(
        &self,
        _request: Request<GetCollectionInfoRequestInternal>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }

    async fn wait_for_shard_state(
        &self,
        _request: Request<WaitForShardStateRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }

    async fn update_shard_cutoff_point(
        &self,
        _request: Request<UpdateShardCutoffPointRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }

    async fn get_shard_optimizations(
        &self,
        _request: Request<GetShardOptimizationsRequest>,
    ) -> Result<Response<GetShardOptimizationsResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }

    async fn get_shard_memory_report(
        &self,
        _request: Request<GetShardMemoryReportRequest>,
    ) -> Result<Response<GetShardMemoryReportResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }
}

#[tonic::async_trait]
impl ShardSnapshots for MockPeer {
    async fn recover(
        &self,
        _request: Request<RecoverShardSnapshotRequest>,
    ) -> Result<Response<RecoverSnapshotResponse>, Status> {
        if let Some(pause) = &self.recovery_pause {
            pause.wait().await;
        }
        Ok(Response::new(RecoverSnapshotResponse { time: 0.0 }))
    }

    async fn create(
        &self,
        _request: Request<CreateShardSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }

    async fn list(
        &self,
        _request: Request<ListShardSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }

    async fn delete(
        &self,
        _request: Request<DeleteShardSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("not exercised by sender lock tests"))
    }
}

struct TestConsensus {
    pause: Option<Arc<TransferPause>>,
}

#[async_trait::async_trait]
impl ShardTransferConsensus for TestConsensus {
    fn this_peer_id(&self) -> PeerId {
        THIS_PEER
    }
    fn peers(&self) -> Vec<PeerId> {
        vec![THIS_PEER, REMOTE_PEER]
    }
    fn consensus_commit_term(&self) -> (u64, u64) {
        (1, 1)
    }
    fn is_leader_established(&self) -> bool {
        true
    }

    async fn recovered_switch_to_partial_confirm_remote(
        &self,
        _transfer_config: &ShardTransfer,
        _collection_id: &CollectionId,
        _remote_shard: &RemoteShard,
    ) -> CollectionResult<()> {
        if let Some(pause) = &self.pause {
            pause.wait().await;
        }
        Err(CollectionError::service_error("test checkpoint released"))
    }

    fn recovered_switch_to_partial(
        &self,
        _transfer_config: &ShardTransfer,
        _collection_id: CollectionId,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }

    async fn start_shard_transfer(
        &self,
        _transfer_config: ShardTransfer,
        _collection_id: CollectionId,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }

    async fn restart_shard_transfer(
        &self,
        _transfer_config: ShardTransfer,
        _collection_id: CollectionId,
        _default_method: ShardTransferMethod,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }

    async fn abort_shard_transfer(
        &self,
        _transfer: ShardTransferKey,
        _collection_id: CollectionId,
        _reason: &str,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }

    async fn set_shard_replica_set_state(
        &self,
        _peer_id: Option<PeerId>,
        _collection_id: CollectionId,
        _shard_id: ShardId,
        _state: ReplicaState,
        _from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }

    async fn commit_read_hashring(
        &self,
        _collection_id: CollectionId,
        _reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }

    async fn commit_write_hashring(
        &self,
        _collection_id: CollectionId,
        _reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by sender lock tests")
    }
}
