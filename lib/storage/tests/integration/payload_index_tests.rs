use std::convert::Infallible;
use std::future::{Ready, ready};
use std::task::{Context, Poll};

use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::PointOperations;
use collection::operations::types::CollectionError;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::channel_service::ChannelService;
use collection::shards::remote_shard::RemoteShard;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard_trait::WaitUntil;
use common::ambient::{AmbientContext, AmbientFutureExt};
use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, PayloadSchemaType};
use storage::content_manager::CollectionContainer;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreatePayloadIndex, DropPayloadIndex,
};
use storage::content_manager::consensus_state_machine::Action;
use storage::content_manager::errors::StorageResult;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::AccessRequirements;
use tokio_util::task::AbortOnDropHandle;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic::{Code, Status};

use super::alias_tests::{FULL_ACCESS, create_collection, new_dispatcher};

#[test]
fn payload_index_legacy_forward_failure() {
    for code in [Code::NotFound, Code::FailedPrecondition] {
        payload_index_forward_failure(false, code);
    }
}

#[test]
fn payload_index_action_forward_failure() {
    for code in [Code::NotFound, Code::FailedPrecondition] {
        payload_index_forward_failure(true, code);
    }
}

#[test]
fn payload_index_committed_updates_bypass_write_limiter() {
    use collection::operations::point_ops::{
        PointInsertOperationsInternal, PointStructPersisted, VectorStructPersisted,
    };
    use common::ambient::hw::HwMetric;
    use storage::content_manager::consensus_state_machine::CollectionConfigDiff;

    for use_applier in [false, true] {
        for proxy in [false, true] {
            let (_storage_dir, handle, dispatcher) = new_dispatcher();
            create_collection(&handle, &dispatcher, "test");
            let pass = new_unchecked_verification_pass();
            let toc = dispatcher.toc(&FULL_ACCESS, &pass);
            toc.apply_action(Action::SetReplicaState {
                collection: "test".into(),
                shard_id: 0,
                peer_id: 0,
                state: ReplicaState::Active,
            })
            .unwrap();
            toc.apply_action(Action::UpdateCollectionConfig {
                collection: "test".into(),
                diff: Box::new(CollectionConfigDiff::StrictMode(
                    serde_json::from_value(serde_json::json!({
                        "enabled": true, "write_rate_limit": 1,
                    }))
                    .unwrap(),
                )),
            })
            .unwrap();
            let collection_pass = FULL_ACCESS
                .check_collection_access("test", AccessRequirements::new(), "test")
                .unwrap();
            let collection = handle
                .block_on(toc.get_collection(&collection_pass))
                .unwrap();
            let replica_set = handle.block_on(async {
                collection
                    .shards_holder()
                    .read()
                    .await
                    .get_shard(0)
                    .unwrap()
                    .clone()
            });
            let segments = handle
                .block_on(replica_set.local_segments_for_test())
                .unwrap();
            let metrics = toc.get_collection_hw_metrics("test".into());
            let point = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                    id: 1.into(),
                    vector: VectorStructPersisted::Single(vec![1.0; 10]),
                    payload: Some(
                        serde_json::from_value(serde_json::json!({"city": "Madrid"})).unwrap(),
                    ),
                }]),
            ));
            let result = handle.block_on(
                replica_set
                    .update_local(point.clone().into(), WaitUntil::Visible, None, false)
                    .measured(AmbientContext::request("test", metrics.clone())),
            );
            result.unwrap();
            let before = metrics.load()[HwMetric::PayloadIndexIoWrite];
            let _server = if proxy {
                let (remote, server) = missing_index_target(&handle, Code::NotFound);
                handle
                    .block_on(replica_set.proxify_local(remote, None, None))
                    .unwrap();
                Some(server)
            } else {
                None
            };
            let field_name = "city".parse().unwrap();

            for _ in 0..2 {
                apply_index(toc, &field_name, true, use_applier).unwrap();
            }

            handle.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        let indexed = segments.read().iter().all(|(_, segment)| {
                            segment
                                .get()
                                .read()
                                .get_indexed_fields()
                                .contains_key(&field_name)
                        });
                        if indexed && metrics.load()[HwMetric::PayloadIndexIoWrite] > before {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("committed index must reach segments and retain hardware measurements");
            });
            let result = handle.block_on(
                replica_set
                    .update_local(point.into(), WaitUntil::Wal, None, false)
                    .measured(AmbientContext::request("test", metrics)),
            );
            assert!(
                matches!(result, Err(CollectionError::RateLimitExceeded { .. })),
                "point writes must remain rate limited: {result:?}"
            );
            if proxy {
                handle.block_on(replica_set.un_proxify_local()).unwrap();
            }
        }
    }
}

fn payload_index_forward_failure(use_applier: bool, code: Code) {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.apply_action(Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 0,
        peer_id: 0,
        state: ReplicaState::Active,
    })
    .unwrap();
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let replica_set = handle.block_on(async {
        collection
            .shards_holder()
            .read()
            .await
            .get_shard(0)
            .unwrap()
            .clone()
    });
    let segments = handle
        .block_on(replica_set.local_segments_for_test())
        .unwrap();
    let (remote, _server) = missing_index_target(&handle, code);
    handle
        .block_on(replica_set.proxify_local(remote, None, None))
        .unwrap();

    // The target rejects forwarding after the proxy has applied the local update
    let field_name: JsonPath = "city".parse().unwrap();
    for create in [true, false] {
        for _ in 0..2 {
            apply_index(toc, &field_name, create, use_applier).unwrap();

            let state = toc.collection_state("test").unwrap();
            assert_eq!(
                state.payload_index_schema.schema.contains_key(&field_name),
                create
            );
            assert!(segments.read().iter().all(|(_, segment)| {
                segment
                    .get()
                    .read()
                    .get_indexed_fields()
                    .contains_key(&field_name)
                    == create
            }));
            assert!(handle.block_on(replica_set.is_proxy()));
        }
    }

    // Point updates must still report a failed forward
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: vec![1.into()],
    });
    let context = AmbientContext::request("test", toc.get_collection_hw_metrics("test".into()));
    let result = handle.block_on(
        collection
            .update_all_local(operation, WaitUntil::Wal, false)
            .measured(context),
    );

    let Err(CollectionError::ForwardProxyError { error, .. }) = result else {
        panic!("point update must report a forwarding error, got {result:?}");
    };
    assert!(matches!(
        (code, *error),
        (Code::NotFound, CollectionError::NotFound { .. })
            | (
                Code::FailedPrecondition,
                CollectionError::PreConditionFailed { .. }
            )
    ));
    handle.block_on(replica_set.un_proxify_local()).unwrap();
}

fn apply_index(
    toc: &TableOfContent,
    field_name: &JsonPath,
    create: bool,
    use_applier: bool,
) -> StorageResult<()> {
    let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    if use_applier {
        let action = if create {
            Action::SetPayloadIndex {
                collection: "test".into(),
                field_name: field_name.clone(),
                field_schema,
            }
        } else {
            Action::DropPayloadIndex {
                collection: "test".into(),
                field_name: field_name.clone(),
            }
        };
        toc.apply_action(action)
    } else {
        let operation = if create {
            CollectionMetaOperations::CreatePayloadIndex(CreatePayloadIndex {
                collection_name: "test".into(),
                field_name: field_name.clone(),
                field_schema,
            })
        } else {
            CollectionMetaOperations::DropPayloadIndex(DropPayloadIndex {
                collection_name: "test".into(),
                field_name: field_name.clone(),
            })
        };
        CollectionContainer::perform_collection_meta_op(toc, operation).map(|_| ())
    }
}
fn missing_index_target(
    handle: &tokio::runtime::Handle,
    code: Code,
) -> (RemoteShard, AbortOnDropHandle<()>) {
    let listener = handle
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let stream = listener.accept().await.map(|(stream, _)| stream);
        Some((stream, listener))
    });
    let server = handle.spawn(async move {
        Server::builder()
            .add_service(MissingIndexTarget(code))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let channel_service = ChannelService::new(6333, false, None, None);
    channel_service
        .id_to_address
        .write()
        .insert(1, format!("http://{address}").parse().unwrap());
    let remote = RemoteShard::new(0, "test".into(), 1, channel_service);

    (remote, AbortOnDropHandle::new(server))
}

#[derive(Clone)]
struct MissingIndexTarget(Code);

impl Service<http::Request<tonic::body::Body>> for MissingIndexTarget {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<tonic::body::Body>) -> Self::Future {
        ready(Ok(Status::new(
            self.0,
            "missing collection or target shard",
        )
        .into_http()))
    }
}

impl NamedService for MissingIndexTarget {
    const NAME: &'static str = "qdrant.PointsInternal";
}
