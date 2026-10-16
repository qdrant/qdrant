use collection::operations::types::CollectionError;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use tempfile::Builder;

use crate::common::{NoopReshardingConsensus, simple_collection_fixture};

#[tokio::test]
async fn transfer_start_rejection_does_not_register_transfer() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 2).await;
    let transfer = ShardTransfer {
        shard_id: 0,
        to_shard_id: Some(1),
        from: 0,
        to: 0,
        sync: true,
        method: Some(ShardTransferMethod::ReshardingStreamRecords),
        filter: None,
    };

    let result = collection
        .start_shard_transfer(
            transfer.clone(),
            Box::new(NoopReshardingConsensus),
            dir.path().join("transfer-temp"),
            std::future::ready(()),
            std::future::ready(()),
        )
        .await;

    assert!(matches!(result, Err(CollectionError::BadInput { .. })));
    assert!(collection.state().await.transfers.is_empty());
}
