use std::sync::Arc;

use collection::common::stoppable_task_async::spawn_async_cancellable;
use collection::config::ShardingMethod;
use collection::operations::types::ShardTransferInfo;
use collection::shards::shard_holder::ShardHolder;
use collection::shards::transfer::ShardTransfer;
use collection::shards::transfer::transfer_tasks_pool::{
    TransferTaskItem, TransferTaskProgress, TransferTasksPool,
};
use collection::telemetry::CollectionTelemetry;
use parking_lot::Mutex;

use super::*;

fn collection(id: &str, transfers: Vec<ShardTransferInfo>) -> CollectionTelemetryEnum {
    CollectionTelemetryEnum::Full(Box::new(CollectionTelemetry {
        id: id.into(),
        init_time_ms: None,
        config: None,
        shards: None,
        transfers: Some(transfers),
        resharding: None,
        shard_clean_tasks: None,
    }))
}

fn render(transfers: Vec<ShardTransferInfo>, peer_id: Option<PeerId>) -> String {
    let telemetry = CollectionsTelemetry {
        number_of_collections: 2,
        collections: Some(vec![
            collection("test", transfers),
            collection("empty", vec![]),
        ]),
        ..Default::default()
    };
    let mut metrics = MetricsData::empty();
    telemetry.add_metrics(&mut metrics, Some("qdrant_"), peer_id);
    metrics.format_metrics()
}

#[tokio::test]
async fn failed_transfer_metric_tracks_local_sender_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();
    let mut pool = TransferTasksPool::new("test".into());
    let mut transfers = Vec::new();

    // Two failed tasks, a successful task, a running task, and an unknown task.
    for shard_id in 0..5 {
        let transfer = ShardTransfer {
            shard_id,
            to_shard_id: None,
            from: 1,
            to: 2,
            sync: true,
            method: None,
            filter: None,
        };
        holder
            .register_start_shard_transfer(transfer.clone())
            .unwrap();
        if shard_id < 4 {
            let mut task = spawn_async_cancellable(move |cancel| async move {
                if shard_id == 3 {
                    cancel.cancelled().await;
                }
                shard_id >= 2
            });
            if shard_id < 3 {
                (&mut task.join_handle).await.unwrap();
            }
            let mut progress = TransferTaskProgress::new();
            progress.set(5, 10);
            pool.add_task(
                &transfer,
                TransferTaskItem {
                    task,
                    started_at: chrono::Utc::now(),
                    progress: Arc::new(Mutex::new(progress)),
                },
            );
        }
        transfers.push(transfer);
    }

    let info = holder.get_shard_transfer_info(&pool);
    assert_eq!(
        info.iter().map(|t| t.failed).collect::<Vec<_>>(),
        [true, true, false, false, false]
    );
    assert!(
        info[0]
            .comment
            .as_deref()
            .unwrap()
            .contains("Transferring records (5/10)")
    );

    let output = render(info.clone(), Some(1));
    assert!(output.contains("# TYPE qdrant_collection_shard_transfer_failed gauge\n"));
    assert!(output.contains("qdrant_collection_shard_transfer_failed{id=\"test\"} 2\n"));
    assert!(output.contains("qdrant_collection_shard_transfer_failed{id=\"empty\"} 0\n"));
    // Existing incoming/outgoing metrics still count registered transfers.
    assert!(output.contains("qdrant_collection_shard_transfer_outgoing{id=\"test\"} 5\n"));
    assert!(output.contains("qdrant_collection_shard_transfer_incoming{id=\"test\"} 0\n"));
    assert_eq!(
        output
            .lines()
            .filter(|line| line.starts_with("# TYPE qdrant_collection_shard_transfer_"))
            .count(),
        3
    );

    // Failures are reported on the sender, not again on the receiver or without a peer ID.
    for peer_id in [Some(2), Some(3), None] {
        let output = render(info.clone(), peer_id);
        assert!(output.contains("qdrant_collection_shard_transfer_failed{id=\"test\"} 0\n"));
    }

    // This is a gauge of registered failures, not a cumulative failure counter.
    holder.register_abort_transfer(&transfers[0].key()).unwrap();
    let output = render(holder.get_shard_transfer_info(&pool), Some(1));
    assert!(output.contains("qdrant_collection_shard_transfer_failed{id=\"test\"} 1\n"));

    pool.stop_task(&transfers[3].key()).await.unwrap();
}

#[test]
fn transfer_failure_status_is_internal() {
    let info = ShardTransferInfo {
        shard_id: 1,
        to_shard_id: None,
        from: 1,
        to: 2,
        sync: true,
        method: None,
        comment: Some("transfer progress".into()),
        failed: true,
    };
    let json = serde_json::to_value(&info).unwrap();
    assert!(json.get("failed").is_none());
    assert_eq!(json["comment"], "transfer progress");
    let schema = schemars::schema_for!(ShardTransferInfo);
    assert!(
        !schema
            .schema
            .object
            .unwrap()
            .properties
            .contains_key("failed")
    );

    let remote: api::grpc::qdrant::ShardTransferTelemetry = info.into();
    let remote = ShardTransferInfo::try_from(remote).unwrap();
    assert!(!remote.failed);
    assert_eq!(remote.comment.as_deref(), Some("transfer progress"));
}
