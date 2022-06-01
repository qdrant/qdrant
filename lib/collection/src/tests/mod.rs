use crate::collection_manager::fixtures::{
    get_indexing_optimizer, get_merge_optimizer, random_segment,
};
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder, SegmentId};
use crate::update_handler::{Optimizer, UpdateHandler};
use futures::future::join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tempdir::TempDir;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn test_optimization_process() {
    let dir = TempDir::new("segment_dir").unwrap();
    let temp_dir = TempDir::new("segment_temp_dir").unwrap();

    let dim = 256;
    let mut holder = SegmentHolder::default();

    let segments_to_merge = vec![
        holder.add(random_segment(dir.path(), 100, 3, dim)),
        holder.add(random_segment(dir.path(), 100, 3, dim)),
        holder.add(random_segment(dir.path(), 100, 3, dim)),
    ];

    let segment_to_index = holder.add(random_segment(dir.path(), 100, 110, dim));

    let _other_segment_ids: Vec<SegmentId> = vec![
        holder.add(random_segment(dir.path(), 100, 20, dim)),
        holder.add(random_segment(dir.path(), 100, 20, dim)),
    ];

    let merge_optimizer: Arc<Optimizer> =
        Arc::new(get_merge_optimizer(dir.path(), temp_dir.path(), dim));
    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));

    let optimizers = Arc::new(vec![merge_optimizer, indexing_optimizer]);

    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(optimizers.clone(), segments.clone(), |_| {});

    assert_eq!(handles.len(), 2);

    let join_res = join_all(handles.into_iter().map(|x| x.join_handle).collect_vec()).await;

    let handles_2 =
        UpdateHandler::launch_optimization(optimizers.clone(), segments.clone(), |_| {});

    assert_eq!(handles_2.len(), 0);

    for res in join_res {
        assert!(res.is_ok());
        assert!(res.unwrap());
    }

    assert_eq!(segments.read().len(), 4);

    assert!(segments.read().get(segment_to_index).is_none());

    for sid in segments_to_merge {
        assert!(segments.read().get(sid).is_none());
    }
}

#[tokio::test]
async fn test_cancel_optimization() {
    let dir = TempDir::new("segment_dir").unwrap();
    let temp_dir = TempDir::new("segment_temp_dir").unwrap();

    let mut holder = SegmentHolder::default();
    let dim = 256;

    for _ in 0..5 {
        holder.add(random_segment(dir.path(), 100, 1000, dim));
    }

    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));

    let optimizers = Arc::new(vec![indexing_optimizer]);

    let now = Instant::now();

    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(optimizers.clone(), segments.clone(), |_| {});

    sleep(Duration::from_millis(100)).await;

    let join_handles = handles.into_iter().map(|h| h.stop()).collect_vec();

    let optimization_res = join_all(join_handles).await;

    let actual_optimization_duration = now.elapsed().as_millis();
    eprintln!(
        "actual_optimization_duration = {:#?} ms",
        actual_optimization_duration
    );

    for res in optimization_res {
        let was_finished = res.expect("Should be no errors during optimization");
        assert!(!was_finished);
    }

    for (_idx, segment) in segments.read().iter() {
        match segment {
            LockedSegment::Original(_) => {}
            LockedSegment::Proxy(_) => panic!("segment is not restored"),
        }
    }
}
