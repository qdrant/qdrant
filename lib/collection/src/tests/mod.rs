use crate::collection_manager::fixtures::{
    get_indexing_optimizer, get_merge_optimizer, random_segment,
};
use crate::collection_manager::holders::segment_holder::{SegmentHolder, SegmentId};
use crate::update_handler::{Optimizer, UpdateHandler};
use futures::future::join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use std::sync::Arc;
use tempdir::TempDir;

#[tokio::test]
async fn test_optimization_process() {
    let dir = TempDir::new("segment_dir").unwrap();
    let temp_dir = TempDir::new("segment_temp_dir").unwrap();

    let mut holder = SegmentHolder::default();

    let segments_to_merge = vec![
        holder.add(random_segment(dir.path(), 100, 3, 4)),
        holder.add(random_segment(dir.path(), 100, 3, 4)),
        holder.add(random_segment(dir.path(), 100, 3, 4)),
    ];

    let segment_to_index = holder.add(random_segment(dir.path(), 100, 110, 4));

    let _other_segment_ids: Vec<SegmentId> = vec![
        holder.add(random_segment(dir.path(), 100, 20, 4)),
        holder.add(random_segment(dir.path(), 100, 20, 4)),
    ];

    let merge_optimizer: Arc<Optimizer> =
        Arc::new(get_merge_optimizer(dir.path(), temp_dir.path()));
    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path()));

    let optimizers = Arc::new(vec![merge_optimizer, indexing_optimizer]);

    let segments = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(optimizers.clone(), segments.clone());

    assert_eq!(handles.len(), 2);

    let join_res = join_all(handles.into_iter().map(|x| x.join_handle).collect_vec()).await;

    let handles_2 = UpdateHandler::launch_optimization(optimizers.clone(), segments.clone());

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
