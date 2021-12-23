use std::sync::Arc;
use tempdir::TempDir;
use parking_lot::RwLock;
use crate::collection_manager::fixtures::{get_merge_optimizer, random_segment};
use crate::collection_manager::holders::segment_holder::{SegmentHolder, SegmentId};
use crate::update_handler::{Optimizer, UpdateHandler};

#[tokio::test]
async fn test_optimization_process() {
    let dir = TempDir::new("segment_dir").unwrap();
    let temp_dir = TempDir::new("segment_temp_dir").unwrap();

    let mut holder = SegmentHolder::default();

    let mut segments_to_merge = vec![];

    segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));
    segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));
    segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));

    let mut other_segment_ids: Vec<SegmentId> = vec![];

    other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
    other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
    other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));

    let merge_optimizer: Arc<Optimizer> = Arc::new(get_merge_optimizer(dir.path(), temp_dir.path()));

    let optimizers = Arc::new(vec![merge_optimizer]);

    let segments = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::process_optimization(optimizers.clone(), segments.clone());

    assert_eq!(handles.len(), 1);
}