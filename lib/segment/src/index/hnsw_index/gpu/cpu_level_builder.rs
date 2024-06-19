use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use common::types::{PointOffsetType, ScoredPointOffset};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;

use super::batched_points::PointLinkingData;
use super::gpu_search_context::GpuRequest;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::index::hnsw_index::graph_layers::GraphLayersBase;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::payload_storage::FilterContext;
use crate::vector_storage::RawScorer;

// Build level on CPU. Returns the last point id that was processed.
pub fn build_level_on_cpu<'a, T: Iterator<Item = &'a PointLinkingData> + Send>(
    pool: &ThreadPool,
    graph_layers_builder: &GraphLayersBuilder,
    level: usize,
    stopped: &AtomicBool,
    linking_points: T,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<Option<PointOffsetType>> {
    let linking_points = Mutex::new(linking_points);
    let last_processed_point_id = Mutex::new(None);

    let mut current_batch_points = vec![];
    let mut prev_batches_points = HashSet::new();

    let cpu_build_result = pool.install(|| {
        (0..graph_layers_builder.links_layers.len())
            .into_par_iter()
            .try_for_each(|_| -> OperationResult<()> {
                check_process_stopped(stopped)?;

                let linking_point = {
                    let mut linking_points = linking_points.lock().unwrap();
                    let linking_point = linking_points.next();
                    linking_point.map(|linking_point| {
                        last_processed_point_id
                            .lock()
                            .unwrap()
                            .replace(linking_point.point_id);
                        linking_point
                    })
                };

                if let Some(linking_point) = linking_point {
                    if graph_layers_builder.get_point_level(linking_point.point_id) < level {
                        update_entry_on_cpu(
                            graph_layers_builder,
                            linking_point,
                            level,
                            &points_scorer_builder,
                        )
                    } else {
                        link_point_on_cpu(
                            graph_layers_builder,
                            linking_point,
                            level,
                            &points_scorer_builder,
                        )
                    }
                } else {
                    Ok(())
                }
            })
    });

    let last_processed_point_id = *last_processed_point_id.lock().unwrap();
    match cpu_build_result {
        Ok(_) => Ok(last_processed_point_id),
        Err(OperationError::Cancelled { .. }) => Ok(last_processed_point_id),
        Err(e) => Err(e),
    }
}

fn update_entry_on_cpu<'a>(
    graph_layers_builder: &GraphLayersBuilder,
    linking_point: &'a PointLinkingData,
    level: usize,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<()> {
    let (raw_scorer, filter_context) = points_scorer_builder(linking_point.point_id)?;
    let mut points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
    let new_entry = graph_layers_builder
        .search_entry_on_level(
            linking_point.entry.load(Ordering::Relaxed),
            level,
            &mut points_scorer,
        )
        .idx;
    linking_point.entry.store(new_entry, Ordering::Relaxed);
    Ok(())
}

fn link_point_on_cpu<'a>(
    graph_layers_builder: &GraphLayersBuilder,
    linking_point: &'a PointLinkingData,
    level: usize,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<()> {
    let (raw_scorer, filter_context) = points_scorer_builder(linking_point.point_id)?;
    let mut points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
    let (patches, new_entries) = graph_layers_builder.get_patch(
        GpuRequest {
            id: linking_point.point_id,
            entry: linking_point.entry.load(Ordering::Relaxed),
        },
        level,
        points_scorer,
    );
    //todo(apply)

    graph_layers_builder.apply_patch(level, patches);

    Ok(())
}
