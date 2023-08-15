use std::ops::Range;
use std::sync::atomic::AtomicU32;

use ahash::HashSet;
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;

#[derive(Debug)]
pub struct PointLinkingData {
    pub point_id: PointOffsetType,
    pub level: usize,
    pub batch_index: usize,
    pub entry: AtomicU32,
}

#[derive(Debug)]
pub struct Batch<'a> {
    pub points: &'a [PointLinkingData],
    pub level: usize,
    pub end_index: usize,
}

pub struct BatchedPoints {
    pub points: Vec<PointLinkingData>,
    pub batches: Vec<Range<usize>>,
    pub ids_by_batches: Vec<HashSet<PointOffsetType>>,
    pub first_point_id: PointOffsetType,
    pub levels_count: usize,
}

impl BatchedPoints {
    pub fn new(
        level_fn: impl Fn(PointOffsetType) -> usize,
        mut ids: Vec<PointOffsetType>,
        groups_count: usize,
    ) -> OperationResult<Self> {
        Self::sort_points_by_level(&level_fn, &mut ids);
        let first_point_id = ids.remove(0);

        let batches = Self::build_initial_batches(&level_fn, &ids, groups_count);

        let mut points = Vec::with_capacity(ids.len());
        for (batch_index, batch) in batches.iter().enumerate() {
            for i in batch.clone() {
                let point_id = ids[i];
                let level = level_fn(point_id);
                points.push(PointLinkingData {
                    point_id,
                    level,
                    batch_index,
                    entry: first_point_id.into(),
                });
            }
        }

        let ids_by_batches = batches
            .iter()
            .map(|batch| {
                batch
                    .clone()
                    .map(|i| ids[i])
                    .collect::<HashSet<PointOffsetType>>()
            })
            .collect();

        Ok(Self {
            points,
            batches,
            ids_by_batches,
            first_point_id,
            levels_count: level_fn(first_point_id) + 1,
        })
    }

    pub fn is_same_batch(
        &self,
        linking_point: &PointLinkingData,
        other_point: PointOffsetType,
    ) -> bool {
        self.ids_by_batches[linking_point.batch_index].contains(&other_point)
    }

    pub fn iter_batches(&self, skip_count: usize) -> impl Iterator<Item = Batch> {
        self.batches
            .iter()
            .filter(move |batch| batch.end > skip_count)
            .map(move |batch| {
                let intersected_batch = std::cmp::max(batch.start, skip_count)..batch.end;
                let level = self.points[intersected_batch.start].level;
                Batch {
                    end_index: intersected_batch.end,
                    points: &self.points[intersected_batch],
                    level,
                }
            })
    }

    pub fn sort_points_by_level(
        level_fn: impl Fn(PointOffsetType) -> usize,
        ids: &mut [PointOffsetType],
    ) {
        ids.sort_by(|&a, &b| {
            let a_level = level_fn(a);
            let b_level = level_fn(b);
            match b_level.cmp(&a_level) {
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => a.cmp(&b),
            }
        });
    }

    fn build_initial_batches(
        level_fn: impl Fn(PointOffsetType) -> usize,
        ids: &[PointOffsetType],
        groups_count: usize,
    ) -> Vec<Range<usize>> {
        let timer = std::time::Instant::now();

        let num_vectors = ids.len();
        let mut batches: Vec<_> = (0..num_vectors.div_ceil(groups_count))
            .map(|start| {
                groups_count * start..std::cmp::min(groups_count * (start + 1), num_vectors)
            })
            .collect();

        let mut batch_index = 0usize;
        while batch_index < batches.len() {
            let batch = batches[batch_index].clone();
            let point_id = ids[batch.start];
            let batch_level = level_fn(point_id);
            for i in 1..batch.len() {
                let point_id = ids[batch.start + i];
                let level = level_fn(point_id);
                // divide batch by level. all batches must be on the same level
                if level != batch_level {
                    let batch1 = batch.start..batch.start + i;
                    let batch2 = batch.start + i..batch.end;
                    batches[batch_index] = batch1;
                    batches.insert(batch_index + 1, batch2);
                    break;
                }
            }

            batch_index += 1;
        }

        for batch_pair in batches.windows(2) {
            if batch_pair.len() == 2 {
                assert_eq!(batch_pair[0].end, batch_pair[1].start);
            }
        }

        log::trace!("Initial batchs time: {:?}", timer.elapsed());

        batches
    }
}
