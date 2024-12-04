use std::ops::Range;
use std::sync::atomic::AtomicU32;

use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;

/// Pending point params (entry and level).
#[derive(Debug)]
pub struct PointLinkingData {
    pub point_id: PointOffsetType,
    pub level: usize,
    pub entry: AtomicU32,
}

/// Batch of points to insert in one shader invocation.
#[derive(Debug)]
pub struct Batch<'a> {
    pub points: &'a [PointLinkingData],
    pub level: usize,
}

/// Batched points for GPU processing.
/// Each batch is one shader invocation.
/// All points in one batch have the same level.
/// Size of batch is limited by subgroups count.
pub struct BatchedPoints {
    points: Vec<PointLinkingData>,
    batches: Vec<Range<usize>>,
    first_point_id: PointOffsetType,
    levels_count: usize,
    remap: Vec<PointOffsetType>,
}

impl BatchedPoints {
    pub fn new(
        level_fn: impl Fn(PointOffsetType) -> usize,
        mut ids: Vec<PointOffsetType>,
        groups_count: usize,
    ) -> OperationResult<Self> {
        Self::sort_points_by_level(&level_fn, &mut ids);

        let mut remap = vec![0; ids.iter().max().copied().unwrap_or_default() as usize + 1];
        for (remapped_id, id) in ids.iter().enumerate() {
            remap[*id as usize] = remapped_id as PointOffsetType;
        }

        let first_point_id = ids.remove(0);

        let batches = Self::build_initial_batches(&level_fn, &ids, groups_count);

        let mut points = Vec::with_capacity(ids.len());
        for batch in batches.iter() {
            for i in batch.clone() {
                let point_id = ids[i];
                let level = level_fn(point_id);
                points.push(PointLinkingData {
                    point_id,
                    level,
                    entry: first_point_id.into(),
                });
            }
        }

        Ok(Self {
            points,
            batches,
            first_point_id,
            levels_count: level_fn(first_point_id) + 1,
            remap,
        })
    }

    pub fn first_point_id(&self) -> PointOffsetType {
        self.first_point_id
    }

    pub fn levels_count(&self) -> usize {
        self.levels_count
    }

    pub fn remap(&self) -> &[PointOffsetType] {
        &self.remap
    }

    pub fn points(&self) -> &[PointLinkingData] {
        &self.points
    }

    pub fn iter_batches(&self, skip_count: usize) -> impl Iterator<Item = Batch> {
        self.batches
            .iter()
            .filter(move |batch| batch.end > skip_count)
            .map(move |batch| {
                let intersected_batch = std::cmp::max(batch.start, skip_count)..batch.end;
                let level = self.points[intersected_batch.start].level;
                Batch {
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

        batches
    }
}
