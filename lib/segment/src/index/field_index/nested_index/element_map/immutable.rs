//! Element map of an immutable index: built once, in point order, so the start offsets
//! of the points are enough to map both ways.

use std::io::Write as _;
use std::ops::Range;
use std::path::PathBuf;

use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;

use super::{ElementMapBuilder, ElementOffsetType, ElementUpdate};
use crate::common::operation_error::{OperationError, OperationResult};

const STARTS_FILE: &str = "starts.bin";

#[derive(Debug)]
pub struct ImmutableElementMap {
    /// Start offset of the elements of each point, followed by the number of elements:
    /// point `p` owns `starts[p]..starts[p + 1]`. Never empty.
    starts: Vec<ElementOffsetType>,
    max_elements_per_point: usize,
    path: PathBuf,
}

impl ImmutableElementMap {
    /// Requires `builder` to have assigned the points in ascending order.
    pub fn create(path: PathBuf, builder: &ElementMapBuilder) -> OperationResult<Self> {
        let ElementMapBuilder {
            ranges,
            elements_count,
        } = builder;

        let mut starts = Vec::new();
        for &(point, range) in ranges {
            if (point as usize) < starts.len() {
                return Err(OperationError::service_error(format!(
                    "elements of an immutable nested index must be assigned in point order, \
                     got point {point} out of order",
                )));
            }
            // Points without elements own an empty range at the start of the next one
            starts.resize(point as usize + 1, range.start);
        }
        starts.push(*elements_count);

        common::fs::atomic_save(&path.join(STARTS_FILE), |writer| {
            writer.write_all(bytemuck::cast_slice(&starts))
        })?;
        Ok(Self::new(path, starts))
    }

    pub fn open(path: PathBuf) -> OperationResult<Option<Self>> {
        let file = path.join(STARTS_FILE);
        if !file.exists() {
            return Ok(None);
        }
        let bytes = fs_err::read(&file)?;
        if bytes.is_empty() || bytes.len() % size_of::<ElementOffsetType>() != 0 {
            return Err(OperationError::service_error(format!(
                "element map {} has an invalid size: {}",
                file.display(),
                bytes.len(),
            )));
        }
        let starts: Vec<ElementOffsetType> = bytemuck::pod_collect_to_vec(&bytes);
        if !starts.is_sorted() {
            return Err(OperationError::service_error(format!(
                "element map {} is not sorted",
                file.display(),
            )));
        }
        Ok(Some(Self::new(path, starts)))
    }

    fn new(path: PathBuf, starts: Vec<ElementOffsetType>) -> Self {
        let max_elements_per_point = starts
            .windows(2)
            .map(|window| (window[1] - window[0]) as usize)
            .max()
            .unwrap_or(0);
        Self {
            starts,
            max_elements_per_point,
            path,
        }
    }

    pub fn point_elements(&self, point: PointOffsetType) -> Range<ElementOffsetType> {
        let point = point as usize;
        match (self.starts.get(point), self.starts.get(point + 1)) {
            (Some(&start), Some(&end)) => start..end,
            (Some(_) | None, None) | (None, Some(_)) => 0..0,
        }
    }

    pub fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        if element >= self.elements_count_offset() {
            return None;
        }
        // The last point starting at or before `element`: points without elements
        // before it share its start
        let point = self.starts.partition_point(|&start| start <= element) - 1;
        Some(point as PointOffsetType)
    }

    pub fn points_upper_bound(&self) -> usize {
        self.starts.len() - 1
    }

    pub fn elements_count(&self) -> usize {
        self.elements_count_offset() as usize
    }

    fn elements_count_offset(&self) -> ElementOffsetType {
        self.starts.last().copied().unwrap_or(0)
    }

    pub fn max_elements_per_point(&self) -> usize {
        self.max_elements_per_point
    }

    pub fn assign(&self, point: PointOffsetType, count: usize) -> OperationResult<ElementUpdate> {
        let current = self.point_elements(point);
        if count == 0 {
            return Ok(ElementUpdate {
                old: current,
                new: 0..0,
            });
        }
        if count != current.len() {
            return Err(OperationError::service_error(format!(
                "can't change the number of elements of point {point} in an immutable nested \
                 index from {} to {count}",
                current.len(),
            )));
        }
        Ok(ElementUpdate {
            old: current.clone(),
            new: current,
        })
    }

    pub fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
        let mut deleted = BitVec::repeat(false, self.elements_count());
        for point in deleted_points.iter_ones() {
            let elements = self.point_elements(point as PointOffsetType);
            deleted[elements.start as usize..elements.end as usize].fill(true);
        }
        deleted
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.join(STARTS_FILE)]
    }

    pub fn wipe(self) -> OperationResult<()> {
        if self.path.exists() {
            fs_err::remove_dir_all(&self.path)?;
        }
        Ok(())
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.starts.capacity() * size_of::<ElementOffsetType>()
    }
}
