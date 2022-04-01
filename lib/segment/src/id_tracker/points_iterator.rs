use crate::types::PointOffsetType;

pub trait PointsIterator {
    /// Number of unique records in the segment
    fn points_count(&self) -> usize;
    /// Iterate over all non-removed internal ids (offsets)
    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;
    /// Max stored ID
    fn max_id(&self) -> PointOffsetType;
}

pub type PointsIteratorSS = dyn PointsIterator + Sync + Send;
