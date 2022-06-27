use crate::types::PointOffsetType;

pub trait PointsIterator {

}

pub type PointsIteratorSS = dyn PointsIterator + Sync + Send;
