use crate::types::{PointIdType, PointOffsetType};

pub trait IdMapper {
    /// Returns internal ID of the point, which is used inside this segment
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType>;

    /// Return external ID for internal point, defined by user
    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType>;

    /// Set mapping
    fn set_link(&mut self, external_id: PointIdType, internal_id: PointOffsetType);

    /// Drop mapping
    fn drop(&mut self, external_id: PointIdType);
}
