use crate::entry::entry_point::OperationResult;
use crate::types::{PointIdType, PointOffsetType};

/// Trait for point ids mapper.
///
/// This mapper is used to convert external (i.e. user-facing) point id into internal point id.
/// Internal ids are useful for contiguous-ness
pub trait IdMapper {
    /// Returns internal ID of the point, which is used inside this segment
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType>;

    /// Return external ID for internal point, defined by user
    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType>;

    /// Set mapping
    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()>;

    /// Drop mapping
    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()>;

    /// Iterate over all external ids
    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;

    /// Iterate starting from a given ID
    fn iter_from(
        &self,
        external_id: PointIdType,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_>;

    /// Force persistence of current mapper state.
    fn flush(&self) -> OperationResult<()>;
}
