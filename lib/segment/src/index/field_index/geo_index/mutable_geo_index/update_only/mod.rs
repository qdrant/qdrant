use serde_json::Value;

use super::super::GeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{UpdateOnlyIndexKind, ValueIndexer};
use crate::types::RawGeoPoint;

/// Writes what [`MutableGeoIndex`] persists: the point's coordinates, in the
/// packed form the index stores. The geo hash structure is rebuilt from them on
/// open, so it plays no part here.
///
/// [`MutableGeoIndex`]: super::MutableGeoIndex
pub struct UpdateOnlyGeoKind;

impl UpdateOnlyIndexKind for UpdateOnlyGeoKind {
    type Stored = Vec<RawGeoPoint>;

    fn extract(&self, values: &[&Value]) -> OperationResult<Option<Self::Stored>> {
        let stored: Vec<RawGeoPoint> = <GeoIndex as ValueIndexer>::flatten_values(values)
            .into_iter()
            .map(RawGeoPoint::from)
            .collect();

        Ok((!stored.is_empty()).then_some(stored))
    }
}
