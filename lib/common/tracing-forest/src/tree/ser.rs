use crate::tree::FieldSet;
#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
use serde::ser::{SerializeMap, Serializer};
use std::time::Duration;
use tracing::Level;

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(super) fn level<S: Serializer>(level: &Level, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(level.as_str())
}

pub(super) fn nanos<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u128(duration.as_nanos())
}

pub(super) fn fields<S: Serializer>(fields: &FieldSet, serializer: S) -> Result<S::Ok, S::Error> {
    let mut model = serializer.serialize_map(Some(fields.len()))?;
    for field in fields {
        model.serialize_entry(field.key(), field.value())?;
    }
    model.end()
}

#[cfg(feature = "chrono")]
pub(super) fn timestamp<S: Serializer>(
    timestamp: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&timestamp.to_rfc3339())
}
