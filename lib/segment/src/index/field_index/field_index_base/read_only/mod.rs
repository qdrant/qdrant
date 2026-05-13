mod read_ops;

use common::universal_io::UniversalRead;

use crate::index::field_index::bool_index::ReadOnlyBoolIndex;
use crate::index::field_index::full_text_index::read_only_text_index::ReadOnlyFullTextIndex;
use crate::index::field_index::geo_index::ReadOnlyGeoMapIndex;
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::null_index::ReadOnlyNullIndex;
use crate::index::field_index::numeric_index::ReadOnlyNumericIndex;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, UuidIntType, UuidPayloadType,
};

// Not yet wired into the broader read-path; lifecycle and constructors land
// in a follow-up. Variants intentionally share the `*Index` postfix to mirror
// `FieldIndex`.
#[allow(dead_code, clippy::enum_variant_names)]
pub enum ReadOnlyFieldIndex<S: UniversalRead> {
    IntIndex(ReadOnlyNumericIndex<IntPayloadType, IntPayloadType, S>),
    DatetimeIndex(ReadOnlyNumericIndex<IntPayloadType, DateTimePayloadType, S>),
    IntMapIndex(ReadOnlyMapIndex<IntPayloadType, S>),
    KeywordIndex(ReadOnlyMapIndex<str, S>),
    FloatIndex(ReadOnlyNumericIndex<FloatPayloadType, FloatPayloadType, S>),
    GeoIndex(ReadOnlyGeoMapIndex<S>),
    FullTextIndex(ReadOnlyFullTextIndex<S>),
    BoolIndex(ReadOnlyBoolIndex<S>),
    UuidIndex(ReadOnlyNumericIndex<UuidIntType, UuidPayloadType, S>),
    UuidMapIndex(ReadOnlyMapIndex<UuidIntType, S>),
    NullIndex(ReadOnlyNullIndex<S>),
}
