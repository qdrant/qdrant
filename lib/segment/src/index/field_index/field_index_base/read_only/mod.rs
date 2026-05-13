mod read_ops;

use common::universal_io::UniversalRead;

use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::null_index::ReadOnlyNullIndex;
use crate::types::{IntPayloadType, UuidIntType};

// Not yet wired into the broader read-path; lifecycle and constructors land
// in a follow-up. Variants intentionally share the `*Index` postfix to mirror
// `FieldIndex`.
#[allow(dead_code, clippy::enum_variant_names)]
pub enum ReadOnlyFieldIndex<S: UniversalRead> {
    // IntIndex(NumericIndex<IntPayloadType, IntPayloadType>),
    // DatetimeIndex(NumericIndex<IntPayloadType, DateTimePayloadType>),
    IntMapIndex(ReadOnlyMapIndex<IntPayloadType, S>),
    KeywordIndex(ReadOnlyMapIndex<str, S>),
    // FloatIndex(NumericIndex<FloatPayloadType, FloatPayloadType>),
    // GeoIndex(GeoMapIndex),
    // FullTextIndex(FullTextIndex),
    // BoolIndex(BoolIndex),
    // UuidIndex(NumericIndex<UuidIntType, UuidPayloadType>),
    UuidMapIndex(ReadOnlyMapIndex<UuidIntType, S>),
    NullIndex(ReadOnlyNullIndex<S>),
}
