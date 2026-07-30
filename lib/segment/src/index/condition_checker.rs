use common::condition_checker::{
    CheckItem, ConditionChecker, ConstantConditionChecker, Rest, Select, default_check_batched,
};
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use common::universal_io::MmapFile;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::bool_index::{
    BoolConditionChecker, ImmutableBoolIndex, MutableBoolIndex, ReadOnlyBoolIndex,
};
use crate::index::field_index::full_text_index::{
    FullTextConditionChecker, FullTextIndex, ReadOnlyFullTextIndex,
};
use crate::index::field_index::geo_index::{GeoConditionChecker, GeoIndex, ReadOnlyGeoIndex};
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::map_index::{MapConditionChecker, MapIndex};
use crate::index::field_index::null_index::{
    ImmutableNullIndex, MutableNullIndex, NullConditionChecker, ReadOnlyNullIndex,
};
use crate::index::field_index::numeric_index::{
    NumericIndexInner, RangeConditionChecker, ReadOnlyNumericIndexInner,
};
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
#[cfg(feature = "testing")]
use crate::index::plain_payload_index::PlainFilterContext;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::struct_payload_index::IdsConditionChecker;
use crate::types::{
    FloatPayloadType, GeoBoundingBox, GeoRadius, IntPayloadType, PolygonWrapper, UuidIntType,
};

type BoolCC<'a, Idx> = BoolConditionChecker<'a, Idx>;
type NullCC<'a, Idx> = NullConditionChecker<'a, Idx>;
type GeoCC<'a, R, C> = GeoConditionChecker<'a, R, C>;
type RangeRwCC<'a, T> = RangeConditionChecker<'a, NumericIndexInner<T>, T>;
type RangeRoCC<'a, S, T> = RangeConditionChecker<'a, ReadOnlyNumericIndexInner<T, S>, T>;
type FullTextCC<'a, Idx> = FullTextConditionChecker<'a, Idx>;
type MapWrCC<'a, T> = MapConditionChecker<'a, T, MapIndex<T>>;
type MapRoCC<'a, S, T> = MapConditionChecker<'a, T, ReadOnlyMapIndex<T, S>>;

/// All [`ConditionChecker`] implementations used in this [crate].
pub enum ConditionCheckerEnum<'a> {
    Build(BuildConditionChecker<'a>),
    Constant(ConstantConditionChecker<OperationError>),
    Dyn(Box<dyn ConditionChecker<Error = OperationError> + 'a>),
    Filter(OptimizedFilter<'a>),
    Ids(IdsConditionChecker),
    #[cfg(feature = "testing")]
    Plain(PlainFilterContext<'a>),
    #[cfg(feature = "testing")]
    TestBitOfId(TestBitOfId),

    // bool index
    BoolImmutable(BoolCC<'a, ImmutableBoolIndex>),
    BoolMutable(BoolCC<'a, MutableBoolIndex>),
    #[cfg(target_os = "linux")]
    BoolRoIoUring(BoolCC<'a, ReadOnlyBoolIndex<IoUringFile>>),
    BoolRoMmap(BoolCC<'a, ReadOnlyBoolIndex<MmapFile>>),

    // null index
    NullImmutable(NullCC<'a, ImmutableNullIndex>),
    NullMutable(NullCC<'a, MutableNullIndex>),
    #[cfg(target_os = "linux")]
    NullRoIoUring(NullCC<'a, ReadOnlyNullIndex<IoUringFile>>),
    NullRoMmap(NullCC<'a, ReadOnlyNullIndex<MmapFile>>),

    // geo index
    GeoRadiusWritable(GeoCC<'a, GeoIndex, GeoRadius>),
    GeoBoundingBoxWritable(GeoCC<'a, GeoIndex, GeoBoundingBox>),
    GeoPolygonWritable(GeoCC<'a, GeoIndex, PolygonWrapper>),

    #[cfg(target_os = "linux")]
    GeoRadiusRoIoUring(GeoCC<'a, ReadOnlyGeoIndex<IoUringFile>, GeoRadius>),
    #[cfg(target_os = "linux")]
    GeoBoundingBoxRoIoUring(GeoCC<'a, ReadOnlyGeoIndex<IoUringFile>, GeoBoundingBox>),
    #[cfg(target_os = "linux")]
    GeoPolygonRoIoUring(GeoCC<'a, ReadOnlyGeoIndex<IoUringFile>, PolygonWrapper>),

    GeoRadiusRoMmap(GeoCC<'a, ReadOnlyGeoIndex<MmapFile>, GeoRadius>),
    GeoBoundingBoxRoMmap(GeoCC<'a, ReadOnlyGeoIndex<MmapFile>, GeoBoundingBox>),
    GeoPolygonRoMmap(GeoCC<'a, ReadOnlyGeoIndex<MmapFile>, PolygonWrapper>),

    // numeric index
    NumericFloatWritable(RangeRwCC<'a, FloatPayloadType>),
    NumericIntWritable(RangeRwCC<'a, IntPayloadType>),
    NumericUuidWritable(RangeRwCC<'a, UuidIntType>),

    #[cfg(target_os = "linux")]
    NumericFloatRoIoUring(RangeRoCC<'a, IoUringFile, FloatPayloadType>),
    #[cfg(target_os = "linux")]
    NumericIntRoIoUring(RangeRoCC<'a, IoUringFile, IntPayloadType>),
    #[cfg(target_os = "linux")]
    NumericUuidRoIoUring(RangeRoCC<'a, IoUringFile, UuidIntType>),

    NumericFloatRoMmap(RangeRoCC<'a, MmapFile, FloatPayloadType>),
    NumericIntRoMmap(RangeRoCC<'a, MmapFile, IntPayloadType>),
    NumericUuidRoMmap(RangeRoCC<'a, MmapFile, UuidIntType>),

    // full-text index
    FullTextWritable(FullTextCC<'a, FullTextIndex>),
    #[cfg(target_os = "linux")]
    FullTextRoIoUring(FullTextCC<'a, ReadOnlyFullTextIndex<IoUringFile>>),
    FullTextRoMmap(FullTextCC<'a, ReadOnlyFullTextIndex<MmapFile>>),

    // map index
    MapIntWritable(MapWrCC<'a, IntPayloadType>),
    MapStrWritable(MapWrCC<'a, str>),
    MapUuidWritable(MapWrCC<'a, UuidIntType>),

    #[cfg(target_os = "linux")]
    MapIntRoIoUring(MapRoCC<'a, IoUringFile, IntPayloadType>),
    #[cfg(target_os = "linux")]
    MapStrRoIoUring(MapRoCC<'a, IoUringFile, str>),
    #[cfg(target_os = "linux")]
    MapUuidRoIoUring(MapRoCC<'a, IoUringFile, UuidIntType>),

    MapIntRoMmap(MapRoCC<'a, MmapFile, IntPayloadType>),
    MapStrRoMmap(MapRoCC<'a, MmapFile, str>),
    MapUuidRoMmap(MapRoCC<'a, MmapFile, UuidIntType>),
}

impl ConditionChecker for ConditionCheckerEnum<'_> {
    type Error = OperationError;

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        match self {
            Self::Dyn(c) => default_check_batched(ids, select, rest, |id| c.check(id)),
            Self::Build(c) => c.check_batched(ids, select, rest),
            Self::Constant(c) => c.check_batched(ids, select, rest),
            Self::Filter(c) => c.check_batched(ids, select, rest),
            Self::Ids(c) => c.check_batched(ids, select, rest),
            #[cfg(feature = "testing")]
            Self::Plain(c) => c.check_batched(ids, select, rest),
            #[cfg(feature = "testing")]
            Self::TestBitOfId(c) => c.check_batched(ids, select, rest),
            Self::BoolImmutable(c) => c.check_batched(ids, select, rest),
            Self::BoolMutable(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::BoolRoIoUring(c) => c.check_batched(ids, select, rest),
            Self::BoolRoMmap(c) => c.check_batched(ids, select, rest),
            Self::NullImmutable(c) => c.check_batched(ids, select, rest),
            Self::NullMutable(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::NullRoIoUring(c) => c.check_batched(ids, select, rest),
            Self::NullRoMmap(c) => c.check_batched(ids, select, rest),
            Self::GeoRadiusWritable(c) => c.check_batched(ids, select, rest),
            Self::GeoBoundingBoxWritable(c) => c.check_batched(ids, select, rest),
            Self::GeoPolygonWritable(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::GeoRadiusRoIoUring(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::GeoBoundingBoxRoIoUring(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::GeoPolygonRoIoUring(c) => c.check_batched(ids, select, rest),
            Self::GeoRadiusRoMmap(c) => c.check_batched(ids, select, rest),
            Self::GeoBoundingBoxRoMmap(c) => c.check_batched(ids, select, rest),
            Self::GeoPolygonRoMmap(c) => c.check_batched(ids, select, rest),
            Self::NumericFloatWritable(c) => c.check_batched(ids, select, rest),
            Self::NumericIntWritable(c) => c.check_batched(ids, select, rest),
            Self::NumericUuidWritable(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::NumericFloatRoIoUring(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::NumericIntRoIoUring(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::NumericUuidRoIoUring(c) => c.check_batched(ids, select, rest),
            Self::NumericFloatRoMmap(c) => c.check_batched(ids, select, rest),
            Self::NumericIntRoMmap(c) => c.check_batched(ids, select, rest),
            Self::NumericUuidRoMmap(c) => c.check_batched(ids, select, rest),
            Self::FullTextWritable(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::FullTextRoIoUring(c) => c.check_batched(ids, select, rest),
            Self::FullTextRoMmap(c) => c.check_batched(ids, select, rest),
            Self::MapIntWritable(c) => c.check_batched(ids, select, rest),
            Self::MapStrWritable(c) => c.check_batched(ids, select, rest),
            Self::MapUuidWritable(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::MapIntRoIoUring(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::MapStrRoIoUring(c) => c.check_batched(ids, select, rest),
            #[cfg(target_os = "linux")]
            Self::MapUuidRoIoUring(c) => c.check_batched(ids, select, rest),
            Self::MapIntRoMmap(c) => c.check_batched(ids, select, rest),
            Self::MapStrRoMmap(c) => c.check_batched(ids, select, rest),
            Self::MapUuidRoMmap(c) => c.check_batched(ids, select, rest),
        }
    }

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        match self {
            Self::Build(c) => c.check(point_id),
            Self::Constant(c) => c.check(point_id),
            Self::Dyn(c) => c.check(point_id),
            Self::Filter(c) => c.check(point_id),
            Self::Ids(c) => c.check(point_id),
            #[cfg(feature = "testing")]
            Self::Plain(c) => c.check(point_id),
            #[cfg(feature = "testing")]
            Self::TestBitOfId(c) => c.check(point_id),
            Self::BoolImmutable(c) => c.check(point_id),
            Self::BoolMutable(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::BoolRoIoUring(c) => c.check(point_id),
            Self::BoolRoMmap(c) => c.check(point_id),
            Self::NullImmutable(c) => c.check(point_id),
            Self::NullMutable(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::NullRoIoUring(c) => c.check(point_id),
            Self::NullRoMmap(c) => c.check(point_id),
            Self::GeoRadiusWritable(c) => c.check(point_id),
            Self::GeoBoundingBoxWritable(c) => c.check(point_id),
            Self::GeoPolygonWritable(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::GeoRadiusRoIoUring(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::GeoBoundingBoxRoIoUring(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::GeoPolygonRoIoUring(c) => c.check(point_id),
            Self::GeoRadiusRoMmap(c) => c.check(point_id),
            Self::GeoBoundingBoxRoMmap(c) => c.check(point_id),
            Self::GeoPolygonRoMmap(c) => c.check(point_id),
            Self::NumericFloatWritable(c) => c.check(point_id),
            Self::NumericIntWritable(c) => c.check(point_id),
            Self::NumericUuidWritable(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::NumericFloatRoIoUring(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::NumericIntRoIoUring(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::NumericUuidRoIoUring(c) => c.check(point_id),
            Self::NumericFloatRoMmap(c) => c.check(point_id),
            Self::NumericIntRoMmap(c) => c.check(point_id),
            Self::NumericUuidRoMmap(c) => c.check(point_id),
            Self::FullTextWritable(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::FullTextRoIoUring(c) => c.check(point_id),
            Self::FullTextRoMmap(c) => c.check(point_id),
            Self::MapIntWritable(c) => c.check(point_id),
            Self::MapStrWritable(c) => c.check(point_id),
            Self::MapUuidWritable(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::MapIntRoIoUring(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::MapStrRoIoUring(c) => c.check(point_id),
            #[cfg(target_os = "linux")]
            Self::MapUuidRoIoUring(c) => c.check(point_id),
            Self::MapIntRoMmap(c) => c.check(point_id),
            Self::MapStrRoMmap(c) => c.check(point_id),
            Self::MapUuidRoMmap(c) => c.check(point_id),
        }
    }

    fn check_infallible(&self, point_id: PointOffsetType) -> bool {
        match self {
            Self::Build(c) => c.check_infallible(point_id),
            Self::Constant(c) => c.check_infallible(point_id),
            Self::Dyn(c) => c.check_infallible(point_id),
            Self::Filter(c) => c.check_infallible(point_id),
            Self::Ids(c) => c.check_infallible(point_id),
            #[cfg(feature = "testing")]
            Self::Plain(c) => c.check_infallible(point_id),
            #[cfg(feature = "testing")]
            Self::TestBitOfId(c) => c.check_infallible(point_id),
            Self::BoolImmutable(c) => c.check_infallible(point_id),
            Self::BoolMutable(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::BoolRoIoUring(c) => c.check_infallible(point_id),
            Self::BoolRoMmap(c) => c.check_infallible(point_id),
            Self::NullImmutable(c) => c.check_infallible(point_id),
            Self::NullMutable(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::NullRoIoUring(c) => c.check_infallible(point_id),
            Self::NullRoMmap(c) => c.check_infallible(point_id),
            Self::GeoRadiusWritable(c) => c.check_infallible(point_id),
            Self::GeoBoundingBoxWritable(c) => c.check_infallible(point_id),
            Self::GeoPolygonWritable(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::GeoRadiusRoIoUring(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::GeoBoundingBoxRoIoUring(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::GeoPolygonRoIoUring(c) => c.check_infallible(point_id),
            Self::GeoRadiusRoMmap(c) => c.check_infallible(point_id),
            Self::GeoBoundingBoxRoMmap(c) => c.check_infallible(point_id),
            Self::GeoPolygonRoMmap(c) => c.check_infallible(point_id),
            Self::NumericFloatWritable(c) => c.check_infallible(point_id),
            Self::NumericIntWritable(c) => c.check_infallible(point_id),
            Self::NumericUuidWritable(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::NumericFloatRoIoUring(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::NumericIntRoIoUring(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::NumericUuidRoIoUring(c) => c.check_infallible(point_id),
            Self::NumericFloatRoMmap(c) => c.check_infallible(point_id),
            Self::NumericIntRoMmap(c) => c.check_infallible(point_id),
            Self::NumericUuidRoMmap(c) => c.check_infallible(point_id),
            Self::FullTextWritable(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::FullTextRoIoUring(c) => c.check_infallible(point_id),
            Self::FullTextRoMmap(c) => c.check_infallible(point_id),
            Self::MapIntWritable(c) => c.check_infallible(point_id),
            Self::MapStrWritable(c) => c.check_infallible(point_id),
            Self::MapUuidWritable(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::MapIntRoIoUring(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::MapStrRoIoUring(c) => c.check_infallible(point_id),
            #[cfg(target_os = "linux")]
            Self::MapUuidRoIoUring(c) => c.check_infallible(point_id),
            Self::MapIntRoMmap(c) => c.check_infallible(point_id),
            Self::MapStrRoMmap(c) => c.check_infallible(point_id),
            Self::MapUuidRoMmap(c) => c.check_infallible(point_id),
        }
    }
}

/// Point matches when its ID has specified bit set.
#[cfg(feature = "testing")]
pub struct TestBitOfId(pub u32);

#[cfg(feature = "testing")]
impl ConditionChecker for TestBitOfId {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(point_id >> self.0 & 1 == 1)
    }

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        let (mut left, mut right): (Vec<K>, Vec<K>) = ids
            .iter()
            .copied()
            .partition(|item| (item.point_id() >> self.0 & 1 == 1) == select.is_match());
        left.reverse();
        right.reverse();
        if rest == Rest::Discard
            && let Some(&poison) = left.first().or(right.first())
        {
            right.fill(poison);
        }
        ids[..left.len()].copy_from_slice(&left);
        ids[left.len()..].copy_from_slice(&right);
        Ok(left.len())
    }
}
