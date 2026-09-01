#[cfg(test)]
use common::universal_io::ReadOnly;
use common::universal_io::{
    DiskCache, DiskCacheRemote, MmapFile, UniversalRead, UniversalReadFsAsync,
};

use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::bool_index::{BoolConditionChecker, ReadOnlyBoolIndex};
use crate::index::field_index::full_text_index::{FullTextConditionChecker, ReadOnlyFullTextIndex};
use crate::index::field_index::geo_index::{GeoConditionChecker, ReadOnlyGeoIndex};
use crate::index::field_index::map_index::MapConditionChecker;
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::null_index::{NullConditionChecker, ReadOnlyNullIndex};
use crate::index::field_index::numeric_index::{RangeConditionChecker, ReadOnlyNumericIndexInner};
use crate::types::{
    FloatPayloadType, GeoBoundingBox, GeoRadius, IntPayloadType, PolygonWrapper, UuidIntType,
};

type BoolCC<'a, S> = BoolConditionChecker<'a, ReadOnlyBoolIndex<S>>;
type EnumCC<'a> = ConditionCheckerEnum<'a>;
type FullTextCC<'a, S> = FullTextConditionChecker<'a, ReadOnlyFullTextIndex<S>>;
type GeoCC<'a, S, C> = GeoConditionChecker<'a, ReadOnlyGeoIndex<S>, C>;
type MapCC<'a, S, T> = MapConditionChecker<'a, T, ReadOnlyMapIndex<T, S>>;
type NullCC<'a, S> = NullConditionChecker<'a, ReadOnlyNullIndex<S>>;
type RangeCC<'a, S, T> = RangeConditionChecker<'a, ReadOnlyNumericIndexInner<T, S>, T>;

/// Extensions over [`UniversalRead`].
///
/// Sometimes generic-over-[`UniversalRead`] code must branch on the concrete
/// UIO type. This trait is a place for such branching. Any implementations of
/// [`UniversalRead`] used in this [crate] should implement this.
///
/// The `Fs: UniversalReadFsAsync` bound makes every read-only-segment backend
/// async-capable at the filesystem level — [`CachedFs`] (which every segment
/// open and live-reload goes through) parks `open_async` futures in its
/// prefetch pool. Notably io_uring implements no async surface and is
/// therefore not a read-only-segment backend.
///
/// [`CachedFs`]: common::universal_io::CachedFs
#[rustfmt::skip]
pub trait UniversalReadExt: UniversalRead<Fs: UniversalReadFsAsync> {
    // Methods to convert generic `BlahBlahConditionChecker<S>` -> non-generic enum.
    fn condition_checker_bool            <'a>(i: BoolCC<'a, Self>)                    -> EnumCC<'a>;
    fn condition_checker_full_text       <'a>(i: FullTextCC<'a, Self>)                -> EnumCC<'a>;
    fn condition_checker_geo_bounding_box<'a>(i: GeoCC<'a, Self, GeoBoundingBox>)     -> EnumCC<'a>;
    fn condition_checker_geo_polygon     <'a>(i: GeoCC<'a, Self, PolygonWrapper>)     -> EnumCC<'a>;
    fn condition_checker_geo_radius      <'a>(i: GeoCC<'a, Self, GeoRadius>)          -> EnumCC<'a>;
    fn condition_checker_map_int         <'a>(i: MapCC<'a, Self, IntPayloadType>)     -> EnumCC<'a>;
    fn condition_checker_map_str         <'a>(i: MapCC<'a, Self, str>)                -> EnumCC<'a>;
    fn condition_checker_map_uuid        <'a>(i: MapCC<'a, Self, UuidIntType>)        -> EnumCC<'a>;
    fn condition_checker_null            <'a>(i: NullCC<'a, Self>)                    -> EnumCC<'a>;
    fn condition_checker_numeric_float   <'a>(i: RangeCC<'a, Self, FloatPayloadType>) -> EnumCC<'a>;
    fn condition_checker_numeric_int     <'a>(i: RangeCC<'a, Self, IntPayloadType>)   -> EnumCC<'a>;
    fn condition_checker_numeric_uuid    <'a>(i: RangeCC<'a, Self, UuidIntType>)      -> EnumCC<'a>;

    // Not limited to the condition-checker-related methods, might be extended in the future.
}

#[rustfmt::skip]
impl UniversalReadExt for MmapFile {
    fn condition_checker_bool            <'a>(i: BoolCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::BoolRoMmap          (i) }
    fn condition_checker_full_text       <'a>(i: FullTextCC<'a, Self>)                -> EnumCC<'a> { EnumCC::FullTextRoMmap      (i) }
    fn condition_checker_geo_bounding_box<'a>(i: GeoCC<'a, Self, GeoBoundingBox>)     -> EnumCC<'a> { EnumCC::GeoBoundingBoxRoMmap(i) }
    fn condition_checker_geo_polygon     <'a>(i: GeoCC<'a, Self, PolygonWrapper>)     -> EnumCC<'a> { EnumCC::GeoPolygonRoMmap    (i) }
    fn condition_checker_geo_radius      <'a>(i: GeoCC<'a, Self, GeoRadius>)          -> EnumCC<'a> { EnumCC::GeoRadiusRoMmap     (i) }
    fn condition_checker_map_int         <'a>(i: MapCC<'a, Self, IntPayloadType>)     -> EnumCC<'a> { EnumCC::MapIntRoMmap        (i) }
    fn condition_checker_map_str         <'a>(i: MapCC<'a, Self, str>)                -> EnumCC<'a> { EnumCC::MapStrRoMmap        (i) }
    fn condition_checker_map_uuid        <'a>(i: MapCC<'a, Self, UuidIntType>)        -> EnumCC<'a> { EnumCC::MapUuidRoMmap       (i) }
    fn condition_checker_null            <'a>(i: NullCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::NullRoMmap          (i) }
    fn condition_checker_numeric_float   <'a>(i: RangeCC<'a, Self, FloatPayloadType>) -> EnumCC<'a> { EnumCC::NumericFloatRoMmap  (i) }
    fn condition_checker_numeric_int     <'a>(i: RangeCC<'a, Self, IntPayloadType>)   -> EnumCC<'a> { EnumCC::NumericIntRoMmap    (i) }
    fn condition_checker_numeric_uuid    <'a>(i: RangeCC<'a, Self, UuidIntType>)      -> EnumCC<'a> { EnumCC::NumericUuidRoMmap   (i) }
}

// TODO: add enum variants for these
#[rustfmt::skip]
impl<R: DiskCacheRemote> UniversalReadExt for DiskCache<R> {
    fn condition_checker_bool            <'a>(i: BoolCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_full_text       <'a>(i: FullTextCC<'a, Self>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_bounding_box<'a>(i: GeoCC<'a, Self, GeoBoundingBox>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_polygon     <'a>(i: GeoCC<'a, Self, PolygonWrapper>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_radius      <'a>(i: GeoCC<'a, Self, GeoRadius>)          -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_int         <'a>(i: MapCC<'a, Self, IntPayloadType>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_str         <'a>(i: MapCC<'a, Self, str>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_uuid        <'a>(i: MapCC<'a, Self, UuidIntType>)        -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_null            <'a>(i: NullCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_float   <'a>(i: RangeCC<'a, Self, FloatPayloadType>) -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_int     <'a>(i: RangeCC<'a, Self, IntPayloadType>)   -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_uuid    <'a>(i: RangeCC<'a, Self, UuidIntType>)      -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
}

#[cfg(test)]
#[rustfmt::skip]
impl UniversalReadExt for ReadOnly<MmapFile> {
    fn condition_checker_bool            <'a>(i: BoolCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_full_text       <'a>(i: FullTextCC<'a, Self>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_bounding_box<'a>(i: GeoCC<'a, Self, GeoBoundingBox>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_polygon     <'a>(i: GeoCC<'a, Self, PolygonWrapper>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_radius      <'a>(i: GeoCC<'a, Self, GeoRadius>)          -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_int         <'a>(i: MapCC<'a, Self, IntPayloadType>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_str         <'a>(i: MapCC<'a, Self, str>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_uuid        <'a>(i: MapCC<'a, Self, UuidIntType>)        -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_null            <'a>(i: NullCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_float   <'a>(i: RangeCC<'a, Self, FloatPayloadType>) -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_int     <'a>(i: RangeCC<'a, Self, IntPayloadType>)   -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_uuid    <'a>(i: RangeCC<'a, Self, UuidIntType>)      -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
}

#[rustfmt::skip]
impl<A: io_bridge_object_store::AsyncRead + Clone> UniversalReadExt for io_bridge_object_store::BlobFile<A> {
    fn condition_checker_bool            <'a>(i: BoolCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_full_text       <'a>(i: FullTextCC<'a, Self>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_bounding_box<'a>(i: GeoCC<'a, Self, GeoBoundingBox>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_polygon     <'a>(i: GeoCC<'a, Self, PolygonWrapper>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_radius      <'a>(i: GeoCC<'a, Self, GeoRadius>)          -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_int         <'a>(i: MapCC<'a, Self, IntPayloadType>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_str         <'a>(i: MapCC<'a, Self, str>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_uuid        <'a>(i: MapCC<'a, Self, UuidIntType>)        -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_null            <'a>(i: NullCC<'a, Self>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_float   <'a>(i: RangeCC<'a, Self, FloatPayloadType>) -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_int     <'a>(i: RangeCC<'a, Self, IntPayloadType>)   -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_uuid    <'a>(i: RangeCC<'a, Self, UuidIntType>)      -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
}
