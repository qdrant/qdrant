use std::collections::{BTreeSet, HashSet};
use std::ops::Range;

use common::bitvec::BitVec;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rand::SeedableRng;
use rand::prelude::StdRng;
use rstest::rstest;
use serde_json::json;
use tempfile::{Builder, TempDir};

use super::builders::{GeoMapIndexGridstoreBuilder, GeoMapIndexMmapBuilder};
use super::immutable_geo_index::ImmutableGeoMapIndex;
use super::mmap_geo_index::StoredGeoMapIndex;
use super::{GEO_QUERY_MAX_REGION, GeoMapIndex};
use crate::fixtures::payload_fixtures::random_geo_payload;
use crate::index::field_index::geo_hash::{GeoHash, circle_hashes, polygon_hashes};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadFieldIndex, PayloadFieldIndexRead,
    ValueIndexer,
};
use crate::json_path::JsonPath;
use crate::types::test_utils::build_polygon;
use crate::types::{
    FieldCondition, GeoBoundingBox, GeoLineString, GeoPoint, GeoPolygon, GeoRadius,
};

/// Generous default size for the deleted-points bitslice used in tests.
///
/// Must be larger than the stored mmap deletion bitslice for any test in
/// this file (which is sized to the highest point id, rounded up to a
/// `usize` boundary). 4096 bits comfortably covers all current tests.
const TEST_DELETED_BITS: usize = 4096;

/// All-zero deletion bitslice for tests that don't care about deletions.
fn empty_deleted() -> BitVec {
    BitVec::repeat(false, TEST_DELETED_BITS)
}

/// Deletion bitslice with specific points marked as deleted.
fn deleted_with(points: &[PointOffsetType]) -> BitVec {
    let mut v = empty_deleted();
    for &p in points {
        v.set(p as usize, true);
    }
    v
}

type Database = ();

#[derive(Clone, Copy, PartialEq, Debug)]
enum IndexType {
    MutableGridstore,
    Mmap,
    RamMmap,
}

enum IndexBuilder {
    MutableGridstore(GeoMapIndexGridstoreBuilder),
    Mmap(GeoMapIndexMmapBuilder),
    RamMmap(GeoMapIndexMmapBuilder),
}

impl IndexBuilder {
    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&serde_json::Value],
        hw_counter: &HardwareCounterCell,
    ) -> crate::common::operation_error::OperationResult<()> {
        match self {
            IndexBuilder::MutableGridstore(builder) => builder.add_point(id, payload, hw_counter),
            IndexBuilder::Mmap(builder) => builder.add_point(id, payload, hw_counter),
            IndexBuilder::RamMmap(builder) => builder.add_point(id, payload, hw_counter),
        }
    }

    fn finalize(self) -> crate::common::operation_error::OperationResult<GeoMapIndex> {
        match self {
            IndexBuilder::MutableGridstore(builder) => builder.finalize(),
            IndexBuilder::Mmap(builder) => builder.finalize(),
            IndexBuilder::RamMmap(builder) => {
                let GeoMapIndex::Storage(index) = builder.finalize()? else {
                    panic!("expected mmap index");
                };

                let index =
                    GeoMapIndex::Immutable(ImmutableGeoMapIndex::open_mmap(*index).unwrap());
                Ok(index)
            }
        }
    }
}

const NYC: GeoPoint = GeoPoint::new_unchecked(-73.991516, 40.75798);

const BERLIN: GeoPoint = GeoPoint::new_unchecked(13.41053, 52.52437);

const POTSDAM: GeoPoint = GeoPoint::new_unchecked(13.064473, 52.390569);

const TOKYO: GeoPoint = GeoPoint::new_unchecked(139.691706, 35.689487);

const LOS_ANGELES: GeoPoint = GeoPoint::new_unchecked(-118.243683, 34.052235);

fn condition_for_geo_radius(key: &str, geo_radius: GeoRadius) -> FieldCondition {
    FieldCondition::new_geo_radius(JsonPath::new(key), geo_radius)
}

fn condition_for_geo_polygon(key: &str, geo_polygon: GeoPolygon) -> FieldCondition {
    FieldCondition::new_geo_polygon(JsonPath::new(key), geo_polygon)
}

fn condition_for_geo_box(key: &str, geo_bounding_box: GeoBoundingBox) -> FieldCondition {
    FieldCondition::new_geo_bounding_box(JsonPath::new(key), geo_bounding_box)
}

#[cfg(feature = "testing")]
fn create_builder(index_type: IndexType) -> (IndexBuilder, TempDir, Database) {
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();

    let db = ();

    let mut builder = match index_type {
        IndexType::MutableGridstore => IndexBuilder::MutableGridstore(
            GeoMapIndex::builder_gridstore(temp_dir.path().to_path_buf()),
        ),
        IndexType::Mmap => IndexBuilder::Mmap(GeoMapIndex::builder_mmap(
            temp_dir.path(),
            true,
            &empty_deleted(),
        )),
        IndexType::RamMmap => IndexBuilder::RamMmap(GeoMapIndex::builder_mmap(
            temp_dir.path(),
            false,
            &empty_deleted(),
        )),
    };
    match &mut builder {
        IndexBuilder::MutableGridstore(builder) => builder.init().unwrap(),
        IndexBuilder::Mmap(builder) => builder.init().unwrap(),
        IndexBuilder::RamMmap(builder) => builder.init().unwrap(),
    }
    (builder, temp_dir, db)
}

fn build_random_index(
    num_points: usize,
    num_geo_values: usize,
    index_type: IndexType,
) -> (GeoMapIndex, TempDir, Database) {
    let mut rnd = StdRng::seed_from_u64(42);
    let (mut builder, temp_dir, db) = create_builder(index_type);

    for idx in 0..num_points {
        let geo_points = random_geo_payload(&mut rnd, num_geo_values..=num_geo_values);
        let array_payload = serde_json::Value::Array(geo_points);
        builder
            .add_point(
                idx as PointOffsetType,
                &[&array_payload],
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }

    let index = builder.finalize().unwrap();
    assert_eq!(index.points_count(), num_points);
    assert_eq!(index.points_values_count(), num_points * num_geo_values);
    (index, temp_dir, db)
}

const EARTH_RADIUS_METERS: f64 = 6371.0 * 1000.;
const LON_RANGE: Range<f64> = -180.0..180.0;
const LAT_RANGE: Range<f64> = -90.0..90.0;
const COORD_EPS: f64 = 1e-12;

fn radius_to_polygon(circle: &GeoRadius) -> GeoPolygon {
    let angular_radius: f64 = circle.radius.0 / EARTH_RADIUS_METERS;

    let angular_lat = circle.center.lat.to_radians();
    let mut min_lat = (angular_lat - angular_radius).to_degrees();
    let mut max_lat = (angular_lat + angular_radius).to_degrees();

    let (min_lon, max_lon) = if LAT_RANGE.start < min_lat && max_lat < LAT_RANGE.end {
        let angular_lon = circle.center.lon.to_radians();
        let delta_lon = (angular_radius.sin() / angular_lat.cos()).asin();

        let min_lon = (angular_lon - delta_lon).to_degrees();
        let max_lon = (angular_lon + delta_lon).to_degrees();

        (min_lon, max_lon)
    } else {
        if LAT_RANGE.start > min_lat {
            min_lat = LAT_RANGE.start + COORD_EPS;
        }
        if max_lat > LAT_RANGE.end {
            max_lat = LAT_RANGE.end - COORD_EPS;
        }

        (LON_RANGE.start + COORD_EPS, LON_RANGE.end - COORD_EPS)
    };

    build_polygon(vec![
        (min_lon, min_lat),
        (min_lon, max_lat),
        (max_lon, max_lat),
        (max_lon, min_lat),
        (min_lon, min_lat),
    ])
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_polygon_with_exclusion(#[case] index_type: IndexType) {
    fn check_cardinality_match(
        hashes: Vec<GeoHash>,
        field_condition: FieldCondition,
        index_type: IndexType,
    ) {
        let (field_index, _, _) = build_random_index(500, 20, index_type);
        let exact_points_for_hashes = field_index.iterator(hashes).unwrap().collect_vec();
        let real_cardinality = exact_points_for_hashes.len();

        let hw_counter = HardwareCounterCell::new();
        let card = field_index
            .estimate_cardinality(&field_condition, &hw_counter)
            .unwrap();
        let card = card.unwrap();

        eprintln!("real_cardinality = {real_cardinality:#?}");
        eprintln!("card = {card:#?}");

        assert!(card.min <= real_cardinality);
        assert!(card.max >= real_cardinality);

        assert!(card.exp >= card.min);
        assert!(card.exp <= card.max);
    }

    let europe = GeoLineString {
        points: vec![
            GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
            GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
            GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
            GeoPoint::new_unchecked(-8.396395372995187, 46.85848915174239),
            GeoPoint::new_unchecked(-10.508661204875182, 35.64130367692255),
            GeoPoint::new_unchecked(0.9590825812569506, 36.55931431668104),
            GeoPoint::new_unchecked(17.925941188829, 34.89268498908065),
            GeoPoint::new_unchecked(26.378822944221042, 38.87157101630817),
            GeoPoint::new_unchecked(41.568021588510476, 47.7100126473878),
            GeoPoint::new_unchecked(29.149194109528253, 70.96161947624168),
            GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
        ],
    };

    let berlin = GeoLineString {
        points: vec![
            GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
            GeoPoint::new_unchecked(13.11841750240768, 52.550216162683455),
            GeoPoint::new_unchecked(13.11841750240768, 52.40371784468752),
            GeoPoint::new_unchecked(13.391870497137859, 52.40546474165669),
            GeoPoint::new_unchecked(13.653869963292806, 52.35739986654923),
            GeoPoint::new_unchecked(13.754088338324664, 52.44213360096185),
            GeoPoint::new_unchecked(13.60805584899208, 52.47702797300224),
            GeoPoint::new_unchecked(13.63382628828623, 52.53367235825061),
            GeoPoint::new_unchecked(13.48493041681067, 52.60241883100514),
            GeoPoint::new_unchecked(13.52788114896677, 52.6571647548233),
            GeoPoint::new_unchecked(13.257291536380365, 52.667584785254064),
            GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
        ],
    };

    let europe_no_berlin = GeoPolygon {
        exterior: europe,
        interiors: Some(vec![berlin]),
    };
    check_cardinality_match(
        polygon_hashes(&europe_no_berlin, GEO_QUERY_MAX_REGION).unwrap(),
        condition_for_geo_polygon("test", europe_no_berlin.clone()),
        index_type,
    );
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn match_cardinality(#[case] index_type: IndexType) {
    fn check_cardinality_match(
        hashes: Vec<GeoHash>,
        field_condition: FieldCondition,
        index_type: IndexType,
    ) {
        let (field_index, _, _) = build_random_index(500, 20, index_type);
        let exact_points_for_hashes = field_index.iterator(hashes).unwrap().collect_vec();
        let real_cardinality = exact_points_for_hashes.len();

        let hw_counter = HardwareCounterCell::new();

        let card = field_index
            .estimate_cardinality(&field_condition, &hw_counter)
            .unwrap();
        let card = card.unwrap();

        eprintln!("real_cardinality = {real_cardinality:#?}");
        eprintln!("card = {card:#?}");

        assert!(card.min <= real_cardinality);
        assert!(card.max >= real_cardinality);

        assert!(card.exp >= card.min);
        assert!(card.exp <= card.max);
    }

    let r_meters = 500_000.0;
    let geo_radius = GeoRadius {
        center: NYC,
        radius: OrderedFloat(r_meters),
    };
    let nyc_hashes = circle_hashes(&geo_radius, GEO_QUERY_MAX_REGION).unwrap();
    check_cardinality_match(
        nyc_hashes,
        condition_for_geo_radius("test", geo_radius),
        index_type,
    );

    let geo_polygon = radius_to_polygon(&geo_radius);
    let polygon_hashes = polygon_hashes(&geo_polygon, GEO_QUERY_MAX_REGION).unwrap();
    check_cardinality_match(
        polygon_hashes,
        condition_for_geo_polygon("test", geo_polygon),
        index_type,
    );
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn geo_indexed_filtering(#[case] index_type: IndexType) {
    fn check_geo_indexed_filtering<F>(
        field_condition: FieldCondition,
        check_fn: F,
        index_type: IndexType,
    ) where
        F: Fn(&GeoPoint) -> bool + Clone,
    {
        let (field_index, _, _) = build_random_index(1000, 5, index_type);

        let hw_counter = HardwareCounterCell::new();
        let mut matched_points = (0..field_index.count_indexed_points() as PointOffsetType)
            .filter_map(|idx| {
                if field_index.check_values_any(idx, &hw_counter, check_fn.clone()) {
                    Some(idx as PointOffsetType)
                } else {
                    None
                }
            })
            .collect_vec();

        assert!(!matched_points.is_empty());

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let mut indexed_matched_points = field_index
            .filter(&field_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();

        matched_points.sort_unstable();
        indexed_matched_points.sort_unstable();

        assert_eq!(matched_points, indexed_matched_points);
    }

    let r_meters = 500_000.0;
    let geo_radius = GeoRadius {
        center: NYC,
        radius: OrderedFloat(r_meters),
    };
    check_geo_indexed_filtering(
        condition_for_geo_radius("test", geo_radius),
        |geo_point| geo_radius.check_point(geo_point),
        index_type,
    );

    let geo_polygon: GeoPolygon = build_polygon(vec![
        (-60.0, 37.0),
        (-60.0, 45.0),
        (-50.0, 45.0),
        (-50.0, 37.0),
        (-60.0, 37.0),
    ]);
    check_geo_indexed_filtering(
        condition_for_geo_polygon("test", geo_polygon.clone()),
        |geo_point| geo_polygon.convert().check_point(geo_point),
        index_type,
    );
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_payload_blocks(#[case] index_type: IndexType) {
    let (field_index, _, _) = build_random_index(1000, 5, index_type);
    let hw_counter = HardwareCounterCell::new();
    let top_level_points = field_index
        .points_of_hash(Default::default(), &hw_counter)
        .unwrap();
    assert_eq!(top_level_points, 1_000);
    let block_hashes = field_index.large_hashes(100).unwrap().collect_vec();
    assert!(!block_hashes.is_empty());
    for (geohash, size) in block_hashes {
        assert_eq!(geohash.len(), 1);
        assert!(size > 100);
        assert!(size < 1000);
    }

    let mut blocks = Vec::new();
    field_index
        .for_each_payload_block(100, JsonPath::new("test"), &mut |block| {
            blocks.push(block);
            Ok(())
        })
        .unwrap();
    blocks.iter().for_each(|block| {
        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let block_points = field_index
            .filter(&block.condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        assert_eq!(block_points.len(), block.cardinality);
    });
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn match_cardinality_point_with_multi_far_geo_payload(#[case] index_type: IndexType) {
    let (mut builder, _, _) = create_builder(index_type);

    let r_meters = 100.0;
    let geo_values = json!([
        {
            "lon": BERLIN.lon,
            "lat": BERLIN.lat
        },
        {
            "lon": NYC.lon,
            "lat": NYC.lat
        }
    ]);
    let hw_counter = HardwareCounterCell::new();
    builder.add_point(1, &[&geo_values], &hw_counter).unwrap();
    let index = builder.finalize().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let nyc_geo_radius = GeoRadius {
        center: NYC,
        radius: OrderedFloat(r_meters),
    };
    let field_condition = condition_for_geo_radius("test", nyc_geo_radius);
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 1);
    assert_eq!(card.max, 1);
    assert_eq!(card.exp, 1);

    let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&nyc_geo_radius));
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 1);
    assert_eq!(card.max, 1);
    assert_eq!(card.exp, 1);

    let berlin_geo_radius = GeoRadius {
        center: BERLIN,
        radius: OrderedFloat(r_meters),
    };
    let field_condition = condition_for_geo_radius("test", berlin_geo_radius);
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 1);
    assert_eq!(card.max, 1);
    assert_eq!(card.exp, 1);

    let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 1);
    assert_eq!(card.max, 1);
    assert_eq!(card.exp, 1);

    let tokyo_geo_radius = GeoRadius {
        center: TOKYO,
        radius: OrderedFloat(r_meters),
    };
    let field_condition = condition_for_geo_radius("test", tokyo_geo_radius);
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 0);
    assert_eq!(card.max, 0);
    assert_eq!(card.exp, 0);

    let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&tokyo_geo_radius));
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 0);
    assert_eq!(card.max, 0);
    assert_eq!(card.exp, 0);
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn match_cardinality_point_with_multi_close_geo_payload(#[case] index_type: IndexType) {
    let (mut builder, _, _) = create_builder(index_type);
    let geo_values = json!([
        {
            "lon": BERLIN.lon,
            "lat": BERLIN.lat
        },
        {
            "lon": POTSDAM.lon,
            "lat": POTSDAM.lat
        }
    ]);
    let hw_counter = HardwareCounterCell::new();
    builder.add_point(1, &[&geo_values], &hw_counter).unwrap();
    let index = builder.finalize().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let berlin_geo_radius = GeoRadius {
        center: BERLIN,
        radius: OrderedFloat(50_000.0),
    };
    let field_condition = condition_for_geo_radius("test", berlin_geo_radius);
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 1);
    assert_eq!(card.max, 1);
    assert_eq!(card.exp, 1);

    let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
    let card = index
        .estimate_cardinality(&field_condition, &hw_counter)
        .unwrap();
    let card = card.unwrap();
    assert_eq!(card.min, 1);
    assert_eq!(card.max, 1);
    assert_eq!(card.exp, 1);
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn load_from_disk(#[case] index_type: IndexType) {
    let temp_dir = {
        let (mut builder, temp_dir, _) = create_builder(index_type);

        let geo_values = json!([
            {
                "lon": BERLIN.lon,
                "lat": BERLIN.lat
            },
            {
                "lon": POTSDAM.lon,
                "lat": POTSDAM.lat
            }
        ]);
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(1, &[&geo_values], &hw_counter).unwrap();
        builder.finalize().unwrap();
        temp_dir
    };

    let new_index = match index_type {
        IndexType::MutableGridstore => {
            GeoMapIndex::new_gridstore(temp_dir.path().to_path_buf(), true)
                .unwrap()
                .unwrap()
        }
        IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path(), false, &empty_deleted())
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => GeoMapIndex::Immutable(
            ImmutableGeoMapIndex::open_mmap(
                StoredGeoMapIndex::open(temp_dir.path(), false, &empty_deleted())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        ),
    };

    let berlin_geo_radius = GeoRadius {
        center: BERLIN,
        radius: OrderedFloat(50_000.0),
    };

    let field_condition = condition_for_geo_radius("test", berlin_geo_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let point_offsets = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect_vec();
    assert_eq!(point_offsets, vec![1]);

    let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let point_offsets = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect_vec();
    assert_eq!(point_offsets, vec![1]);
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn same_geo_index_between_points_test(#[case] index_type: IndexType) {
    let temp_dir = {
        let (mut builder, temp_dir, _) = create_builder(index_type);

        let geo_values = json!([
            {
                "lon": BERLIN.lon,
                "lat": BERLIN.lat
            },
            {
                "lon": POTSDAM.lon,
                "lat": POTSDAM.lat
            }
        ]);
        let hw_counter = HardwareCounterCell::new();
        let payload = [&geo_values];
        builder.add_point(1, &payload, &hw_counter).unwrap();
        builder.add_point(2, &payload, &hw_counter).unwrap();
        let mut index = builder.finalize().unwrap();

        index.remove_point(1).unwrap();
        index.flusher()().unwrap();

        assert_eq!(index.points_count(), 1);
        if index_type != IndexType::Mmap {
            assert_eq!(index.points_values_count(), 2);
        }
        drop(index);
        temp_dir
    };

    let new_index = match index_type {
        IndexType::MutableGridstore => {
            GeoMapIndex::new_gridstore(temp_dir.path().to_path_buf(), true)
                .unwrap()
                .unwrap()
        }
        IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path(), false, &deleted_with(&[1]))
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => GeoMapIndex::Immutable(
            ImmutableGeoMapIndex::open_mmap(
                StoredGeoMapIndex::open(temp_dir.path(), false, &deleted_with(&[1]))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        ),
    };
    assert_eq!(new_index.points_count(), 1);
    if index_type != IndexType::Mmap {
        assert_eq!(new_index.points_values_count(), 2);
    }
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_empty_index_cardinality(#[case] index_type: IndexType) {
    let polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
                GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
            ],
        },
        interiors: None,
    };
    let polygon_with_interior = GeoPolygon {
        exterior: polygon.exterior.clone(),
        interiors: Some(vec![GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
                GeoPoint::new_unchecked(13.11841750240768, 52.550216162683455),
                GeoPoint::new_unchecked(13.11841750240768, 52.40371784468752),
                GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
            ],
        }]),
    };
    let hashes = polygon_hashes(&polygon, GEO_QUERY_MAX_REGION).unwrap();
    let hashes_with_interior =
        polygon_hashes(&polygon_with_interior, GEO_QUERY_MAX_REGION).unwrap();

    let hw_counter = HardwareCounterCell::new();

    let (field_index, _, _) = build_random_index(0, 0, index_type);
    assert!(
        field_index
            .match_cardinality(&hashes, &hw_counter)
            .unwrap()
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),
    );
    assert!(
        field_index
            .match_cardinality(&hashes_with_interior, &hw_counter)
            .unwrap()
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),
    );

    let (field_index, _, _) = build_random_index(0, 100, index_type);
    assert!(
        field_index
            .match_cardinality(&hashes, &hw_counter)
            .unwrap()
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),
    );
    assert!(
        field_index
            .match_cardinality(&hashes_with_interior, &hw_counter)
            .unwrap()
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),
    );

    let (field_index, _, _) = build_random_index(100, 100, index_type);
    assert!(
        !field_index
            .match_cardinality(&hashes, &hw_counter)
            .unwrap()
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),
    );
    assert!(
        !field_index
            .match_cardinality(&hashes_with_interior, &hw_counter)
            .unwrap()
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),
    );
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn query_across_antimeridian(#[case] index_type: IndexType) {
    let (mut builder, _, _) = create_builder(index_type);
    let geo_values = json!([
        {
            "lon": BERLIN.lon,
            "lat": BERLIN.lat
        }
    ]);
    let hw_counter = HardwareCounterCell::new();

    builder.add_point(1, &[&geo_values], &hw_counter).unwrap();

    let geo_values = json!([
        {
            "lon": LOS_ANGELES.lon,
            "lat": LOS_ANGELES.lat
        }
    ]);
    builder.add_point(2, &[&geo_values], &hw_counter).unwrap();

    let geo_values = json!([
        {
            "lon": TOKYO.lon,
            "lat": TOKYO.lat
        }
    ]);
    builder.add_point(3, &[&geo_values], &hw_counter).unwrap();

    let new_index = builder.finalize().unwrap();
    assert_eq!(new_index.points_count(), 3);
    assert_eq!(new_index.points_values_count(), 3);

    let bounding_box = GeoBoundingBox {
        top_left: GeoPoint::new_unchecked(167.0, 74.071028),
        bottom_right: GeoPoint::new_unchecked(-66.885417, 18.7763),
    };

    let field_condition = condition_for_geo_box("test", bounding_box);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let point_offsets = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect_vec();
    assert_eq!(point_offsets, vec![2]);
}

/// Removing a point with duplicate geo values in a multi-value geo field
/// must not produce spurious "no points for hash X was found" warnings.
#[rstest]
#[case(IndexType::MutableGridstore)]
fn test_remove_point_with_duplicate_geo_values(#[case] index_type: IndexType) {
    let (mut builder, _temp_dir, _db) = create_builder(index_type);
    let hw_counter = HardwareCounterCell::new();

    let duplicate_geo = json!([
        {"lon": BERLIN.lon, "lat": BERLIN.lat},
        {"lon": BERLIN.lon, "lat": BERLIN.lat}
    ]);
    builder
        .add_point(0, &[&duplicate_geo], &hw_counter)
        .unwrap();

    let single_geo = json!({"lon": NYC.lon, "lat": NYC.lat});
    builder.add_point(1, &[&single_geo], &hw_counter).unwrap();

    let mut index = builder.finalize().unwrap();

    assert_eq!(index.points_count(), 2);
    assert_eq!(index.points_values_count(), 3);

    index.remove_point(0).unwrap();

    assert_eq!(index.points_count(), 1);
    assert_eq!(index.points_values_count(), 1);

    let geo_radius = GeoRadius {
        center: NYC,
        radius: OrderedFloat(100.0),
    };
    let field_condition = condition_for_geo_radius("test", geo_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let results = index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect_vec();
    assert_eq!(results, vec![1]);

    index.remove_point(1).unwrap();

    assert_eq!(index.points_count(), 0);
    assert_eq!(index.points_values_count(), 0);
}

/// Simulate the user's scenario: frequently adding and removing points
/// with geo payloads.
#[rstest]
#[case(IndexType::MutableGridstore)]
fn test_frequent_add_remove_geo_points(#[case] index_type: IndexType) {
    let (mut builder, _temp_dir, _db) = create_builder(index_type);
    let hw_counter = HardwareCounterCell::new();

    let berlin_geo = json!({"lon": BERLIN.lon, "lat": BERLIN.lat});
    builder.add_point(0, &[&berlin_geo], &hw_counter).unwrap();

    let mut index = builder.finalize().unwrap();
    assert_eq!(index.points_count(), 1);

    for i in 1u32..20 {
        let geo = json!({"lon": NYC.lon, "lat": NYC.lat});
        index.add_point(i, &[&geo], &hw_counter).unwrap();
        assert_eq!(index.points_count(), 2);

        index.remove_point(i).unwrap();
        assert_eq!(index.points_count(), 1);
    }

    let tokyo_geo = json!({"lon": TOKYO.lon, "lat": TOKYO.lat});
    index.add_point(0, &[&tokyo_geo], &hw_counter).unwrap();
    assert_eq!(index.points_count(), 1);
    assert_eq!(index.points_values_count(), 1);

    let geo_radius = GeoRadius {
        center: TOKYO,
        radius: OrderedFloat(100.0),
    };
    let field_condition = condition_for_geo_radius("test", geo_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let results = index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect_vec();
    assert_eq!(results, vec![0]);

    index.remove_point(0).unwrap();
    assert_eq!(index.points_count(), 0);
    assert_eq!(index.points_values_count(), 0);
}

#[rstest]
#[case(&[IndexType::MutableGridstore, IndexType::Mmap, IndexType::RamMmap], false)]
#[case(&[IndexType::MutableGridstore, IndexType::RamMmap], true)]
fn test_congruence(#[case] types: &[IndexType], #[case] deleted: bool) {
    const POINT_COUNT: usize = 500;

    let (mut indices, _data): (Vec<_>, Vec<_>) = types
        .iter()
        .copied()
        .map(|index_type| {
            let (index, temp_dir, db) = build_random_index(POINT_COUNT, 20, index_type);
            (index, (temp_dir, db))
        })
        .unzip();

    let polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
                GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
            ],
        },
        interiors: None,
    };
    let hashes = polygon_hashes(&polygon, GEO_QUERY_MAX_REGION).unwrap();

    if deleted {
        for index in indices.iter_mut() {
            index.remove_point(10).unwrap();
            index.remove_point(11).unwrap();
            index.remove_point(12).unwrap();
            index.remove_point(100).unwrap();
            index.remove_point(150).unwrap();
        }
    }

    for index in &indices[1..] {
        assert_eq!(indices[0].points_count(), index.points_count());
        if !deleted {
            assert_eq!(
                indices[0].points_values_count(),
                index.points_values_count(),
            );
        }
        assert_eq!(
            indices[0].max_values_per_point(),
            index.max_values_per_point(),
        );
        let hw_counter = HardwareCounterCell::disposable();
        for &hash in &hashes {
            assert_eq!(
                indices[0].points_of_hash(hash, &hw_counter).unwrap(),
                index.points_of_hash(hash, &hw_counter).unwrap(),
            );
            assert_eq!(
                indices[0].values_of_hash(hash, &hw_counter),
                index.values_of_hash(hash, &hw_counter),
            );
        }
        assert_eq!(
            indices[0]
                .large_hashes(20)
                .unwrap()
                .map(|(hash, _)| hash)
                .collect::<BTreeSet<_>>(),
            index
                .large_hashes(20)
                .unwrap()
                .map(|(hash, _)| hash)
                .collect::<BTreeSet<_>>(),
        );
        assert_eq!(
            indices[0]
                .iterator(hashes.clone())
                .unwrap()
                .collect::<HashSet<_>>(),
            index
                .iterator(hashes.clone())
                .unwrap()
                .collect::<HashSet<_>>(),
        );
        for point_id in 0..POINT_COUNT {
            assert_eq!(
                indices[0].values_count(point_id as PointOffsetType),
                index.values_count(point_id as PointOffsetType),
            );
            if !deleted {
                assert_eq!(
                    indices[0]
                        .get_values(point_id as PointOffsetType)
                        .unwrap()
                        .collect::<BTreeSet<_>>(),
                    index
                        .get_values(point_id as PointOffsetType)
                        .unwrap()
                        .collect::<BTreeSet<_>>(),
                );
            }
            assert_eq!(
                indices[0].values_is_empty(point_id as PointOffsetType),
                index.values_is_empty(point_id as PointOffsetType),
            );
        }
    }
}

/// Cross-check that [`StoredGeoMapIndex::all_points`] produces the same
/// point set as the mutable and immutable index implementations.
#[test]
fn test_all_points_congruence() {
    const POINT_COUNT: usize = 500;

    let (mut mutable_index, _mutable_tmp, _) =
        build_random_index(POINT_COUNT, 20, IndexType::MutableGridstore);
    let (mut immutable_index, _immutable_tmp, _) =
        build_random_index(POINT_COUNT, 20, IndexType::RamMmap);
    let (mut mmap_index, _mmap_tmp, _) = build_random_index(POINT_COUNT, 20, IndexType::Mmap);

    let GeoMapIndex::Storage(storage_index) = &mmap_index else {
        panic!("expected Mmap variant to build into Storage");
    };

    let europe_polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
                GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
            ],
        },
        interiors: None,
    };
    let europe_hashes = polygon_hashes(&europe_polygon, GEO_QUERY_MAX_REGION).unwrap();

    let nyc_circle = GeoRadius {
        center: NYC,
        radius: OrderedFloat(300_000.0),
    };
    let tokyo_circle = GeoRadius {
        center: TOKYO,
        radius: OrderedFloat(300_000.0),
    };
    let mut disjoint_hashes = circle_hashes(&nyc_circle, GEO_QUERY_MAX_REGION).unwrap();
    disjoint_hashes.extend(circle_hashes(&tokyo_circle, GEO_QUERY_MAX_REGION).unwrap());

    let mut overlapping_hashes = europe_hashes.clone();
    overlapping_hashes.extend(europe_hashes.iter().copied());
    if let Some(first) = europe_hashes.first() {
        let len = first.len();
        if len > 2 {
            overlapping_hashes.push(first.truncate(len - 1));
            overlapping_hashes.push(first.truncate(len - 2));
        }
    }

    let global_polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(-170.0, -80.0),
                GeoPoint::new_unchecked(170.0, -80.0),
                GeoPoint::new_unchecked(170.0, 80.0),
                GeoPoint::new_unchecked(-170.0, 80.0),
                GeoPoint::new_unchecked(-170.0, -80.0),
            ],
        },
        interiors: None,
    };
    let global_hashes = polygon_hashes(&global_polygon, GEO_QUERY_MAX_REGION).unwrap();

    let cases: Vec<(&str, Vec<GeoHash>)> = vec![
        ("europe_single_region", europe_hashes),
        ("nyc_plus_tokyo_disjoint", disjoint_hashes),
        ("overlapping_and_duplicated", overlapping_hashes),
        ("global_large_region", global_hashes),
    ];

    let assert_all_points_match = |mutable_index: &GeoMapIndex,
                                   immutable_index: &GeoMapIndex,
                                   storage_index: &StoredGeoMapIndex<MmapFile>,
                                   cases: &[(&str, Vec<GeoHash>)],
                                   phase: &str| {
        for (name, hashes) in cases {
            let hashes = hashes.clone();
            let mutable_result: HashSet<PointOffsetType> =
                mutable_index.iterator(hashes.clone()).unwrap().collect();

            let immutable_result: HashSet<PointOffsetType> =
                immutable_index.iterator(hashes.clone()).unwrap().collect();

            let all_points_result: HashSet<PointOffsetType> = storage_index
                .all_points(hashes)
                .unwrap()
                .into_iter()
                .collect();

            assert_eq!(
                mutable_result, immutable_result,
                "{phase}: case `{name}`: mutable vs immutable differ",
            );
            assert_eq!(
                mutable_result, all_points_result,
                "{phase}: case `{name}`: mutable vs all_points differ",
            );
        }
    };

    assert_all_points_match(
        &mutable_index,
        &immutable_index,
        storage_index,
        &cases,
        "baseline",
    );

    const DELETED_POINT_IDS: &[PointOffsetType] = &[10, 11, 12, 100, 150];
    for &id in DELETED_POINT_IDS {
        mutable_index.remove_point(id).unwrap();
        immutable_index.remove_point(id).unwrap();
        mmap_index.remove_point(id).unwrap();
    }

    let GeoMapIndex::Storage(storage_index) = &mmap_index else {
        panic!("expected Mmap variant to build into Storage");
    };

    assert_all_points_match(
        &mutable_index,
        &immutable_index,
        storage_index,
        &cases,
        "after point deletions",
    );
}

/// Reload contract: runtime deletions are not persisted by the mmap geo
/// index. Callers must re-supply the deletion bitslice on reload.
#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_geo_index_reload(#[case] index_type: IndexType) {
    let temp_dir = {
        let (mut builder, temp_dir, _) = create_builder(index_type);

        let hw_counter = HardwareCounterCell::new();

        let berlin = json!({ "lon": BERLIN.lon, "lat": BERLIN.lat });
        let potsdam = json!({ "lon": POTSDAM.lon, "lat": POTSDAM.lat });
        let tokyo = json!({ "lon": TOKYO.lon, "lat": TOKYO.lat });
        let nyc = json!({ "lon": NYC.lon, "lat": NYC.lat });

        builder.add_point(1, &[&berlin], &hw_counter).unwrap();
        builder.add_point(2, &[&berlin], &hw_counter).unwrap();
        builder.add_point(3, &[&potsdam], &hw_counter).unwrap();
        builder.add_point(4, &[&potsdam], &hw_counter).unwrap();
        builder.add_point(5, &[&tokyo], &hw_counter).unwrap();
        builder.add_point(6, &[&tokyo], &hw_counter).unwrap();
        builder.add_point(7, &[&nyc], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();

        index.remove_point(2).unwrap();
        index.remove_point(3).unwrap();
        index.remove_point(6).unwrap();
        index.flusher()().unwrap();
        assert_eq!(index.points_count(), 4);
        drop(index);
        temp_dir
    };

    let deleted = deleted_with(&[2, 3, 6]);
    let new_index = match index_type {
        IndexType::MutableGridstore => {
            GeoMapIndex::new_gridstore(temp_dir.path().to_path_buf(), true)
                .unwrap()
                .unwrap()
        }
        IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path(), false, &deleted)
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => GeoMapIndex::Immutable(
            ImmutableGeoMapIndex::open_mmap(
                StoredGeoMapIndex::open(temp_dir.path(), false, &deleted)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        ),
    };

    assert_eq!(new_index.points_count(), 4);

    let berlin_radius = GeoRadius {
        center: BERLIN,
        radius: OrderedFloat(50_000.0),
    };
    let field_condition = condition_for_geo_radius("test", berlin_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let mut hits: Vec<PointOffsetType> = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect();
    hits.sort();
    assert_eq!(hits, vec![1, 4]);

    let tokyo_radius = GeoRadius {
        center: TOKYO,
        radius: OrderedFloat(50_000.0),
    };
    let field_condition = condition_for_geo_radius("test", tokyo_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let mut hits: Vec<PointOffsetType> = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect();
    hits.sort();
    assert_eq!(hits, vec![5]);

    let nyc_radius = GeoRadius {
        center: NYC,
        radius: OrderedFloat(50_000.0),
    };
    let field_condition = condition_for_geo_radius("test", nyc_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let mut hits: Vec<PointOffsetType> = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect();
    hits.sort();
    assert_eq!(hits, vec![7]);
}

/// Regression test: when reloading an mmap geo index with a `deleted_points`
/// bitslice shorter than `point_to_values.len()`, missing entries must
/// default to live, not deleted.
#[rstest]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_geo_index_reload_short_deleted_bitslice(#[case] index_type: IndexType) {
    let temp_dir = {
        let (mut builder, temp_dir, _) = create_builder(index_type);

        let hw_counter = HardwareCounterCell::new();

        let berlin = json!({ "lon": BERLIN.lon, "lat": BERLIN.lat });

        builder.add_point(1, &[&berlin], &hw_counter).unwrap();
        builder.add_point(2, &[&berlin], &hw_counter).unwrap();
        builder.add_point(3, &[], &hw_counter).unwrap();
        builder.add_point(4, &[&berlin], &hw_counter).unwrap();

        builder.finalize().unwrap();
        temp_dir
    };

    let mut short_deleted = BitVec::repeat(false, 2);
    short_deleted.set(1, true);
    let new_index = match index_type {
        IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path(), false, &short_deleted)
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => GeoMapIndex::Immutable(
            ImmutableGeoMapIndex::open_mmap(
                StoredGeoMapIndex::open(temp_dir.path(), false, &short_deleted)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        ),
        IndexType::MutableGridstore => unreachable!(),
    };

    let berlin_radius = GeoRadius {
        center: BERLIN,
        radius: OrderedFloat(50_000.0),
    };
    let field_condition = condition_for_geo_radius("test", berlin_radius);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let mut hits: Vec<PointOffsetType> = new_index
        .filter(&field_condition, &hw_counter)
        .unwrap()
        .unwrap()
        .collect();
    hits.sort();
    assert_eq!(hits, vec![2, 4]);
}
