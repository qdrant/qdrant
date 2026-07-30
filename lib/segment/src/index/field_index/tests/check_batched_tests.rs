use common::bitvec::BitVec;
use common::condition_checker::{ConditionChecker, assert_congruence};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFs;
use common::universal_io::MmapFs;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rand::prelude::*;
use serde::Serialize;
use serde_json::Value;

use crate::data_types::index::TextIndexParams;
use crate::index::field_index::index_selector::IndexSelector;
use crate::index::field_index::{
    FieldIndexBuilderTrait, PayloadFieldIndexRead, ReadOnlyFieldIndex,
};
use crate::json_path::JsonPath;
use crate::types::{
    AnyVariants, FieldCondition, GeoPoint, GeoRadius, Match, MatchPhrase, MatchTextAny, Memory,
    PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType, Range,
};

const KEY: &str = "field";

#[test]
fn geo() {
    let geo = |lon, lat| GeoPoint::new_unchecked(lon, lat);
    check_index(
        &PayloadFieldSchema::FieldType(PayloadSchemaType::Geo),
        &FieldCondition::new_geo_radius(
            JsonPath::new(KEY),
            GeoRadius {
                center: GeoPoint::new_unchecked(0.0, 0.0),
                radius: OrderedFloat(6_000_000.0),
            },
        ),
        &[
            vec![geo(0.0, 0.0)],
            vec![geo(-8.0, 7.5)],
            vec![geo(8.0, -8.0), geo(150.0, 0.0)],
        ],
        &[
            vec![],
            vec![geo(150.0, 0.0)],
            vec![geo(165.0, -8.0), geo(155.0, 8.0)],
        ],
    );
}

#[test]
fn numeric() {
    let range = Range {
        lt: Some(OrderedFloat(75.0)),
        gt: None,
        gte: Some(OrderedFloat(25.0)),
        lte: None,
    };
    check_index(
        &PayloadFieldSchema::FieldType(PayloadSchemaType::Float),
        &FieldCondition::new_range(JsonPath::new(KEY), range),
        &[vec![25.0], vec![74.9], vec![50.0, 100.0]],
        &[vec![], vec![24.9], vec![75.0, 100.0]],
    );
}

#[test]
fn map_keyword() {
    check_index(
        &PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
        &FieldCondition::new_match(JsonPath::new(KEY), "red".to_string().into()),
        &[vec!["red"], vec!["red", "blue"], vec!["b", "red"]],
        &[
            vec![],
            vec![""],
            vec!["b", "blue"],
            vec!["a-rather-long-keyword-to-vary-the-value-length"],
        ],
    );
}

#[test]
fn map_except() {
    check_index(
        &PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
        &FieldCondition::new_match(
            JsonPath::new(KEY),
            Match::new_except(AnyVariants::Strings(
                ["red".to_string(), "green".to_string()]
                    .into_iter()
                    .collect(),
            )),
        ),
        &[vec!["outsider"], vec!["red", "outsider"]],
        &[vec![], vec!["red"], vec!["green", "red"]],
    );
}

#[test]
fn text_match() {
    check_index(
        &PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams::default())),
        &FieldCondition::new_match(JsonPath::new(KEY), Match::new_text("alpha beta")),
        &[
            vec!["gamma alpha beta"],
            vec!["beta alpha"],
            vec!["alpha", "delta beta"],
        ],
        &[
            vec![],
            vec!["alpha gamma"],
            vec!["beta", "delta beta"],
            vec!["gamma", "delta"],
        ],
    );
}

#[test]
fn text_any() {
    check_index(
        &PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams::default())),
        &FieldCondition::new_match(
            JsonPath::new(KEY),
            Match::TextAny(MatchTextAny {
                text_any: "alpha beta".into(),
            }),
        ),
        &[
            vec!["alpha"],
            vec!["gamma beta"],
            vec!["delta", "beta alpha"],
        ],
        &[vec![], vec!["gamma"], vec!["delta gamma", "delta"]],
    );
}

#[test]
fn text_phrase() {
    check_index(
        &PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
            phrase_matching: Some(true),
            ..Default::default()
        })),
        &FieldCondition::new_match(
            JsonPath::new(KEY),
            Match::Phrase(MatchPhrase {
                phrase: "alpha beta".into(),
            }),
        ),
        &[
            vec!["alpha beta"],
            vec!["gamma alpha beta delta"],
            vec!["gamma", "alpha beta"],
        ],
        &[
            vec![],
            vec!["alpha gamma beta"],
            vec!["beta alpha"],
            vec!["gamma alpha", "beta delta"],
        ],
    );
}

fn check_index<T: Serialize>(
    schema: &PayloadFieldSchema,
    condition: &FieldCondition,
    yes: &[Vec<T>],
    no: &[Vec<T>],
) {
    let mut rng = StdRng::seed_from_u64(0);
    let field = JsonPath::new(KEY);

    let mut points: Vec<(Vec<Value>, bool, bool, bool)> = Vec::new(); // (row, baked, removed, expected)
    for (rows, matched) in [(yes, true), (no, false)] {
        for row in rows {
            let row: Vec<Value> = row
                .iter()
                .map(|v| serde_json::to_value(v).unwrap())
                .collect();
            points.push((row.clone(), false, false, matched));
            points.push((row.clone(), false, true, false));
            points.push((row.clone(), true, false, false));
            points.push((row.clone(), true, true, false));
        }
    }
    points.shuffle(&mut rng);

    for points in [&points[..0], &points[..]] {
        let n = points.len();
        let baked: BitVec = points.iter().map(|&(_, baked, _, _)| baked).collect();
        let deleted: BitVec = points
            .iter()
            .map(|&(_, baked, removed, _)| baked || removed)
            .collect();

        for memory in [Memory::Pinned, Memory::Cold] {
            let dir = tempfile::tempdir().unwrap();
            let dir = dir.path();

            // Build index
            let mut builders = IndexSelector::NonAppendable { dir, memory }
                .index_builder(&field, schema, &baked)
                .unwrap();
            assert_eq!(builders.len(), 1);
            let mut builder = builders.pop().unwrap();
            builder.init().unwrap();
            for (id, (row, _, _, _)) in (0u32..).zip(points) {
                builder
                    .add_point(id, &row.iter().collect_vec(), &HardwareCounterCell::new())
                    .unwrap();
            }
            let mut index = builder.finalize().unwrap();
            for (id, &(_, _, removed, _)) in (0u32..).zip(points) {
                if removed {
                    index.remove_point(id).unwrap();
                }
            }

            // Sanity check: `check()` is the same as `expected`
            let checker = index.condition_checker(condition, HwMeasurementAcc::new());
            let checker = checker.unwrap().unwrap();
            for (id, &(_, _, _, expected)) in (0u32..).zip(points) {
                assert_eq!(checker.check(id).unwrap(), expected, "{id}");
            }

            assert_congruence(&checker, n, &mut rng);

            let r#type = index.get_full_index_type();

            let index =
                ReadOnlyFieldIndex::open(&MmapFs, dir, &field, schema, &r#type, n, &deleted, None);
            let index = index.unwrap().unwrap();
            let checker = index.condition_checker(condition, HwMeasurementAcc::new());
            assert_congruence(&checker.unwrap().unwrap(), n as _, &mut rng);

            #[cfg(target_os = "linux")]
            {
                let index = ReadOnlyFieldIndex::open(
                    &IoUringFs, dir, &field, schema, &r#type, n, &deleted, None,
                );
                let index = index.unwrap().unwrap();
                let checker = index.condition_checker(condition, HwMeasurementAcc::new());
                assert_congruence(&checker.unwrap().unwrap(), n, &mut rng);
            }
        }
    }
}
