#![no_main]
use std::hint::black_box;

use arbitrary::Arbitrary;
use common::types::PointOffsetType;
use libfuzzer_sys::fuzz_target;
use segment::common::rocksdb_wrapper::open_db_with_existing_cf;
use segment::common::utils::MultiValue;
use segment::data_types::text_index::TextIndexParams;
use segment::index::field_index::full_text_index::text_index::FullTextIndex;
use segment::index::field_index::{PayloadFieldIndex, ValueIndexer};
use segment::types::{FieldCondition, Match, MatchText};
use tempfile::Builder;

fn filter_request(text: &str) -> FieldCondition {
    FieldCondition {
        key: "text".to_owned(),
        r#match: Some(Match::Text(MatchText {
            text: text.to_owned(),
        })),
        range: None,
        geo_bounding_box: None,
        geo_radius: None,
        values_count: None,
        geo_polygon: None,
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum FullTextIndexOperation {
    Filter(String),
    Add(usize, String),
    Remove(usize),
}

#[derive(Arbitrary, Debug, Clone)]
struct Ctx {
    text_index_ops: Vec<FullTextIndexOperation>,
    config: TextIndexParams,
}

fuzz_target!(|data: Ctx| {
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    // Limits idx to a max of 500, this way there will be a higher chance that
    // an added point will be removed by the next operation.
    let minimize_point_offset = |x| (x % 500) as PointOffsetType;

    let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();

    let mut index = FullTextIndex::new(db, data.config.clone(), "text");

    index.recreate().unwrap();

    for operation in data.text_index_ops.iter() {
        match operation {
            FullTextIndexOperation::Filter(text) => {
                let filter_condition = filter_request(text);
                let _: Vec<_> = black_box(
                    index
                        .filter(&filter_condition)
                        .unwrap_or(Box::new(Vec::new().into_iter()))
                        .collect(),
                );
            }
            FullTextIndexOperation::Add(idx, text) => {
                let payload = serde_json::json!([text]);
                let _ = black_box(index.add_point(
                    minimize_point_offset(*idx),
                    &MultiValue::Single(Some(&payload)),
                ));
            }
            FullTextIndexOperation::Remove(idx) => {
                let _ = black_box(index.remove_point(minimize_point_offset(*idx)));
            }
        }
    }

    index.flusher()().unwrap();
});
