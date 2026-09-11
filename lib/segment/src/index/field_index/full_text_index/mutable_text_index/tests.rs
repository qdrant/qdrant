// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rstest::rstest;
use tempfile::Builder;

use super::super::FullTextIndex;
use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::index::field_index::{PayloadFieldIndex, ValueIndexer};
use crate::json_path::JsonPath;
use crate::types::{FieldCondition, Match};

fn filter_request(text: &str) -> FieldCondition {
    FieldCondition::new_match(JsonPath::new("text"), Match::new_text(text))
}

#[test]
fn test_full_text_indexing() {
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::counter::hardware_counter::HardwareCounterCell;

    use crate::index::field_index::{PayloadFieldIndex, PayloadFieldIndexRead, ValueIndexer};

    let payloads: Vec<_> = vec![
        serde_json::json!(
            "The celebration had a long way to go and even in the silent depths of Multivac's underground chambers, it hung in the air."
        ),
        serde_json::json!("If nothing else, there was the mere fact of isolation and silence."),
        serde_json::json!([
            "For the first time in a decade, technicians were not scurrying about the vitals of the giant computer, ",
            "the soft lights did not wink out their erratic patterns, the flow of information in and out had halted."
        ]),
        serde_json::json!(
            "It would not be halted long, of course, for the needs of peace would be pressing."
        ),
        serde_json::json!(
            "Yet now, for a day, perhaps for a week, even Multivac might celebrate the great time, and rest."
        ),
    ];

    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config = TextIndexParams {
        memory: None,
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Word,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
        phrase_matching: None,
        on_disk: None,
        stopwords: None,
        stemmer: None,
        ascii_folding: None,
        enable_hnsw: None,
    };

    {
        let mut index =
            FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config.clone(), true)
                .unwrap()
                .unwrap();

        let hw_cell = HardwareCounterCell::new();

        for (idx, payload) in payloads.iter().enumerate() {
            index
                .add_point(idx as PointOffsetType, &[payload], &hw_cell)
                .unwrap();
        }

        assert_eq!(index.count_indexed_points().unwrap(), payloads.len());

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();

        let filter_condition = filter_request("multivac");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert_eq!(search_res, vec![0, 4]);

        let filter_condition = filter_request("giant computer");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert_eq!(search_res, vec![2]);

        let filter_condition = filter_request("the great time");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert_eq!(search_res, vec![4]);

        index.remove_point(2).unwrap();
        index.remove_point(3).unwrap();

        let filter_condition = filter_request("giant computer");
        assert!(
            index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .unwrap()
                .next()
                .is_none()
        );

        assert_eq!(index.count_indexed_points().unwrap(), payloads.len() - 2);

        let payload = serde_json::json!([
            "The last question was asked for the first time, half in jest, on May 21, 2061,",
            "at a time when humanity first stepped into the light."
        ]);
        index.add_point(3, &[&payload], &hw_cell).unwrap();

        let payload = serde_json::json!([
            "The question came about as a result of a five dollar bet over highballs, and it happened this way: "
        ]);
        index.add_point(4, &[&payload], &hw_cell).unwrap();

        assert_eq!(index.count_indexed_points().unwrap(), payloads.len() - 1);

        index.flusher()().unwrap();
    }

    {
        let mut index = FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config, true)
            .unwrap()
            .unwrap();

        assert_eq!(index.count_indexed_points().unwrap(), 4);

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();

        let filter_condition = filter_request("multivac");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert_eq!(search_res, vec![0]);

        let filter_condition = filter_request("the");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert_eq!(search_res, vec![0, 1, 3, 4]);

        // check deletion
        index.remove_point(0).unwrap();
        let filter_condition = filter_request("multivac");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert!(search_res.is_empty());
        assert_eq!(index.count_indexed_points().unwrap(), 3);

        index.remove_point(3).unwrap();
        let filter_condition = filter_request("the");
        let search_res: Vec<_> = index
            .filter(&filter_condition, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        assert_eq!(search_res, vec![1, 4]);
        assert_eq!(index.count_indexed_points().unwrap(), 2);

        // check deletion of non-existing point
        index.remove_point(3).unwrap();
        assert_eq!(index.count_indexed_points().unwrap(), 2);
    }
}

/// Reach the in-memory lengths of a gridstore-backed index.
fn doc_lens(index: &FullTextIndex) -> (Vec<u32>, u64) {
    let FullTextIndex::Mutable(index) = index else {
        panic!("expected a mutable (gridstore) index");
    };
    let inverted = &index.inner.inverted_index;
    (inverted.point_to_doc_len.clone(), inverted.total_tokens)
}

fn length_config(phrase_matching: bool) -> TextIndexParams {
    TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Whitespace,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
        phrase_matching: Some(phrase_matching),
        on_disk: None,
        memory: None,
        stopwords: None,
        stemmer: None,
        ascii_folding: None,
        enable_hnsw: None,
    }
}

/// Document length has to survive a reload, and the `phrase_matching: false`
/// case is the one at risk: those tokens are sorted and deduplicated on the way
/// to the gridstore, so a length derived from the stored tokens after reopening
/// would count distinct terms rather than all of them.
#[rstest]
fn doc_len_survives_gridstore_reload(#[values(false, true)] phrase_matching: bool) {
    let temp_dir = Builder::new().prefix("doc_len_reload").tempdir().unwrap();
    let path = temp_dir.path().join("index");
    let hw_counter = HardwareCounterCell::new();

    // Point 1 repeats "the" three times: 7 tokens, 5 distinct.
    let payloads = [
        serde_json::json!("alpha beta gamma"),
        serde_json::json!("the cat sat on the mat the"),
    ];
    let expected = vec![3u32, 7];

    {
        let mut index =
            FullTextIndex::new_gridstore(path.clone(), length_config(phrase_matching), true)
                .unwrap()
                .unwrap();
        for (idx, payload) in payloads.iter().enumerate() {
            index
                .add_point(idx as PointOffsetType, &[payload], &hw_counter)
                .unwrap();
        }
        let (lens, total) = doc_lens(&index);
        assert_eq!(lens, expected, "lengths wrong before reload");
        assert_eq!(total, 10);
        index.flusher()().unwrap();
    }

    let reopened = FullTextIndex::new_gridstore(path, length_config(phrase_matching), false)
        .unwrap()
        .unwrap();
    let (lens, total) = doc_lens(&reopened);
    assert_eq!(lens, expected, "lengths lost across reload");
    assert_eq!(total, 10);
}

/// Array boundary sentinels occupy a position so phrases cannot match across
/// two elements, but they are not content and must not count toward length.
#[test]
fn doc_len_excludes_array_boundary_sentinels() {
    let temp_dir = Builder::new().prefix("doc_len_sentinel").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    // Two elements, three tokens each. With phrase matching a sentinel is
    // inserted between them; the length must still be six.
    let payload = serde_json::json!(["alpha beta gamma", "delta epsilon zeta"]);

    for phrase_matching in [false, true] {
        let path = temp_dir.path().join(format!("index_{phrase_matching}"));
        let mut index = FullTextIndex::new_gridstore(path, length_config(phrase_matching), true)
            .unwrap()
            .unwrap();
        index.add_point(0, &[&payload], &hw_counter).unwrap();

        let (lens, total) = doc_lens(&index);
        assert_eq!(lens, vec![6], "phrase_matching = {phrase_matching}");
        assert_eq!(total, 6, "phrase_matching = {phrase_matching}");
    }
}

/// Removing a point takes its length back out of the running total, so `avgdl`
/// is not inflated by documents that no longer exist.
#[test]
fn removing_a_point_discounts_its_length() {
    let temp_dir = Builder::new().prefix("doc_len_remove").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut index =
        FullTextIndex::new_gridstore(temp_dir.path().join("index"), length_config(false), true)
            .unwrap()
            .unwrap();
    index
        .add_point(0, &[&serde_json::json!("alpha beta gamma")], &hw_counter)
        .unwrap();
    index
        .add_point(1, &[&serde_json::json!("delta epsilon")], &hw_counter)
        .unwrap();
    assert_eq!(doc_lens(&index), (vec![3, 2], 5));

    index.remove_point(1).unwrap();
    assert_eq!(doc_lens(&index), (vec![3, 0], 3));

    // Removing twice must not double-discount.
    index.remove_point(1).unwrap();
    assert_eq!(doc_lens(&index), (vec![3, 0], 3));
}

/// Overwriting a point replaces its length instead of adding to it.
#[test]
fn overwriting_a_point_replaces_its_length() {
    let temp_dir = Builder::new()
        .prefix("doc_len_overwrite")
        .tempdir()
        .unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut index =
        FullTextIndex::new_gridstore(temp_dir.path().join("index"), length_config(false), true)
            .unwrap()
            .unwrap();
    index
        .add_point(0, &[&serde_json::json!("alpha beta gamma")], &hw_counter)
        .unwrap();
    assert_eq!(doc_lens(&index), (vec![3], 3));

    index
        .add_point(0, &[&serde_json::json!("delta epsilon")], &hw_counter)
        .unwrap();
    assert_eq!(doc_lens(&index), (vec![2], 2));
}
