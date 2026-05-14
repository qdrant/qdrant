use gridstore::Gridstore;
use gridstore::config::StorageOptions;

use super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::tokenizers::Tokenizer;
use crate::data_types::index::TextIndexParams;

mod lifecycle;
mod read_ops;

pub(super) const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    compression: Some(gridstore::config::Compression::None),
    page_size_bytes: None,
    block_size_bytes: None,
    region_size_blocks: None,
};

pub struct MutableFullTextIndex {
    pub(super) inverted_index: MutableInvertedIndex,
    pub(super) config: TextIndexParams,
    pub(super) storage: Gridstore<Vec<u8>>,
    pub(super) tokenizer: Tokenizer,
}

#[cfg(test)]
mod tests {
    use common::types::PointOffsetType;
    use tempfile::Builder;

    use super::super::text_index::FullTextIndex;
    use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
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

            assert_eq!(index.count_indexed_points(), payloads.len());

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

            assert_eq!(index.count_indexed_points(), payloads.len() - 2);

            let payload = serde_json::json!([
                "The last question was asked for the first time, half in jest, on May 21, 2061,",
                "at a time when humanity first stepped into the light."
            ]);
            index.add_point(3, &[&payload], &hw_cell).unwrap();

            let payload = serde_json::json!([
                "The question came about as a result of a five dollar bet over highballs, and it happened this way: "
            ]);
            index.add_point(4, &[&payload], &hw_cell).unwrap();

            assert_eq!(index.count_indexed_points(), payloads.len() - 1);

            index.flusher()().unwrap();
        }

        {
            let mut index =
                FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config, true)
                    .unwrap()
                    .unwrap();

            assert_eq!(index.count_indexed_points(), 4);

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
            assert_eq!(index.count_indexed_points(), 3);

            index.remove_point(3).unwrap();
            let filter_condition = filter_request("the");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![1, 4]);
            assert_eq!(index.count_indexed_points(), 2);

            // check deletion of non-existing point
            index.remove_point(3).unwrap();
            assert_eq!(index.count_indexed_points(), 2);
        }
    }
}
