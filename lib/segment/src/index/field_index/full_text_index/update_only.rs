use std::borrow::Cow;

use itertools::Itertools as _;
use serde_json::Value;

use super::FullTextIndex;
use super::inverted_index::ARRAY_BOUNDARY_SENTINEL;
use super::tokenizers::Tokenizer;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::update_only::{UpdateOnlyIndexKind, extracted_values};

/// Writes what [`MutableFullTextIndex`] persists: the point's tokenized
/// document, in the same encoding, so that whoever opens the index next
/// rebuilds the same inverted index from it.
///
/// Tokenizing is all this does — the token ids and postings the mutable side
/// also maintains live only in that in-memory index.
///
/// [`MutableFullTextIndex`]: super::mutable_text_index::MutableFullTextIndex
pub struct UpdateOnlyTextKind {
    phrase_matching: bool,
    tokenizer: Tokenizer,
}

impl UpdateOnlyTextKind {
    pub fn new(config: &TextIndexParams) -> Self {
        Self {
            phrase_matching: config.phrase_matching.unwrap_or_default(),
            tokenizer: Tokenizer::new_from_text_index_params(config),
        }
    }
}

impl UpdateOnlyIndexKind for UpdateOnlyTextKind {
    type Stored = Vec<u8>;

    fn extract(&self, values: &[&Value]) -> OperationResult<Option<Vec<u8>>> {
        let values = extracted_values::<FullTextIndex>(values);
        if values.is_empty() {
            return Ok(None);
        }

        // A sentinel between the values of an array keeps a phrase from
        // matching across two of them.
        let insert_boundaries = self.phrase_matching && values.len() > 1;

        let mut str_tokens: Vec<Cow<str>> =
            Vec::with_capacity((values.len() * 2).saturating_sub(1));
        for (i, value) in values.iter().enumerate() {
            if insert_boundaries && i > 0 {
                str_tokens.push(Cow::Borrowed(ARRAY_BOUNDARY_SENTINEL));
            }
            self.tokenizer.tokenize_doc(value, |token| {
                str_tokens.push(token);
            });
        }

        // Phrase matching needs the tokens in the order they were written;
        // without it only membership matters, so they are stored sorted and
        // deduplicated.
        let tokens_to_store = if self.phrase_matching {
            str_tokens
        } else {
            str_tokens.into_iter().sorted().dedup().collect()
        };

        Ok(Some(FullTextIndex::serialize_document(tokens_to_store)?))
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, MmapFs};
    use serde_json::json;
    use tempfile::TempDir;

    use super::super::mutable_text_index::MutableFullTextIndex;
    use crate::data_types::index::{TextIndexParams, TextIndexType};
    use crate::index::field_index::UpdateOnlyFieldIndex;
    use crate::index::payload_config::{
        FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
    };
    use crate::json_path::JsonPath;
    use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

    /// The document a point is tokenized into survives the append-only writer
    /// and is picked up by the appendable index that rebuilds the inverted
    /// index from it.
    #[test]
    fn full_text_index_round_trip() {
        let dir = TempDir::with_prefix("update_only_text").unwrap();
        let hw_counter = HardwareCounterCell::new();
        let field = JsonPath::new("f");

        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            ..Default::default()
        };
        let index_type = FullPayloadIndexType {
            index_type: PayloadIndexType::FullTextIndex,
            mutability: IndexMutability::Mutable,
            storage_type: StorageType::Gridstore,
        };
        let storage = index_type.index_type.storage_dir(dir.path(), &field);

        let mut writer: UpdateOnlyFieldIndex<MmapFile> = UpdateOnlyFieldIndex::open(
            MmapFs,
            dir.path(),
            &field,
            &PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(params.clone())),
            &index_type,
        )
        .unwrap()
        .unwrap();

        writer
            .add_point(0, &[&json!("the quick brown fox")], &hw_counter)
            .unwrap();
        // A value the text index cannot read stores nothing.
        writer.add_point(1, &[&json!(42)], &hw_counter).unwrap();
        writer.flush().unwrap();

        let index = MutableFullTextIndex::open_gridstore(storage, params, false)
            .unwrap()
            .unwrap();

        let mut tokens = index.get_doc(0).expect("document stored for slot 0");
        tokens.sort();
        assert_eq!(tokens, vec!["brown", "fox", "quick", "the"]);
        assert!(index.get_doc(1).is_none(), "slot 1 stored nothing");
    }
}
