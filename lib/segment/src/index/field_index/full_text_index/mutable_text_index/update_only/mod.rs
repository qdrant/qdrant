use serde_json::Value;

use super::super::FullTextIndex;
use super::super::tokenizers::Tokenizer;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{UpdateOnlyIndexKind, ValueIndexer};

/// Writes what [`MutableFullTextIndex`] persists: the point's tokenized
/// document, through the same [`FullTextIndex::tokenize_document`], so that
/// whoever opens the index next rebuilds the same inverted index from it.
///
/// [`MutableFullTextIndex`]: super::MutableFullTextIndex
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
        let values = <FullTextIndex as ValueIndexer>::flatten_values(values);
        if values.is_empty() {
            return Ok(None);
        }

        let str_tokens =
            FullTextIndex::tokenize_document(&self.tokenizer, self.phrase_matching, &values);

        Ok(Some(FullTextIndex::serialize_stored_document(
            str_tokens,
            self.phrase_matching,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, MmapFs};
    use serde_json::json;
    use tempfile::TempDir;

    use super::super::MutableFullTextIndex;
    use crate::data_types::index::{TextIndexParams, TextIndexType};
    use crate::index::field_index::UpdateOnlyFieldIndex;
    use crate::index::payload_config::{
        FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
    };
    use crate::json_path::JsonPath;
    use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

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
            &MmapFs,
            dir.path(),
            &field,
            &PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(params.clone())),
            &index_type,
        )
        .unwrap();

        writer
            .add_point(&MmapFs, 0, &[&json!("the quick brown fox")], &hw_counter)
            .unwrap();
        // A value the text index cannot read stores nothing.
        writer
            .add_point(&MmapFs, 1, &[&json!(42)], &hw_counter)
            .unwrap();
        writer.flush(&MmapFs, &hw_counter).unwrap();

        let index = MutableFullTextIndex::open_gridstore(storage, params, false)
            .unwrap()
            .unwrap();

        let mut tokens = index.get_doc(0).expect("document stored for slot 0");
        tokens.sort();
        assert_eq!(tokens, vec!["brown", "fox", "quick", "the"]);
        assert!(index.get_doc(1).is_none(), "slot 1 stored nothing");
    }
}
