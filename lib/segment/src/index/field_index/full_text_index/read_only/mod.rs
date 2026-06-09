use common::universal_io::UniversalRead;

use super::mutable_text_index::read_only::ReadOnlyAppendableFullTextIndex;
use super::on_disk_text_index::OnDiskFullTextIndex;
use crate::index::field_index::full_text_index::immutable_text_index::ImmutableFullTextIndex;

mod lifecycle;
mod read_ops;

/// Read-only counterpart of [`FullTextIndex`][1], parameterised by a
/// [`UniversalRead`] storage.
///
/// Dispatches the [`FullTextIndexRead`][2] / [`PayloadFieldIndexRead`][3] read
/// surface to one of two backing formats:
/// - [`Appendable`][Self::Appendable] — the in-RAM appendable index loaded from
///   the gridstore (write) format;
/// - [`Immutable`][Self::Immutable] — reads directly from the immutable mmap
///   format.
///
/// Mirrors [`ReadOnlyMapIndex`][4]; the [`PayloadFieldIndexRead`] body (filter /
/// cardinality / payload blocks / condition checker) is shared with the
/// writable variant through the [`FullTextIndexRead`] trait and the free
/// functions in [`read_ops`][5]. Constructed via [`Self::open_appendable`] /
/// [`Self::open_immutable`] (see [`lifecycle`]); the upstream
/// [`ReadOnlyFieldIndex`][6] wiring follows in a separate PR.
///
/// [1]: super::FullTextIndex
/// [2]: super::full_text_index_read::FullTextIndexRead
/// [3]: crate::index::field_index::PayloadFieldIndexRead
/// [4]: crate::index::field_index::map_index::read_only::ReadOnlyMapIndex
/// [5]: super::read_ops
/// [6]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
#[allow(clippy::large_enum_variant)]
pub enum ReadOnlyFullTextIndex<S: UniversalRead> {
    /// Loads into RAM from appendable storage format
    Appendable(ReadOnlyAppendableFullTextIndex<S>),
    /// Loads into RAM from immutable format
    Immutable(ImmutableFullTextIndex<S>),
    /// Directly reads from storage in immutable format
    OnDisk(OnDiskFullTextIndex<S>),
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use tempfile::TempDir;

    use super::super::FullTextIndex;
    use super::ReadOnlyFullTextIndex;
    use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
    use crate::index::field_index::{PayloadFieldIndex, PayloadFieldIndexRead, ValueIndexer};
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, Match};

    fn test_config() -> TextIndexParams {
        TextIndexParams {
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
        }
    }

    /// Build an appendable (Gridstore) full-text index on disk, then open it
    /// via the parent enum's [`ReadOnlyFullTextIndex::open_appendable`] over
    /// the write-enforced `ReadOnly<MmapFile>` backend. Verifies the
    /// dispatcher wraps into [`ReadOnlyFullTextIndex::Appendable`] and that
    /// the trait forwarders return the same hit set as the documents inserted.
    #[test]
    fn parent_open_appendable_round_trip() {
        let dir = TempDir::with_prefix("ro_fulltext_parent_gridstore").unwrap();
        let config = test_config();
        let hw_counter = HardwareCounterCell::new();

        let payloads = [
            serde_json::json!("the quick brown fox jumps"),
            serde_json::json!("over the lazy dog"),
            serde_json::json!("the brown bear sleeps"),
        ];

        {
            let mut index =
                FullTextIndex::new_gridstore(dir.path().to_path_buf(), config.clone(), true)
                    .unwrap()
                    .unwrap();
            for (idx, payload) in payloads.iter().enumerate() {
                index
                    .add_point(idx as u32, &[payload], &hw_counter)
                    .unwrap();
            }
            index.flusher()().unwrap();
        }

        // `S = ReadOnly<MmapFile>` → `S::Fs = ReadOnlyFs<MmapFs>`, named via the
        // associated-type projection since the wrapper type isn't exported.
        // The read-only filesystem context is `Default`.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let index: ReadOnlyFullTextIndex<ReadOnly<MmapFile>> =
            ReadOnlyFullTextIndex::open_appendable(&fs, dir.path().to_path_buf(), config)
                .unwrap()
                .unwrap();

        // Dispatcher wraps the leaf into the right variant.
        assert!(matches!(index, ReadOnlyFullTextIndex::Appendable(_)));

        // Trait dispatch on the parent enum forwards into the leaf:
        // every document was indexed (3 points), and `brown` matches the two
        // that contain it while `lazy` matches only the second.
        assert_eq!(index.count_indexed_points(), payloads.len());

        let key = JsonPath::new("test");
        let brown = FieldCondition::new_match(key.clone(), Match::new_text("brown"));
        let lazy = FieldCondition::new_match(key, Match::new_text("lazy"));

        assert_eq!(
            index
                .filter(&brown, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2],
        );
        assert_eq!(
            index
                .filter(&lazy, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![1],
        );
    }
}
