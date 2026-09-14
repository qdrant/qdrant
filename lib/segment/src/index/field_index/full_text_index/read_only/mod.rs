// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use common::universal_io::UniversalRead;

use super::mutable_text_index::read_only::ReadOnlyAppendableFullTextIndex;
use super::on_disk_text_index::OnDiskFullTextIndex;
use crate::index::field_index::full_text_index::immutable_text_index::ImmutableFullTextIndex;

mod lifecycle;
mod live_reload;
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
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use rstest::rstest;
    use tempfile::TempDir;

    use super::super::FullTextIndex;
    use super::ReadOnlyFullTextIndex;
    use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
    use crate::index::field_index::{
        LiveReload, PayloadFieldIndex, PayloadFieldIndexRead, ValueIndexer,
    };
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, Match, MatchPhrase};

    fn test_config() -> TextIndexParams {
        TextIndexParams {
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
        assert_eq!(index.count_indexed_points().unwrap(), payloads.len());

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

    /// The incremental `LiveReload` path must land on exactly the same state as
    /// a fresh `open_appendable` over the post-write gridstore.
    ///
    /// A writer keeps mutating after the read-only view is open: one point is
    /// deleted and two are appended. `live_reload` is handed only that delta and
    /// replays the stored documents itself, so it re-runs the same
    /// post-tokenization indexing the write path runs. Both token-set matching
    /// and phrase matching are checked, because the phrase leg is the one that
    /// depends on the ordered document being indexed as well as the token set.
    #[rstest]
    fn live_reload_matches_fresh_open(#[values(false, true)] phrase_matching: bool) {
        let dir = TempDir::with_prefix("ro_fulltext_live_reload").unwrap();
        let mut config = test_config();
        config.phrase_matching = Some(phrase_matching);
        let hw_counter = HardwareCounterCell::new();

        let initial = [
            serde_json::json!("the quick brown fox jumps"),
            serde_json::json!("over the lazy dog"),
            serde_json::json!("the brown bear sleeps"),
        ];
        // Appended after the read-only view is taken. The last one repeats a
        // term so the document is not just a set of distinct tokens.
        let appended = [
            serde_json::json!("a lazy brown moth"),
            serde_json::json!("the fox and the fox again"),
        ];

        let mut writer =
            FullTextIndex::new_gridstore(dir.path().to_path_buf(), config.clone(), true)
                .unwrap()
                .unwrap();
        for (idx, payload) in initial.iter().enumerate() {
            writer
                .add_point(idx as u32, &[payload], &hw_counter)
                .unwrap();
        }
        writer.flusher()().unwrap();

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        // Read-only view of points 0..=2, taken before the writer continues.
        let mut reloaded: ReadOnlyFullTextIndex<ReadOnly<MmapFile>> =
            ReadOnlyFullTextIndex::open_appendable(&fs, dir.path().to_path_buf(), config.clone())
                .unwrap()
                .unwrap();

        // Writer's delta: drop point 1, append points 3 and 4.
        writer.remove_point(1).unwrap();
        for (offset, payload) in appended.iter().enumerate() {
            writer
                .add_point((3 + offset) as u32, &[payload], &hw_counter)
                .unwrap();
        }
        writer.flusher()().unwrap();

        let deleted: [PointOffsetType; 1] = [1];
        let added: [PointOffsetType; 2] = [3, 4];
        reloaded
            .live_reload(
                &fs,
                &SortedSlice::new(&deleted).unwrap(),
                &SortedSlice::new(&added).unwrap(),
                &hw_counter,
            )
            .unwrap();

        let fresh: ReadOnlyFullTextIndex<ReadOnly<MmapFile>> =
            ReadOnlyFullTextIndex::open_appendable(&fs, dir.path().to_path_buf(), config)
                .unwrap()
                .unwrap();

        assert_eq!(
            reloaded.count_indexed_points().unwrap(),
            fresh.count_indexed_points().unwrap(),
        );

        let key = JsonPath::new("test");
        let mut conditions = vec![
            // Spans deleted, surviving and appended points.
            FieldCondition::new_match(key.clone(), Match::new_text("brown")),
            // Only ever present on the deleted point.
            FieldCondition::new_match(key.clone(), Match::new_text("dog")),
            // Only on appended points.
            FieldCondition::new_match(key.clone(), Match::new_text("moth")),
            // Repeated within one appended document.
            FieldCondition::new_match(key.clone(), Match::new_text("fox")),
            // Multi-token, so it exercises the posting intersection.
            FieldCondition::new_match(key.clone(), Match::new_text("lazy brown")),
        ];
        if phrase_matching {
            // Needs the ordered document, not just the token set: "brown fox"
            // is adjacent in point 0 but not in point 3 ("lazy brown moth").
            conditions.push(FieldCondition::new_match(
                key.clone(),
                Match::Phrase(MatchPhrase::from("brown fox")),
            ));
            conditions.push(FieldCondition::new_match(
                key,
                Match::Phrase(MatchPhrase::from("lazy brown")),
            ));
        }

        for condition in &conditions {
            let from_reload = reloaded
                .filter(condition, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec();
            let from_fresh = fresh
                .filter(condition, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec();
            assert_eq!(from_reload, from_fresh, "diverged for {condition:?}");
            // The deleted point must not survive in either.
            assert!(
                !from_reload.contains(&1),
                "point 1 resurrected by {condition:?}"
            );
        }

        // Guard the fixture: the comparisons above hold vacuously if nothing
        // matches, so pin the results that must come from the reloaded delta.
        let key = JsonPath::new("test");
        let expect = |condition: &FieldCondition, want: Vec<PointOffsetType>| {
            let got = reloaded
                .filter(condition, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec();
            assert_eq!(got, want, "unexpected hits for {condition:?}");
        };

        // Appended point only.
        expect(
            &FieldCondition::new_match(key.clone(), Match::new_text("moth")),
            vec![3],
        );
        // Survivor plus both appended points.
        expect(
            &FieldCondition::new_match(key.clone(), Match::new_text("fox")),
            vec![0, 4],
        );
        // The deleted point was the only holder of this term.
        expect(
            &FieldCondition::new_match(key.clone(), Match::new_text("dog")),
            vec![],
        );

        if phrase_matching {
            // Adjacency has to survive the reload, not just term membership:
            // point 3 has "lazy brown" adjacent, point 0 has "brown fox".
            expect(
                &FieldCondition::new_match(
                    key.clone(),
                    Match::Phrase(MatchPhrase::from("lazy brown")),
                ),
                vec![3],
            );
            expect(
                &FieldCondition::new_match(key, Match::Phrase(MatchPhrase::from("brown fox"))),
                vec![0],
            );
        }
    }
}
