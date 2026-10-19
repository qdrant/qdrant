use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use common::universal_io::{MmapFile, MmapFs, Populate};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;

use super::super::InvertedIndex;
use super::super::immutable_inverted_index::ImmutableInvertedIndex;
use super::super::immutable_postings_enum::ImmutablePostings;
use super::super::mutable_inverted_index::MutableInvertedIndex;
use super::super::mutable_inverted_index_builder::MutableInvertedIndexBuilder;
use super::super::on_disk_inverted_index::OnDiskInvertedIndex;
use super::super::term_frequencies::TermFrequencies;
use super::*;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::query_context::fancy_idf;
use crate::index::condition_checker::{ConditionCheckerEnum, TestBitOfId};
use crate::index::query_optimization::optimized_filter::OptimizedFilter;

const VOCAB: usize = 40;

/// The vocabulary, for queries long enough to keep many terms essential.
const WORDS: [&str; VOCAB] = [
    "w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10", "w11", "w12", "w13", "w14",
    "w15", "w16", "w17", "w18", "w19", "w20", "w21", "w22", "w23", "w24", "w25", "w26", "w27",
    "w28", "w29", "w30", "w31", "w32", "w33", "w34", "w35", "w36", "w37", "w38", "w39",
];

/// A skewed vocabulary: low ranks are common, high ranks rare, so queries
/// mix terms with very different bounds and MaxScore has something to prune.
fn word(rng: &mut StdRng) -> String {
    let rank = (rng.random::<f64>().powi(3) * VOCAB as f64) as usize;
    format!("w{}", rank.min(VOCAB - 1))
}

fn fixture(seed: u64, documents: u32, deleted: &[PointOffsetType]) -> MutableInvertedIndex {
    fixture_with(true, seed, documents, deleted)
}

/// The same corpus for the same seed, with or without positions: token ids
/// are registered in the same order either way.
fn fixture_with(
    positions: bool,
    seed: u64,
    documents: u32,
    deleted: &[PointOffsetType],
) -> MutableInvertedIndex {
    let mut rng = StdRng::seed_from_u64(seed);
    let _scope = ambient::test_guard();
    let mut index = MutableInvertedIndex::new(positions, true);
    for idx in 0..documents {
        let len = rng.random_range(3..=60);
        let tokens: Vec<String> = (0..len).map(|_| word(&mut rng)).collect();
        index.index_str_tokens(idx, &tokens, Some(len)).unwrap();
    }
    for &idx in deleted {
        index.remove(idx);
    }
    index
}

/// Corpus statistics over the live documents of `index`, as the shard-wide
/// gather would produce them for an appendable segment.
fn query(index: &MutableInvertedIndex, terms: &[&str], params: Bm25Params) -> Bm25Query {
    let live = index.points_count as f32;
    let avg = index.total_tokens as f32 / live;
    let terms = terms.iter().filter_map(|term| {
        let token_id = *index.vocab.get(*term)?;
        let df = index.postings[token_id as usize].len() as f32;
        Some(Bm25Term {
            token_id,
            idf: fancy_idf(live, df).max(0.0),
        })
    });
    Bm25Query::new(terms, params, Some(avg)).unwrap()
}

/// Plain BM25 over every live document, by definition rather than by
/// posting lists: the number the engine has to reproduce.
fn reference(
    index: &MutableInvertedIndex,
    query: &Bm25Query,
    accept: impl Fn(PointOffsetType) -> bool,
) -> Vec<ScoredPointOffset> {
    let documents = index.point_to_doc.as_ref().unwrap();
    let lengths = index.point_to_doc_len.as_ref().unwrap();
    let mut scored: Vec<ScoredPointOffset> = documents
        .iter()
        .enumerate()
        .filter_map(|(idx, document)| {
            let document = document.as_ref()?;
            let idx = idx as PointOffsetType;
            if !accept(idx) {
                return None;
            }
            let mut score = 0.0;
            for term in query.terms() {
                let tf = document
                    .tokens()
                    .iter()
                    .filter(|token| **token == term.token_id)
                    .count() as u32;
                if tf > 0 {
                    score += query.term_score(term.idf, tf, Some(lengths[idx as usize]));
                }
            }
            (score > 0.0).then_some(ScoredPointOffset { idx, score })
        })
        .collect();
    scored.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
    scored
}

/// `actual` is a valid top-k of `expected`: same length, same scores in
/// order, and every returned document carries its reference score. Ids are
/// only compared through their scores, since equal scores may legitimately
/// come out in either order.
fn assert_top_k(actual: &[ScoredPointOffset], expected: &[ScoredPointOffset], limit: usize) {
    assert_eq!(
        actual.len(),
        expected.len().min(limit),
        "{actual:?}\n{expected:?}"
    );
    let by_id: HashMap<PointOffsetType, ScoreType> =
        expected.iter().map(|hit| (hit.idx, hit.score)).collect();
    for (i, (hit, reference)) in actual.iter().zip(expected).enumerate() {
        assert!(
            (hit.score - reference.score).abs() <= 1e-4 * reference.score.abs().max(1.0),
            "rank {i}: engine {hit:?}, reference {reference:?}",
        );
        let own = by_id
            .get(&hit.idx)
            .copied()
            .unwrap_or_else(|| panic!("document {} is not in the reference ranking", hit.idx));
        assert!(
            (hit.score - own).abs() <= 1e-4 * own.abs().max(1.0),
            "document {}: engine {}, reference {own}",
            hit.idx,
            hit.score,
        );
    }
    let mut ids: Vec<_> = actual.iter().map(|hit| hit.idx).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), actual.len(), "a document was returned twice");
}

fn on_disk(
    dir: &std::path::Path,
    mutable: &MutableInvertedIndex,
    deleted: &BitVec,
) -> OnDiskInvertedIndex<MmapFile> {
    on_disk_with(true, dir, mutable, deleted)
}

fn on_disk_with(
    positions: bool,
    dir: &std::path::Path,
    mutable: &MutableInvertedIndex,
    deleted: &BitVec,
) -> OnDiskInvertedIndex<MmapFile> {
    OnDiskInvertedIndex::create(dir.to_path_buf(), mutable.clone()).unwrap();
    OnDiskInvertedIndex::<MmapFile>::open(
        &MmapFs,
        dir.to_path_buf(),
        Populate::No,
        positions,
        deleted,
    )
    .unwrap()
    .unwrap()
}

fn run<I: InvertedIndex>(
    index: &I,
    query: &Bm25Query,
    accept: impl Fn(PointOffsetType) -> bool,
    limit: usize,
) -> Vec<ScoredPointOffset> {
    ambient::test(|| {
        index.score_bm25(
            query,
            &Bm25Accept::new(&accept, None),
            limit,
            &AtomicBool::new(false),
        )
    })
    .unwrap()
}

fn queries() -> Vec<Vec<&'static str>> {
    vec![
        vec!["w0"],
        vec!["w39"],
        vec!["w0", "w1"],
        vec!["w0", "w25"],
        vec!["w3", "w17", "w30"],
        vec!["w0", "w1", "w2", "w3", "w4"],
        vec!["w12", "w38", "w39"],
        vec!["w7", "w7", "w9"],
        vec!["w5", "unknown"],
        vec!["unknown"],
        // Long queries, as on ArguAna: enough essential terms for the cursor
        // heap to find the candidates.
        WORDS[..20].to_vec(),
        WORDS[10..].to_vec(),
        WORDS.to_vec(),
    ]
}

/// The cursor heap and the scan find the same candidates with the same hits
/// in the same order, so the two give bit-identical results, at every block
/// size and with or without pruning.
#[rstest]
fn cursor_heap_matches_the_scan(#[values(1, 10, 1000)] limit: usize) {
    heap_matches_scan::<1>(limit);
    heap_matches_scan::<ON_DISK_BLOCK>(limit);
}

fn heap_matches_scan<const BLOCK: usize>(limit: usize) {
    let index = fixture(31, 600, &[2, 20, 200]);
    let lengths = index.point_to_doc_len.as_deref().unwrap();
    let accept = Bm25Accept::new(&|idx| idx % 7 != 3, None);
    let rank = |query: &Bm25Query, heap_min_essential: usize| {
        let postings = query
            .terms()
            .iter()
            .map(|term| index.postings.get(term.token_id as usize))
            .collect();
        let mut cursors = MutableCursors::new(postings);
        score_top_k_with::<_, BLOCK>(
            query,
            &mut cursors,
            lengths.len(),
            |point_ids, out| {
                for (point_id, doc_len) in point_ids.iter().zip(out) {
                    *doc_len = Some(lengths[*point_id as usize]);
                }
                Ok(())
            },
            &accept,
            limit,
            &AtomicBool::new(false),
            Strategy {
                heap_min_essential,
                dense_min_postings_per_doc: f32::INFINITY,
                dense_window: usize::MAX,
            },
        )
        .unwrap()
        .into_iter()
        .map(|hit| (hit.idx, hit.score.to_bits()))
        .collect::<Vec<_>>()
    };
    for terms in queries() {
        let query = query(&index, &terms, Bm25Params::default());
        let scan = rank(&query, usize::MAX);
        assert_eq!(rank(&query, 1), scan, "block {BLOCK}, {terms:?}");
        assert_eq!(rank(&query, 16), scan, "block {BLOCK}, {terms:?}");
    }
}

/// Every shape reproduces the definition, with and without pruning in
/// play, with deletions applied before the conversion to the immutable
/// shapes and again after.
#[rstest]
fn every_shape_matches_the_reference(#[values(1, 3, 10, 1000)] limit: usize) {
    let deleted_before = [4, 5, 6, 100, 250];
    let deleted_after = [7, 8, 300];

    let mut mutable = fixture(11, 400, &deleted_before);
    let mut immutable = ImmutableInvertedIndex::from(mutable.clone());
    let dir = tempfile::tempdir().unwrap();
    let mut after_mask = BitVec::repeat(false, 400);
    for &idx in &deleted_after {
        after_mask.set(idx as usize, true);
    }
    let on_disk = on_disk(dir.path(), &mutable, &after_mask);
    let from_disk = ImmutableInvertedIndex::try_from(&on_disk).unwrap();
    for &idx in &deleted_after {
        mutable.remove(idx);
        immutable.remove(idx);
    }

    for terms in queries() {
        let query = query(&mutable, &terms, Bm25Params::default());
        let expected = reference(&mutable, &query, |_| true);

        eprintln!("{terms:?} limit {limit}");
        assert_top_k(&run(&mutable, &query, |_| true, limit), &expected, limit);
        assert_top_k(&run(&immutable, &query, |_| true, limit), &expected, limit);
        assert_top_k(&run(&on_disk, &query, |_| true, limit), &expected, limit);
        assert_top_k(&run(&from_disk, &query, |_| true, limit), &expected, limit);
    }
}

/// Blocking changes when lengths are read, never the ranking: every block
/// size reproduces the definition, asks for each candidate's length once,
/// and asks in batches no larger than the block.
#[rstest]
fn block_size_does_not_change_the_ranking(#[values(1, 10, 1000)] limit: usize) {
    rank_in_blocks::<1>(limit);
    rank_in_blocks::<2>(limit);
    rank_in_blocks::<7>(limit);
    rank_in_blocks::<ON_DISK_BLOCK>(limit);
    rank_in_blocks::<1000>(limit);
}

fn rank_in_blocks<const BLOCK: usize>(limit: usize) {
    let index = fixture(23, 500, &[3, 30, 300]);
    let lengths = index.point_to_doc_len.as_deref().unwrap();
    for terms in queries() {
        let query = query(&index, &terms, Bm25Params::default());
        let postings = query
            .terms()
            .iter()
            .map(|term| index.postings.get(term.token_id as usize))
            .collect();
        let mut cursors = MutableCursors::new(postings);
        let mut asked: Vec<PointOffsetType> = Vec::new();
        let actual = score_top_k::<_, BLOCK>(
            &query,
            &mut cursors,
            lengths.len(),
            |point_ids, out| {
                assert!(point_ids.len() <= BLOCK);
                asked.extend_from_slice(point_ids);
                for (point_id, doc_len) in point_ids.iter().zip(out) {
                    *doc_len = Some(lengths[*point_id as usize]);
                }
                Ok(())
            },
            &Bm25Accept::new(&|_| true, None),
            limit,
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_top_k(&actual, &reference(&index, &query, |_| true), limit);
        let asked_count = asked.len();
        asked.sort_unstable();
        asked.dedup();
        assert_eq!(
            asked.len(),
            asked_count,
            "block {BLOCK}: a length was read twice"
        );
    }
}

/// The outer filter, checked a block at a time, keeps exactly the documents
/// it allows on every shape and block size, whatever order its batched check
/// leaves them in: this one keeps odd ids and reverses what it is given.
#[rstest]
fn the_filter_is_checked_per_block(#[values(1, 10, 1000)] limit: usize) {
    let mutable = fixture(37, 400, &[8, 9, 100]);
    let immutable = ImmutableInvertedIndex::from(mutable.clone());
    let dir = tempfile::tempdir().unwrap();
    let on_disk = on_disk(dir.path(), &mutable, &BitVec::new());
    let odd = OptimizedFilter::from_checker(ConditionCheckerEnum::TestBitOfId(TestBitOfId(0)));
    let accept = Bm25Accept::new(&|_| true, Some(&odd));
    let is_odd = |idx: PointOffsetType| idx % 2 == 1;
    let is_stopped = AtomicBool::new(false);
    let _scope = ambient::test_guard();

    for terms in queries() {
        let query = query(&mutable, &terms, Bm25Params::default());
        let expected = reference(&mutable, &query, is_odd);
        for actual in [
            mutable.score_bm25(&query, &accept, limit, &is_stopped),
            immutable.score_bm25(&query, &accept, limit, &is_stopped),
            on_disk.score_bm25(&query, &accept, limit, &is_stopped),
        ] {
            let actual = actual.unwrap();
            assert_top_k(&actual, &expected, limit);
            assert!(actual.iter().all(|hit| is_odd(hit.idx)));
        }
        filtered_in_blocks::<2>(&mutable, &query, &accept, &expected, limit);
        filtered_in_blocks::<7>(&mutable, &query, &accept, &expected, limit);
        filtered_in_blocks::<ON_DISK_BLOCK>(&mutable, &query, &accept, &expected, limit);
    }
}

fn filtered_in_blocks<const BLOCK: usize>(
    index: &MutableInvertedIndex,
    query: &Bm25Query,
    accept: &Bm25Accept<'_>,
    expected: &[ScoredPointOffset],
    limit: usize,
) {
    let lengths = index.point_to_doc_len.as_deref().unwrap();
    let postings = query
        .terms()
        .iter()
        .map(|term| index.postings.get(term.token_id as usize))
        .collect();
    let mut cursors = MutableCursors::new(postings);
    let actual = score_top_k::<_, BLOCK>(
        query,
        &mut cursors,
        lengths.len(),
        |point_ids, out| {
            for (point_id, doc_len) in point_ids.iter().zip(out) {
                *doc_len = Some(lengths[*point_id as usize]);
            }
            Ok(())
        },
        accept,
        limit,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_top_k(&actual, expected, limit);
}

/// The caller's filter is applied on every shape, and the pruning
/// threshold is only ever raised by accepted documents.
#[test]
fn accept_restricts_the_ranking() {
    let mutable = fixture(5, 300, &[]);
    let immutable = ImmutableInvertedIndex::from(mutable.clone());
    let dir = tempfile::tempdir().unwrap();
    let on_disk = on_disk(dir.path(), &mutable, &BitVec::new());
    let even = |idx: PointOffsetType| idx.is_multiple_of(2);

    for terms in queries() {
        let query = query(&mutable, &terms, Bm25Params::default());
        let expected = reference(&mutable, &query, even);
        for actual in [
            run(&mutable, &query, even, 7),
            run(&immutable, &query, even, 7),
            run(&on_disk, &query, even, 7),
        ] {
            assert_top_k(&actual, &expected, 7);
            assert!(actual.iter().all(|hit| even(hit.idx)));
        }
    }
}

/// A filter condition whose every check fails, as a storage read can.
struct FailingCondition;

impl ConditionChecker for FailingCondition {
    type Error = OperationError;

    fn check(&self, _point_id: PointOffsetType) -> OperationResult<bool> {
        Err(OperationError::service_error("filter read failed"))
    }

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        default_check_batched(ids, select, rest, |id| self.check(id))
    }
}

/// A filter that fails to read stops the query with its error, on every
/// shape, instead of leaving the point out of the ranking.
#[test]
fn a_failing_filter_reports_its_error() {
    let mutable = fixture(7, 100, &[]);
    let immutable = ImmutableInvertedIndex::from(mutable.clone());
    let dir = tempfile::tempdir().unwrap();
    let on_disk = on_disk(dir.path(), &mutable, &BitVec::new());
    let query = query(&mutable, &["w0", "w1"], Bm25Params::default());

    let filter =
        OptimizedFilter::from_checker(ConditionCheckerEnum::Dyn(Box::new(FailingCondition)));
    let accept = Bm25Accept::new(&|_| true, Some(&filter));
    let is_stopped = AtomicBool::new(false);
    let _scope = ambient::test_guard();
    for result in [
        mutable.score_bm25(&query, &accept, 10, &is_stopped),
        immutable.score_bm25(&query, &accept, 10, &is_stopped),
        on_disk.score_bm25(&query, &accept, 10, &is_stopped),
    ] {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("filter read failed"), "{error}");
    }
}

/// No average length means no length normalization: the same ranking as
/// `b = 0` with one, which is what the sparse route produces.
#[test]
fn missing_average_length_degrades_to_b_zero() {
    let _scope = ambient::test_guard();
    let is_stopped = AtomicBool::new(false);
    let mutable = fixture(9, 200, &[]);

    let terms = ["w0", "w2", "w20"];
    let with_b_zero = query(&mutable, &terms, Bm25Params { k1: 1.2, b: 0.0 });
    let expected = reference(&mutable, &with_b_zero, |_| true);

    let without_average = Bm25Query::new(
        query(&mutable, &terms, Bm25Params::default())
            .terms()
            .iter()
            .copied(),
        Bm25Params::default(),
        None,
    )
    .unwrap();
    let actual = mutable
        .score_bm25(
            &without_average,
            &Bm25Accept::new(&|_| true, None),
            20,
            &is_stopped,
        )
        .unwrap();
    assert_top_k(&actual, &expected, 20);

    // And the normalization does change the answer when it is available,
    // so the test above is not vacuous.
    let normalized = mutable
        .score_bm25(
            &query(&mutable, &terms, Bm25Params::default()),
            &Bm25Accept::new(&|_| true, None),
            20,
            &is_stopped,
        )
        .unwrap();
    assert_ne!(
        normalized.iter().map(|hit| hit.idx).collect::<Vec<_>>(),
        actual.iter().map(|hit| hit.idx).collect::<Vec<_>>(),
    );
}

/// An index without positions ranks exactly as one with them: every shape,
/// frequencies from `term_frequencies.dat` on the immutable and on-disk ones, deletions before and after the conversion, and a term
/// repeated far past a block's width in one document.
#[rstest]
fn counts_only_shapes_match_the_positional_reference(#[values(1, 10, 1000)] limit: usize) {
    let deleted_before = [4, 5, 6, 100, 250];
    let deleted_after = [7, 8, 300];
    let repeated: Vec<String> = vec!["w1".to_string(); 300];

    let mut positional = fixture_with(true, 11, 400, &deleted_before);
    let mut mutable = fixture_with(false, 11, 400, &deleted_before);
    let _scope = ambient::test_guard();
    for index in [&mut positional, &mut mutable] {
        index.index_str_tokens(400, &repeated, Some(300)).unwrap();
    }
    assert!(mutable.point_to_doc.is_none(), "no positions kept");

    let mut immutable = ImmutableInvertedIndex::from(mutable.clone());
    assert!(matches!(immutable.postings, ImmutablePostings::Ids(_)));
    assert!(immutable.term_frequencies.is_some());
    let dir = tempfile::tempdir().unwrap();
    let mut after_mask = BitVec::repeat(false, 401);
    for &idx in &deleted_after {
        after_mask.set(idx as usize, true);
    }
    let on_disk = on_disk_with(false, dir.path(), &mutable, &after_mask);
    let from_disk = ImmutableInvertedIndex::try_from(&on_disk).unwrap();
    for &idx in &deleted_after {
        positional.remove(idx);
        mutable.remove(idx);
        immutable.remove(idx);
    }

    let mut queries = queries();
    queries.push(vec!["w1"]);
    queries.push(vec!["w1", "w2"]);
    for terms in queries {
        let query = query(&positional, &terms, Bm25Params::default());
        let expected = reference(&positional, &query, |_| true);

        eprintln!("{terms:?} limit {limit}");
        assert_top_k(&run(&mutable, &query, |_| true, limit), &expected, limit);
        assert_top_k(&run(&immutable, &query, |_| true, limit), &expected, limit);
        assert_top_k(&run(&on_disk, &query, |_| true, limit), &expected, limit);
        assert_top_k(&run(&from_disk, &query, |_| true, limit), &expected, limit);
    }
}

/// Term frequencies from another build, holding a list for a different
/// number of terms than the postings, or a file cut short, are not trusted:
/// the index still opens, without them, and reports that it cannot score.
#[test]
fn term_frequencies_of_another_build_are_ignored() {
    let _scope = ambient::test_guard();
    let mutable = fixture_with(false, 11, 400, &[]);
    let immutable = ImmutableInvertedIndex::from(mutable.clone());
    let other = ImmutableInvertedIndex::from(fixture_with(false, 12, 3, &[]));
    assert_ne!(immutable.vocab.len(), other.vocab.len());

    let dir = tempfile::tempdir().unwrap();
    let deleted = BitVec::new();
    let opened = on_disk_with(false, dir.path(), &mutable, &deleted);
    assert!(opened.can_score());
    drop(opened);

    fs_err::write(
        dir.path().join("term_frequencies.dat"),
        other.term_frequencies.as_ref().unwrap(),
    )
    .unwrap();
    let reopened = OnDiskInvertedIndex::<MmapFile>::open(
        &MmapFs,
        dir.path().to_path_buf(),
        Populate::No,
        false,
        &deleted,
    )
    .unwrap()
    .unwrap();
    assert!(reopened.records_doc_len());
    assert!(
        !reopened.can_score(),
        "frequencies of another build are not used"
    );
    drop(reopened);

    let own = immutable.term_frequencies.as_ref().unwrap();
    fs_err::write(dir.path().join("term_frequencies.dat"), &own[..10]).unwrap();
    let reopened = OnDiskInvertedIndex::<MmapFile>::open(
        &MmapFs,
        dir.path().to_path_buf(),
        Populate::No,
        false,
        &deleted,
    )
    .unwrap()
    .unwrap();
    assert!(!reopened.can_score(), "a truncated file is not used");
}

#[test]
fn lengths_are_required() {
    let _scope = ambient::test_guard();
    let is_stopped = AtomicBool::new(false);
    let mut without_lengths = MutableInvertedIndex::new(true, false);
    without_lengths
        .index_str_tokens(0, ["alpha", "beta"], None)
        .unwrap();
    let query = Bm25Query::new(
        [Bm25Term {
            token_id: 0,
            idf: 1.0,
        }],
        Bm25Params::default(),
        None,
    )
    .unwrap();
    assert!(
        without_lengths
            .score_bm25(&query, &Bm25Accept::new(&|_| true, None), 10, &is_stopped)
            .is_err()
    );
    let immutable = ImmutableInvertedIndex::from(without_lengths);
    assert!(
        immutable
            .score_bm25(&query, &Bm25Accept::new(&|_| true, None), 10, &is_stopped)
            .is_err()
    );
}

#[test]
fn empty_query_and_zero_limit_return_nothing() {
    let _scope = ambient::test_guard();
    let is_stopped = AtomicBool::new(false);
    let mutable = fixture(3, 50, &[]);
    let empty = Bm25Query::new([], Bm25Params::default(), None).unwrap();
    assert!(
        mutable
            .score_bm25(&empty, &Bm25Accept::new(&|_| true, None), 10, &is_stopped)
            .unwrap()
            .is_empty()
    );
    let query = query(&mutable, &["w0"], Bm25Params::default());
    assert!(
        mutable
            .score_bm25(&query, &Bm25Accept::new(&|_| true, None), 0, &is_stopped)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn stop_flag_interrupts_the_scan() {
    let _scope = ambient::test_guard();
    let is_stopped = AtomicBool::new(true);
    let mutable = fixture(3, 3000, &[]);
    let query = query(&mutable, &["w0"], Bm25Params::default());
    assert!(
        mutable
            .score_bm25(&query, &Bm25Accept::new(&|_| true, None), 10, &is_stopped)
            .is_err()
    );
}

/// Repeated terms count once, and the terms come out ordered by bound.
#[test]
fn query_dedups_and_orders_by_bound() {
    let query = Bm25Query::new(
        [
            Bm25Term {
                token_id: 3,
                idf: 2.0,
            },
            Bm25Term {
                token_id: 1,
                idf: 0.5,
            },
            Bm25Term {
                token_id: 3,
                idf: 2.0,
            },
        ],
        Bm25Params::default(),
        None,
    )
    .unwrap();
    let ids: Vec<_> = query.terms().iter().map(|term| term.token_id).collect();
    assert_eq!(ids, [1, 3]);
}

/// `df` keeps deleted documents on the immutable shapes while `N` drops
/// them, so the statistics a gather reads there understate `IDF`. This
/// measures what that does to the ranking, against the definition over
/// live documents, and bounds it. The same index scored with exact
/// statistics reproduces the definition, so the gap is in the statistics
/// alone, not in the scorer.
#[test]
fn deleted_documents_inflate_df_on_immutable_shapes() {
    let _scope = ambient::test_guard();
    let mutable = fixture(21, 500, &[]);
    let mut immutable = ImmutableInvertedIndex::from(mutable.clone());
    let mut live = mutable;
    // One document in ten.
    for idx in (0..500).filter(|idx| idx % 10 == 0) {
        immutable.remove(idx);
        live.remove(idx);
    }
    let terms = ["w0", "w3", "w17", "w30"];

    let exact = query(&live, &terms, Bm25Params::default());
    let expected = reference(&live, &exact, |_| true);
    assert_top_k(&run(&immutable, &exact, |_| true, 20), &expected, 20);

    // The statistics as a gather reads them off the immutable index:
    // posting lengths still count the removed documents, the document
    // count does not.
    let n = immutable.points_count as f32;
    let avg = immutable.total_tokens as f32 / n;
    let inflated = Bm25Query::new(
        terms.iter().map(|term| {
            let token_id = immutable.vocab[*term];
            let df = immutable.get_posting_len(token_id).unwrap().unwrap() as f32;
            Bm25Term {
                token_id,
                idf: fancy_idf(n, df).max(0.0),
            }
        }),
        Bm25Params::default(),
        Some(avg),
    )
    .unwrap();
    let actual = run(&immutable, &inflated, |_| true, 20);

    let by_id: HashMap<PointOffsetType, ScoreType> =
        expected.iter().map(|hit| (hit.idx, hit.score)).collect();
    let max_relative_deviation = actual
        .iter()
        .map(|hit| {
            let reference = by_id[&hit.idx];
            (hit.score - reference).abs() / reference
        })
        .fold(0.0, ScoreType::max);
    eprintln!("max relative deviation with 10% deleted: {max_relative_deviation}");
    assert!(
        max_relative_deviation > 0.0,
        "the inflation should be visible"
    );
    assert!(
        max_relative_deviation < 0.25,
        "deleted documents move scores by {max_relative_deviation}"
    );
}

/// The postings a reopen rebuilds through the builder carry the same
/// frequencies as the ones the live path built.
#[test]
fn builder_rebuilds_the_same_frequencies() {
    let live = fixture(17, 200, &[3, 30]);
    let mut builder = MutableInvertedIndexBuilder::new(true, true);
    let documents = live.point_to_doc.as_ref().unwrap();
    let vocab: HashMap<TokenId, &str> =
        live.vocab.iter().map(|(s, id)| (*id, s.as_str())).collect();
    for (idx, document) in documents.iter().enumerate() {
        let Some(document) = document else { continue };
        let tokens: Vec<String> = document
            .tokens()
            .iter()
            .map(|t| vocab[t].to_owned())
            .collect();
        let len = live.point_to_doc_len.as_ref().unwrap()[idx];
        builder.add(idx as PointOffsetType, tokens, Some(len));
    }
    let rebuilt = builder.build();

    for (term, &token_id) in &live.vocab {
        let rebuilt_id = rebuilt.vocab[term];
        let (live_posting, rebuilt_posting) = (
            &live.postings[token_id as usize],
            &rebuilt.postings[rebuilt_id as usize],
        );
        assert_eq!(
            (
                live_posting.iter().collect::<Vec<_>>(),
                live_posting.frequencies()
            ),
            (
                rebuilt_posting.iter().collect::<Vec<_>>(),
                rebuilt_posting.frequencies()
            ),
            "postings of {term} differ",
        );
    }
}

/// Parameters outside the domain the MaxScore bound holds in are refused
/// rather than pruned wrongly.
#[test]
fn parameters_outside_the_bound_domain_are_rejected() {
    let term = [Bm25Term {
        token_id: 0,
        idf: 1.0,
    }];
    for params in [
        Bm25Params { k1: 1.2, b: 2.0 },
        Bm25Params { k1: 1.2, b: -0.1 },
        Bm25Params { k1: -1.0, b: 0.75 },
        Bm25Params {
            k1: f32::NAN,
            b: 0.75,
        },
        Bm25Params {
            k1: f32::INFINITY,
            b: 0.75,
        },
    ] {
        assert!(
            Bm25Query::new(term, params, None).is_err(),
            "{params:?} must be rejected"
        );
    }
    assert!(Bm25Query::new(term, Bm25Params::default(), Some(0.0)).is_err());
    assert!(Bm25Query::new(term, Bm25Params::default(), Some(f32::NAN)).is_err());
    for idf in [-1.0, f32::NAN, f32::INFINITY] {
        let term = [Bm25Term { token_id: 0, idf }];
        assert!(
            Bm25Query::new(term, Bm25Params::default(), None).is_err(),
            "idf {idf} must be rejected"
        );
    }
    // The edges of the domain are inside it.
    assert!(Bm25Query::new(term, Bm25Params { k1: 0.0, b: 0.0 }, None).is_ok());
    assert!(Bm25Query::new(term, Bm25Params { k1: 1.2, b: 1.0 }, Some(1.0)).is_ok());
}

/// Cursors over `immutable`'s postings with the frequencies from its frequency
/// file, for calling the scorer directly.
fn frequency_cursors<'a>(
    immutable: &'a ImmutableInvertedIndex,
    query: &Bm25Query,
) -> impl TermCursors + 'a {
    let ImmutablePostings::WithPositions(postings) = &immutable.postings else {
        panic!("the fixture stores positions");
    };
    let frequencies = TermFrequencies::new(immutable.term_frequencies.as_deref().unwrap()).unwrap();
    let views = query
        .terms()
        .iter()
        .map(|term| {
            let posting = postings.get(term.token_id as usize)?;
            Some((posting.view(), frequencies.term(term.token_id).unwrap()))
        })
        .collect();
    FrequencyCursors::new(views)
}

/// A filter that only answers in batches, recording their sizes.
struct BatchOnlyFilter(std::cell::RefCell<Vec<usize>>);

impl ConditionChecker for BatchOnlyFilter {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        panic!("point {point_id} checked on its own, not in a batch");
    }

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        self.0.borrow_mut().push(ids.len());
        default_check_batched(ids, select, rest, |id| Ok(id % 5 != 2))
    }
}

/// The dense pass checks the filter in batches of up to the block size, never
/// one point at a time, so a filter reading storage issues its reads together.
#[test]
fn dense_checks_the_filter_in_batches() {
    let mutable = fixture(41, 500, &[1, 10, 100]);
    let immutable = ImmutableInvertedIndex::from(mutable.clone());
    let lengths = mutable.point_to_doc_len.as_deref().unwrap();
    let filter = BatchOnlyFilter(Default::default());
    for terms in queries() {
        let query = query(&mutable, &terms, Bm25Params::default());
        score_top_k_with::<_, 7>(
            &query,
            &mut frequency_cursors(&immutable, &query),
            lengths.len(),
            |point_ids, out| {
                for (point_id, doc_len) in point_ids.iter().zip(out) {
                    *doc_len = lengths.get(*point_id as usize).copied();
                }
                Ok(())
            },
            &filter,
            1000,
            &AtomicBool::new(false),
            Strategy {
                heap_min_essential: HEAP_MIN_ESSENTIAL,
                dense_min_postings_per_doc: 0.0,
                dense_window: usize::MAX,
            },
        )
        .unwrap();
    }
    let batches = filter.0.borrow();
    assert!(batches.iter().all(|&len| len <= 7), "{batches:?}");
    assert!(batches.iter().any(|&len| len > 1), "{batches:?}");
}

/// Term at a time over a dense array ranks as document at a time does, on
/// the shapes that allow it: the same top `limit` within float rounding, and
/// bit for bit when nothing is pruned, since a document's terms are then summed
/// in the same order. Lengths are read once per document, in batches no larger
/// than the block.
#[rstest]
fn dense_matches_document_at_a_time(
    #[values(1, 3, 10, 1000)] limit: usize,
    #[values(1, 7, 128, 300, usize::MAX)] window: usize,
) {
    let mutable = fixture(41, 500, &[1, 10, 100]);
    let immutable = ImmutableInvertedIndex::from(mutable.clone());
    let dir = tempfile::tempdir().unwrap();
    let on_disk = on_disk(dir.path(), &mutable, &BitVec::repeat(false, 500));
    let lengths = mutable.point_to_doc_len.as_deref().unwrap();
    let accept = Bm25Accept::new(&|idx| idx % 5 != 2, None);
    let rank = |query: &Bm25Query, dense: f32| {
        let mut cursors = frequency_cursors(&immutable, query);
        let mut asked: Vec<PointOffsetType> = Vec::new();
        let ranking = score_top_k_with::<_, 7>(
            query,
            &mut cursors,
            lengths.len(),
            |point_ids, out| {
                assert!(point_ids.len() <= 7);
                asked.extend_from_slice(point_ids);
                for (point_id, doc_len) in point_ids.iter().zip(out) {
                    *doc_len = lengths.get(*point_id as usize).copied();
                }
                Ok(())
            },
            &accept,
            limit,
            &AtomicBool::new(false),
            Strategy {
                heap_min_essential: HEAP_MIN_ESSENTIAL,
                dense_min_postings_per_doc: dense,
                dense_window: window,
            },
        )
        .unwrap();
        let read = asked.len();
        asked.sort_unstable();
        asked.dedup();
        assert_eq!(asked.len(), read, "a length was read twice");
        ranking
    };
    for terms in queries() {
        let query = query(&mutable, &terms, Bm25Params::default());
        let expected = reference(&mutable, &query, |idx| idx % 5 != 2);
        let dense = rank(&query, 0.0);
        let by_document = rank(&query, f32::INFINITY);
        eprintln!("{terms:?} limit {limit}");
        assert_top_k(&dense, &expected, limit);
        assert_top_k(&by_document, &expected, limit);
        if limit >= 500 {
            let bits = |ranking: &[ScoredPointOffset]| {
                ranking
                    .iter()
                    .map(|hit| (hit.idx, hit.score.to_bits()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(&dense), bits(&by_document), "{terms:?}");
        }
        // Through each index's own entry point, whichever path it takes.
        assert_top_k(
            &run(&immutable, &query, |idx| idx % 5 != 2, limit),
            &expected,
            limit,
        );
        assert_top_k(
            &run(&on_disk, &query, |idx| idx % 5 != 2, limit),
            &expected,
            limit,
        );
    }
    // Documents hit only by zero-idf terms score zero, and are still hits: the
    // dense pass tells them apart by the sign bit, which a `-0.0` idf must not
    // leave set.
    let mut vocab: Vec<TokenId> = mutable.vocab.values().copied().collect();
    vocab.sort_unstable();
    for idf in [0.0, -0.0] {
        let terms = vocab
            .iter()
            .take(3)
            .map(|&token_id| Bm25Term { token_id, idf });
        let query = Bm25Query::new(terms, Bm25Params::default(), None).unwrap();
        let dense = rank(&query, 0.0);
        let by_document = rank(&query, f32::INFINITY);
        assert!(!dense.is_empty());
        let bits = |ranking: &[ScoredPointOffset]| {
            ranking
                .iter()
                .map(|hit| (hit.idx, hit.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(&dense), bits(&by_document), "idf {idf:?}");
    }
}
