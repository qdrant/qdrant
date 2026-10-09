use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::counter::hw;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use common::universal_io::{MmapFile, MmapFs, Populate};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;

use super::super::InvertedIndex;
use super::super::immutable_inverted_index::ImmutableInvertedIndex;
use super::super::mutable_inverted_index::MutableInvertedIndex;
use super::super::on_disk_inverted_index::OnDiskInvertedIndex;
use super::*;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::query_context::fancy_idf;
use crate::index::condition_checker::{ConditionCheckerEnum, TestBitOfId};
use crate::index::query_optimization::optimized_filter::OptimizedFilter;

const VOCAB: usize = 40;

/// A skewed vocabulary: low ranks are common, high ranks rare, so queries
/// mix terms with very different bounds and MaxScore has something to prune.
fn word(rng: &mut StdRng) -> String {
    let rank = (rng.random::<f64>().powi(3) * VOCAB as f64) as usize;
    format!("w{}", rank.min(VOCAB - 1))
}

fn fixture(seed: u64, documents: u32, deleted: &[PointOffsetType]) -> MutableInvertedIndex {
    let mut rng = StdRng::seed_from_u64(seed);
    let _hw = hw::test_guard();
    let mut index = MutableInvertedIndex::new(true, true);
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
    immutable: &ImmutableInvertedIndex,
    deleted: &BitVec,
) -> OnDiskInvertedIndex<MmapFile> {
    OnDiskInvertedIndex::create(dir.to_path_buf(), immutable).unwrap();
    OnDiskInvertedIndex::<MmapFile>::open(&MmapFs, dir.to_path_buf(), Populate::No, true, deleted)
        .unwrap()
        .unwrap()
}

fn run<I: InvertedIndex>(
    index: &I,
    query: &Bm25Query,
    accept: impl Fn(PointOffsetType) -> bool,
    limit: usize,
) -> Vec<ScoredPointOffset> {
    hw::test(|| {
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
    ]
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
    let on_disk = on_disk(dir.path(), &immutable, &after_mask);
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
    let documents = index.point_to_doc.as_deref().unwrap();
    let lengths = index.point_to_doc_len.as_deref().unwrap();
    for terms in queries() {
        let query = query(&index, &terms, Bm25Params::default());
        let postings = query
            .terms()
            .iter()
            .map(|term| index.postings.get(term.token_id as usize))
            .collect();
        let mut cursors = MutableCursors::new(postings, documents, query.terms());
        let mut asked: Vec<PointOffsetType> = Vec::new();
        let actual = score_top_k::<_, BLOCK>(
            &query,
            &mut cursors,
            |point_ids, out| {
                assert!(point_ids.len() <= BLOCK);
                asked.extend_from_slice(point_ids);
                for (point_id, doc_len) in point_ids.iter().zip(out) {
                    *doc_len = Some(lengths[*point_id as usize]);
                }
                Ok(())
            },
            |_| true,
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
    let on_disk = on_disk(dir.path(), &immutable, &BitVec::new());
    let odd = OptimizedFilter::from_checker(ConditionCheckerEnum::TestBitOfId(TestBitOfId(0)));
    let accept = Bm25Accept::new(&|_| true, Some(&odd));
    let is_odd = |idx: PointOffsetType| idx % 2 == 1;
    let is_stopped = AtomicBool::new(false);
    let _hw = hw::test_guard();

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
    let documents = index.point_to_doc.as_deref().unwrap();
    let lengths = index.point_to_doc_len.as_deref().unwrap();
    let postings = query
        .terms()
        .iter()
        .map(|term| index.postings.get(term.token_id as usize))
        .collect();
    let mut cursors = MutableCursors::new(postings, documents, query.terms());
    let actual = score_top_k::<_, BLOCK>(
        query,
        &mut cursors,
        |point_ids, out| {
            for (point_id, doc_len) in point_ids.iter().zip(out) {
                *doc_len = Some(lengths[*point_id as usize]);
            }
            Ok(())
        },
        |_| true,
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
    let on_disk = on_disk(dir.path(), &immutable, &BitVec::new());
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
    let on_disk = on_disk(dir.path(), &immutable, &BitVec::new());
    let query = query(&mutable, &["w0", "w1"], Bm25Params::default());

    let filter =
        OptimizedFilter::from_checker(ConditionCheckerEnum::Dyn(Box::new(FailingCondition)));
    let accept = Bm25Accept::new(&|_| true, Some(&filter));
    let is_stopped = AtomicBool::new(false);
    let _hw = hw::test_guard();
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
    let _hw = hw::test_guard();
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

#[test]
fn positions_are_required() {
    let _hw = hw::test_guard();
    let is_stopped = AtomicBool::new(false);
    let mut without_positions = MutableInvertedIndex::new(false, true);
    without_positions
        .index_str_tokens(0, ["alpha", "beta"], Some(2))
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
        without_positions
            .score_bm25(&query, &Bm25Accept::new(&|_| true, None), 10, &is_stopped)
            .is_err()
    );
    let immutable = ImmutableInvertedIndex::from(without_positions);
    assert!(
        immutable
            .score_bm25(&query, &Bm25Accept::new(&|_| true, None), 10, &is_stopped)
            .is_err()
    );
}

/// A query that normalizes by length is refused by an index that records
/// no lengths, rather than silently scored as `b = 0`.
#[test]
fn length_normalization_requires_lengths() {
    let _hw = hw::test_guard();
    let is_stopped = AtomicBool::new(false);
    let mut without_lengths = MutableInvertedIndex::new(true, false);
    without_lengths
        .index_str_tokens(0, ["alpha", "beta"], None)
        .unwrap();
    let term = [Bm25Term {
        token_id: 0,
        idf: 1.0,
    }];
    let normalized = Bm25Query::new(term, Bm25Params::default(), Some(2.0)).unwrap();
    assert!(
        without_lengths
            .score_bm25(
                &normalized,
                &Bm25Accept::new(&|_| true, None),
                10,
                &is_stopped
            )
            .is_err()
    );
    let unnormalized = Bm25Query::new(term, Bm25Params::default(), None).unwrap();
    assert_eq!(
        without_lengths
            .score_bm25(
                &unnormalized,
                &Bm25Accept::new(&|_| true, None),
                10,
                &is_stopped
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn empty_query_and_zero_limit_return_nothing() {
    let _hw = hw::test_guard();
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
    let _hw = hw::test_guard();
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
    let _hw = hw::test_guard();
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
