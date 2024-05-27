use common::types::ScoreType;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType};

use crate::collection_manager::segments_searcher::SegmentsSearcher;

fn score_point(id: usize, score: ScoreType, version: SeqNumberType) -> ScoredPoint {
    ScoredPoint {
        id: PointIdType::NumId(id as u64),
        version,
        score,
        payload: None,
        vector: None,
        shard_key: None,
        order_value: None,
    }
}

fn search_result_b0_s0() -> Vec<ScoredPoint> {
    vec![
        score_point(11, 0.91, 11),
        score_point(12, 0.81, 23),
        score_point(13, 0.71, 12),
        score_point(14, 0.61, 12),
        score_point(15, 0.51, 12),
        score_point(16, 0.41, 31), // Have points, but all bad
    ]
}

fn search_result_b0_s1() -> Vec<ScoredPoint> {
    vec![
        score_point(21, 0.92, 42),
        score_point(22, 0.82, 12),
        score_point(23, 0.74, 32),
        score_point(24, 0.73, 21),
        score_point(25, 0.72, 55),
        score_point(26, 0.71, 10), // may contain more interesting points
    ]
}

fn search_result_b0_s2() -> Vec<ScoredPoint> {
    vec![
        score_point(31, 0.93, 52),
        score_point(32, 0.83, 22),
        score_point(33, 0.73, 21), // less point than expected
    ]
}

fn search_result_b1_s0() -> Vec<ScoredPoint> {
    vec![
        score_point(111, 0.92, 11),
        score_point(112, 0.81, 23),
        score_point(113, 0.71, 12),
    ]
}

fn search_result_b1_s1() -> Vec<ScoredPoint> {
    vec![
        score_point(111, 0.91, 14),
        score_point(112, 0.81, 23),
        score_point(113, 0.71, 12),
    ]
}

fn search_result_b1_s2() -> Vec<ScoredPoint> {
    vec![
        score_point(111, 0.91, 11),
        score_point(112, 0.81, 23),
        score_point(113, 0.71, 12),
    ]
}

#[test]
fn test_aggregation_of_batch_search_results() {
    let search_results = vec![
        vec![search_result_b0_s0(), search_result_b1_s0()],
        vec![search_result_b0_s1(), search_result_b1_s1()],
        vec![search_result_b0_s2(), search_result_b1_s2()],
    ];

    let result_limits = vec![12, 5];

    let further_results = vec![vec![true, true], vec![true, true], vec![false, true]];

    let (aggregator, re_request) = SegmentsSearcher::process_search_result_step1(
        search_results,
        result_limits,
        further_results,
    );

    // ------------Segment----------batch---
    assert!(re_request[&1].contains(&0));

    assert!(re_request[&0].contains(&1)); // re-request all segments
    assert!(re_request[&1].contains(&1));
    assert!(re_request[&2].contains(&1));

    let top_results = aggregator.into_topk();

    eprintln!("top_results = {top_results:#?}");

    assert_eq!(top_results.len(), 2);
    assert_eq!(top_results[0].len(), 12);
    assert_eq!(top_results[1].len(), 3);

    assert_eq!(top_results[0][0].id, PointIdType::NumId(31));
    assert_eq!(top_results[0][1].id, PointIdType::NumId(21));
    assert_eq!(top_results[0][2].id, PointIdType::NumId(11));

    assert_eq!(top_results[1][0].id, PointIdType::NumId(111));
    assert_eq!(top_results[1][0].version, 14);
    assert_eq!(top_results[1][0].score, 0.91);

    assert_eq!(top_results[1][1].id, PointIdType::NumId(112));
    assert_eq!(top_results[1][1].version, 23);
    assert_eq!(top_results[1][1].score, 0.81);

    assert_eq!(top_results[1][2].id, PointIdType::NumId(113));
    assert_eq!(top_results[1][2].version, 12);
    assert_eq!(top_results[1][2].score, 0.71);
}
