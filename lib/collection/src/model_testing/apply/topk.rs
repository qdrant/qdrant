//! Oracle for exact nearest queries: the model scores every candidate itself and checks that the
//! engine returned the best ones, with the scores it computed.
//!
//! The membership checks of `Search` and `Query` accept any `limit` candidates, so a stale copy of
//! a vector, a wrong distance, or a storage conversion that changes values goes unnoticed as long
//! as the point still exists. Here those change the ranking or the score.
//!
//! Left out: TurboQuant storage, where the engine scores the query against the codes and the
//! model only has the dequantized read-back, and sparse vectors, whose exact search has its own
//! candidate set.

use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::{AHashMap, AHashSet};
use segment::types::{Distance, PointIdType};

use super::super::op::model_vector;
use super::super::{Model, VectorKind, VectorValue, candidate_of};
use crate::operations::types::Datatype;

/// How many exact searches were checked, so a run can show the oracle was not idle. Process-wide:
/// harness tests sharing a process add up, which is fine for a number that only has to be nonzero.
static CHECKS: AtomicUsize = AtomicUsize::new(0);

pub(in super::super) fn checks() -> usize {
    CHECKS.load(Ordering::Relaxed)
}

/// Relative slack on a score. The engine sums in f32 and in SIMD order, the model in f64.
const TOLERANCE: f64 = 1e-4;

pub(super) fn supported(vector_name: &str) -> bool {
    let candidate = candidate_of(vector_name);
    let datatype = match candidate.datatype {
        None | Some(Datatype::Float32 | Datatype::Float16 | Datatype::Uint8) => true,
        Some(Datatype::Turbo4 | Datatype::Turbo8 | Datatype::Turbo16) => false,
    };
    let kind = match candidate.kind {
        VectorKind::Dense(_) | VectorKind::MultiDense(_) => true,
        VectorKind::Sparse => false,
    };
    datatype && kind
}

fn larger_is_better(distance: Distance) -> bool {
    match distance {
        Distance::Dot | Distance::Cosine => true,
        Distance::Euclid | Distance::Manhattan => false,
    }
}

/// The score the API reports for two stored vectors. Cosine is a dot product: both sides are
/// already normalized, the stored one at ingestion and the query by `model_vector`.
fn pair_score(distance: Distance, a: &[f32], b: &[f32]) -> f64 {
    let pairs = a.iter().zip(b).map(|(a, b)| (f64::from(*a), f64::from(*b)));
    match distance {
        Distance::Dot | Distance::Cosine => pairs.map(|(a, b)| a * b).sum(),
        Distance::Euclid => pairs.map(|(a, b)| (a - b) * (a - b)).sum::<f64>().sqrt(),
        Distance::Manhattan => pairs.map(|(a, b)| (a - b).abs()).sum(),
    }
}

fn score(distance: Distance, query: &VectorValue, stored: &VectorValue) -> f64 {
    match (query, stored) {
        (VectorValue::Dense(query), VectorValue::Dense(stored)) => {
            pair_score(distance, query, stored)
        }
        // MaxSim: each query row takes its best stored row, and the rows add up.
        (VectorValue::MultiDense(query), VectorValue::MultiDense(stored)) => query
            .iter()
            .map(|query_row| {
                stored
                    .iter()
                    .map(|stored_row| pair_score(distance, query_row, stored_row))
                    .fold(f64::NEG_INFINITY, f64::max)
            })
            .sum(),
        (query, stored) => panic!("top-k oracle: query {query:?} against stored {stored:?}"),
    }
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= TOLERANCE * f64::max(1.0, f64::max(a.abs(), b.abs()))
}

/// `results` is what an exact nearest query over `candidates` returned, in the engine's order.
pub(super) fn check(
    model: &Model,
    vector_name: &str,
    query: &VectorValue,
    candidates: &AHashSet<PointIdType>,
    limit: usize,
    results: &[(PointIdType, f32)],
    ctx: &str,
) {
    CHECKS.fetch_add(1, Ordering::Relaxed);
    let distance = candidate_of(vector_name).distance;
    let larger = larger_is_better(distance);
    // The query goes through the same preprocessing and storage conversion as a stored vector.
    let query = model_vector(vector_name, query);
    // `a` at least as good as `b`, within tolerance.
    let not_worse = |a: f64, b: f64| close(a, b) || if larger { a > b } else { a < b };

    let model_scores: AHashMap<PointIdType, f64> = candidates
        .iter()
        .map(|id| {
            (
                *id,
                score(distance, &query, &model[id].vectors[vector_name]),
            )
        })
        .collect();
    let mut ranked: Vec<f64> = model_scores.values().copied().collect();
    ranked.sort_by(|a, b| {
        if larger {
            b.total_cmp(a)
        } else {
            a.total_cmp(b)
        }
    });
    let k = limit.min(ranked.len());
    if k == 0 {
        return;
    }
    let kth = ranked[k - 1];

    let returned: AHashSet<PointIdType> = results.iter().map(|(id, _)| *id).collect();
    for (id, engine_score) in results {
        let Some(model_score) = model_scores.get(id).copied() else {
            panic!("{ctx}: returned {id:?}, which is not a candidate");
        };
        assert!(
            close(f64::from(*engine_score), model_score),
            "{ctx}: score of {id:?} is {engine_score}, the model computes {model_score} \
             ({distance:?}, query {query:?}, stored {:?})",
            model[id].vectors[vector_name],
        );
        assert!(
            not_worse(model_score, kth),
            "{ctx}: returned {id:?} with score {model_score}, outside the top {k} \
             (the {k}th best score is {kth})",
        );
    }
    for pair in results.windows(2) {
        let (first, second) = (f64::from(pair[0].1), f64::from(pair[1].1));
        assert!(
            not_worse(first, second),
            "{ctx}: results out of order, {:?} ({first}) before {:?} ({second})",
            pair[0].0,
            pair[1].0,
        );
    }
    // Anything clearly better than the k-th score has to be in the result.
    let mut skipped: Vec<(PointIdType, f64)> = model_scores
        .iter()
        .filter(|(id, score)| !returned.contains(*id) && !not_worse(kth, **score))
        .map(|(id, score)| (*id, *score))
        .collect();
    skipped.sort_by_key(|(id, _)| *id);
    assert!(
        skipped.is_empty(),
        "{ctx}: top {k} misses better candidates {skipped:?} (the {k}th best score is {kth}); \
         returned {results:?}",
    );
}
