//! Retrieval quality of the `sefr` relevance feedback strategy, measured
//! against the `naive` strategy and against searching the original query alone.
//!
//! Run with `cargo test -p segment --test sefr_accuracy -- --nocapture` to see
//! the measurement tables.
//!
//! The scenario is an ambiguous query: the relevant documents and a set of
//! confusable ones are equally similar to the original query, so the query on
//! its own cannot separate them and the feedback has to do the work.

use std::fmt::Write as _;

use common::types::ScoreType;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::StandardNormal;
use segment::data_types::vectors::{DenseVector, VectorElementType};
use segment::vector_storage::query::{
    FeedbackItem, NaiveFeedbackCoefficients, NaiveFeedbackQuery, Query, fit_sefr,
};

const DIM: usize = 64;
const RELEVANT: usize = 80;
const CONFUSABLE: usize = 80;
const BACKGROUND: usize = 840;
const TRIALS: usize = 30;
const FEEDBACK_SIZES: [usize; 7] = [2, 4, 8, 16, 32, 64, 128];
/// Blend weights tried for the `target_weight` parameter.
const TARGET_WEIGHTS: [f32; 6] = [0.0, 0.01, 0.1, 1.0, 10.0, 100.0];

fn dot(a: &[VectorElementType], b: &[VectorElementType]) -> ScoreType {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// The naive strategy's score is
/// `a * sim(target) + sum_pairs confidence^b * c * (sim(positive) - sim(negative))`.
///
/// Every term is a similarity against a fixed vector, so with a dot product the
/// whole formula is linear in the candidate and collapses to one vector, exactly
/// like SEFR does. [`naive_score_is_linear_in_the_candidate`] pins this down;
/// the evaluation below relies on it to score candidates cheaply.
fn naive_weight_vector(
    target: &[VectorElementType],
    feedback: &[FeedbackItem<DenseVector>],
    coefficients: &NaiveFeedbackCoefficients,
) -> DenseVector {
    let mut weights: DenseVector = target.iter().map(|x| coefficients.a.0 * x).collect();

    // Mirrors `extract_context_pairs`: every ordered pair with a positive
    // difference in feedback score, at margin zero.
    for positive in feedback {
        for negative in feedback {
            let confidence = positive.score.0 - negative.score.0;
            if confidence <= 0.0 {
                continue;
            }
            let pair_weight = confidence.powf(coefficients.b.0) * coefficients.c.0;
            for (weight, (p, n)) in weights
                .iter_mut()
                .zip(positive.vector.iter().zip(negative.vector.iter()))
            {
                *weight += pair_weight * (p - n);
            }
        }
    }

    weights
}

fn sefr_weight_vector(feedback: &[FeedbackItem<DenseVector>]) -> DenseVector {
    let threshold =
        feedback.iter().map(|item| item.score.0).sum::<ScoreType>() / feedback.len() as ScoreType;

    let positives: Vec<&[VectorElementType]> = feedback
        .iter()
        .filter(|item| item.score.0 > threshold)
        .map(|item| item.vector.as_slice())
        .collect();
    let negatives: Vec<&[VectorElementType]> = feedback
        .iter()
        .filter(|item| item.score.0 <= threshold)
        .map(|item| item.vector.as_slice())
        .collect();

    fit_sefr(&positives, &negatives).unwrap().weights
}

/// An ambiguous query over an embedding space that may have uneven
/// per-dimension scales.
struct Scenario {
    target: DenseVector,
    /// Documents the user wants.
    relevant: Vec<DenseVector>,
    /// Documents equally close to the query, but unwanted.
    confusable: Vec<DenseVector>,
    background: Vec<DenseVector>,
}

fn gaussian(rng: &mut StdRng) -> DenseVector {
    (0..DIM)
        .map(|_| rng.sample::<f64, _>(StandardNormal) as VectorElementType)
        .collect()
}

fn normalized(mut vector: DenseVector) -> DenseVector {
    let norm = vector.iter().map(|x| x * x).sum::<ScoreType>().sqrt();
    for value in &mut vector {
        *value /= norm;
    }
    vector
}

/// Remove the component of `vector` along the unit vector `basis`.
fn orthogonalize(mut vector: DenseVector, basis: &[VectorElementType]) -> DenseVector {
    let projection = dot(&vector, basis);
    for (value, b) in vector.iter_mut().zip(basis) {
        *value -= projection * b;
    }
    vector
}

fn build_scenario(rng: &mut StdRng, anisotropic: bool) -> Scenario {
    // `shared` is what the query captures; `wanted` and `unwanted` are the two
    // senses it cannot distinguish between. All three are mutually orthogonal.
    let shared = normalized(gaussian(rng));
    let wanted = normalized(orthogonalize(gaussian(rng), &shared));
    let unwanted = normalized(orthogonalize(
        orthogonalize(gaussian(rng), &shared),
        &wanted,
    ));

    // Real embeddings rarely have uniform per-dimension scale. Spanning two
    // orders of magnitude is the interesting case for a per-dimension method.
    let scale: DenseVector = (0..DIM)
        .map(|_| {
            if anisotropic {
                10f32.powf(rng.random_range(-1.0..1.0))
            } else {
                1.0
            }
        })
        .collect();

    let apply_scale = |vector: DenseVector| -> DenseVector {
        vector
            .into_iter()
            .zip(&scale)
            .map(|(value, s)| value * s)
            .collect()
    };

    let in_sense = |sense: &[VectorElementType], rng: &mut StdRng| -> DenseVector {
        let noise = gaussian(rng);
        let raw: DenseVector = (0..DIM)
            .map(|i| shared[i] + sense[i] + 0.15 * noise[i])
            .collect();
        apply_scale(raw)
    };

    let relevant = (0..RELEVANT)
        .map(|_| in_sense(&wanted, rng))
        .collect::<Vec<_>>();
    let confusable = (0..CONFUSABLE)
        .map(|_| in_sense(&unwanted, rng))
        .collect::<Vec<_>>();
    // Unrelated documents, at the same norm as the two clusters.
    let background = (0..BACKGROUND)
        .map(|_| {
            apply_scale(
                normalized(gaussian(rng))
                    .iter()
                    .map(|x| x * 2f32.sqrt())
                    .collect(),
            )
        })
        .collect::<Vec<_>>();

    Scenario {
        target: apply_scale(shared),
        relevant,
        confusable,
        background,
    }
}

#[derive(Default, Clone, Copy)]
struct Metrics {
    precision_at_10: f64,
    recall_at_50: f64,
    average_precision: f64,
}

/// Rank the candidates by `score` and measure how well the relevant ones did.
fn evaluate(
    weights: &[VectorElementType],
    relevant: &[DenseVector],
    irrelevant: &[DenseVector],
) -> Metrics {
    let mut scored: Vec<(ScoreType, bool)> = relevant
        .iter()
        .map(|vector| (dot(weights, vector), true))
        .chain(
            irrelevant
                .iter()
                .map(|vector| (dot(weights, vector), false)),
        )
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));

    let hits_at = |k: usize| scored.iter().take(k).filter(|(_, rel)| *rel).count();

    let mut found: usize = 0;
    let mut precision_sum = 0.0;
    for (rank, (_, is_relevant)) in scored.iter().enumerate() {
        if *is_relevant {
            found += 1;
            precision_sum += found as f64 / (rank + 1) as f64;
        }
    }

    Metrics {
        precision_at_10: hits_at(10) as f64 / 10.0,
        recall_at_50: hits_at(50) as f64 / relevant.len() as f64,
        average_precision: precision_sum / relevant.len() as f64,
    }
}

/// A metric's display name paired with a reader for it, so the report can print
/// the same set of columns once per metric.
type MetricAccessor = (&'static str, fn(&Metrics) -> f64);

fn mean(values: &[Metrics]) -> Metrics {
    let n = values.len() as f64;
    Metrics {
        precision_at_10: values.iter().map(|m| m.precision_at_10).sum::<f64>() / n,
        recall_at_50: values.iter().map(|m| m.recall_at_50).sum::<f64>() / n,
        average_precision: values.iter().map(|m| m.average_precision).sum::<f64>() / n,
    }
}

/// Coefficient grid for the naive strategy. Its parameters are meant to be
/// trained per dataset, so the evaluation reports its best grid point, which is
/// the most generous reading available.
///
/// The `a` coefficient scales the original query, so it is what decides whether
/// the strategy uses the query at all. Setting `with_query` mirrors the split
/// between the `sefr` and `sefr + query` columns: the feedback direction on its
/// own, against the feedback direction mixed with the query.
///
/// The pair term sums over roughly `n^2` pairs, so `c` has to reach far enough
/// down for the query term to still be able to dominate at the largest feedback
/// sizes. Without that the grid would handicap the strategy.
fn naive_grid(with_query: bool) -> Vec<NaiveFeedbackCoefficients> {
    let mut grid = Vec::new();
    for b in [0.0, 1.0, 2.0] {
        for c in [1e-5, 1e-4, 1e-3, 1e-2, 0.1, 1.0, 10.0] {
            grid.push(NaiveFeedbackCoefficients {
                a: OrderedFloat(if with_query { 1.0 } else { 0.0 }),
                b: OrderedFloat(b),
                c: OrderedFloat(c),
            });
        }
    }
    grid
}

/// Best grid point for the naive strategy on this trial, by average precision.
fn best_naive(
    with_query: bool,
    target: &[VectorElementType],
    feedback: &[FeedbackItem<DenseVector>],
    relevant: &[DenseVector],
    irrelevant: &[DenseVector],
) -> Metrics {
    naive_grid(with_query)
        .into_iter()
        .map(|coefficients| {
            let weights = naive_weight_vector(target, feedback, &coefficients);
            evaluate(&weights, relevant, irrelevant)
        })
        .max_by(|a, b| a.average_precision.total_cmp(&b.average_precision))
        .unwrap()
}

/// Split each cluster into the part the user gave feedback on and the part left
/// to be retrieved. Feedback items are excluded from scoring, as the query API
/// excludes referenced ids.
fn split_feedback(
    scenario: &Scenario,
    per_class: usize,
    rng: &mut StdRng,
) -> (
    Vec<FeedbackItem<DenseVector>>,
    Vec<DenseVector>,
    Vec<DenseVector>,
) {
    let mut feedback = Vec::with_capacity(per_class * 2);

    // Graded rather than binary scores, so the `confidence` term of the naive
    // formula (and thus its `b` coefficient) actually varies.
    for vector in scenario.relevant.iter().take(per_class) {
        feedback.push(FeedbackItem {
            vector: vector.clone(),
            score: OrderedFloat(rng.random_range(0.7..1.0)),
        });
    }
    for vector in scenario.confusable.iter().take(per_class) {
        feedback.push(FeedbackItem {
            vector: vector.clone(),
            score: OrderedFloat(rng.random_range(0.0..0.3)),
        });
    }

    let held_out_relevant = scenario.relevant[per_class..].to_vec();
    let irrelevant = scenario.confusable[per_class..]
        .iter()
        .chain(scenario.background.iter())
        .cloned()
        .collect();

    (feedback, held_out_relevant, irrelevant)
}

/// The naive formula is linear in the candidate, so folding it into a single
/// vector must reproduce `score_by` exactly. This both documents the property
/// and licenses the folded scoring used by the evaluation.
#[test]
fn naive_score_is_linear_in_the_candidate() {
    let mut rng = StdRng::seed_from_u64(7);

    let grid = naive_grid(false).into_iter().chain(naive_grid(true));
    for coefficients in grid {
        let target = gaussian(&mut rng);
        let feedback: Vec<FeedbackItem<DenseVector>> = (0..6)
            .map(|_| FeedbackItem {
                vector: gaussian(&mut rng),
                score: OrderedFloat(rng.random_range(0.0..1.0)),
            })
            .collect();

        let query = NaiveFeedbackQuery {
            target: target.clone(),
            feedback: feedback.clone(),
            coefficients,
        }
        .into_query();
        let folded = naive_weight_vector(&target, &feedback, &coefficients);

        for _ in 0..8 {
            let candidate = gaussian(&mut rng);
            let walked = query.score_by(|example: &DenseVector| dot(example, &candidate));
            let single_dot = dot(&folded, &candidate);
            assert!(
                (walked - single_dot).abs() <= 1e-3 * walked.abs().max(1.0),
                "walking the pairs gave {walked}, the folded vector gave {single_dot}",
            );
        }
    }
}

/// Blend the SEFR direction with the original query, matching what
/// `SefrParams::target_weight` does.
fn blended(
    sefr_weights: &[VectorElementType],
    target: &[VectorElementType],
    target_weight: f32,
) -> DenseVector {
    let norm = |v: &[VectorElementType]| v.iter().map(|x| x * x).sum::<ScoreType>().sqrt();
    let scale = target_weight * norm(sefr_weights) / norm(target);

    sefr_weights
        .iter()
        .zip(target)
        .map(|(w, t)| w + scale * t)
        .collect()
}

#[test]
fn sefr_accuracy_against_naive_and_the_bare_query() {
    let mut report = String::new();

    for anisotropic in [false, true] {
        let space = if anisotropic {
            "uneven per-dimension scale (10^-1 to 10^1)"
        } else {
            "uniform per-dimension scale"
        };
        writeln!(report, "\n== {space} ==").unwrap();
        writeln!(
            report,
            "{:>8} | {:>7} | {:>7}{:>9} | {:>7}{:>9} | metric",
            "feedback", "query", "naive", "naive+q", "sefr", "sefr+q",
        )
        .unwrap();

        for per_class in FEEDBACK_SIZES.map(|size| size / 2) {
            let mut query_only = Vec::new();
            let mut naive_plain = Vec::new();
            let mut naive_blend = Vec::new();
            let mut sefr_plain = Vec::new();
            let mut sefr_blend = Vec::new();

            for trial in 0..TRIALS {
                let mut rng = StdRng::seed_from_u64(trial as u64);
                let scenario = build_scenario(&mut rng, anisotropic);
                let (feedback, relevant, irrelevant) =
                    split_feedback(&scenario, per_class, &mut rng);
                let target = &scenario.target;

                query_only.push(evaluate(target, &relevant, &irrelevant));

                // Both naive columns get their best coefficients on this trial.
                // `a = 0` drops the query, `a = 1` keeps it.
                naive_plain.push(best_naive(false, target, &feedback, &relevant, &irrelevant));
                naive_blend.push(best_naive(true, target, &feedback, &relevant, &irrelevant));

                let weights = sefr_weight_vector(&feedback);
                sefr_plain.push(evaluate(&weights, &relevant, &irrelevant));

                // And SEFR gets its best blend with the query, symmetrically.
                sefr_blend.push(
                    TARGET_WEIGHTS
                        .into_iter()
                        .map(|target_weight| {
                            let blend = blended(&weights, target, target_weight);
                            evaluate(&blend, &relevant, &irrelevant)
                        })
                        .max_by(|a, b| a.average_precision.total_cmp(&b.average_precision))
                        .unwrap(),
                );
            }

            let query_only = mean(&query_only);
            let naive_plain = mean(&naive_plain);
            let naive_blend = mean(&naive_blend);
            let sefr_plain = mean(&sefr_plain);
            let sefr_blend = mean(&sefr_blend);

            let metrics: [MetricAccessor; 3] = [
                ("mean average precision", |m| m.average_precision),
                ("precision@10", |m| m.precision_at_10),
                ("recall@50", |m| m.recall_at_50),
            ];
            for (name, get) in metrics {
                writeln!(
                    report,
                    "{:>8} | {:>7.3} | {:>7.3}{:>9.3} | {:>7.3}{:>9.3} | {name}",
                    per_class * 2,
                    get(&query_only),
                    get(&naive_plain),
                    get(&naive_blend),
                    get(&sefr_plain),
                    get(&sefr_blend),
                )
                .unwrap();
            }
            writeln!(report).unwrap();

            // The scenario has to leave room for the feedback to matter.
            assert!(
                query_only.precision_at_10 < 0.75,
                "the bare query already scored {:.3} at precision@10, \
                 so the scenario does not exercise the feedback",
                query_only.precision_at_10,
            );

            // Below this the min-max scaler is fit on too few points to place
            // the dimensions sensibly, and SEFR is not competitive.
            const ENOUGH_FEEDBACK: usize = 8;
            if per_class * 2 >= ENOUGH_FEEDBACK {
                assert!(
                    sefr_plain.average_precision > 1.5 * query_only.average_precision,
                    "with {} feedback items sefr scored {:.3} mean average precision \
                     against {:.3} for the bare query, which is not a clear gain",
                    per_class * 2,
                    sefr_plain.average_precision,
                    query_only.average_precision,
                );

                // Uneven per-dimension scale is what SEFR's per-dimension
                // weighting is for, and where it should beat the naive
                // strategy even with the latter's coefficients oracle-tuned.
                // Compared like for like: with the query, and without it.
                if anisotropic {
                    assert!(
                        sefr_plain.average_precision > naive_plain.average_precision,
                        "with {} feedback items in an uneven space sefr scored {:.3} \
                         mean average precision against {:.3} for a tuned naive",
                        per_class * 2,
                        sefr_plain.average_precision,
                        naive_plain.average_precision,
                    );
                    assert!(
                        sefr_blend.average_precision > naive_blend.average_precision,
                        "with {} feedback items in an uneven space sefr+query scored \
                         {:.3} mean average precision against {:.3} for a tuned naive+query",
                        per_class * 2,
                        sefr_blend.average_precision,
                        naive_blend.average_precision,
                    );
                }
            }

            // Mixing in the original query should never be the worse choice,
            // for either strategy.
            assert!(
                sefr_blend.average_precision >= sefr_plain.average_precision - 1e-6,
                "blending the query into sefr hurt: {:.3} against {:.3}",
                sefr_blend.average_precision,
                sefr_plain.average_precision,
            );
            assert!(
                naive_blend.average_precision >= naive_plain.average_precision - 1e-6,
                "blending the query into naive hurt: {:.3} against {:.3}",
                naive_blend.average_precision,
                naive_plain.average_precision,
            );
        }
    }

    println!("{report}");
}

/// Which fixed `target_weight` to recommend. The table above oracle-tunes the
/// blend per trial, which no default can reproduce, so this reports mean average
/// precision for each candidate value held constant.
#[test]
fn sefr_target_weight_sweep() {
    let mut report = String::new();

    for anisotropic in [false, true] {
        let space = if anisotropic {
            "uneven per-dimension scale"
        } else {
            "uniform per-dimension scale"
        };
        writeln!(report, "\n== mean average precision, {space} ==").unwrap();
        write!(report, "{:>8} |", "feedback").unwrap();
        for target_weight in TARGET_WEIGHTS {
            write!(report, "{target_weight:>9}").unwrap();
        }
        writeln!(report).unwrap();

        for per_class in FEEDBACK_SIZES.map(|size| size / 2) {
            let mut per_weight = vec![Vec::new(); TARGET_WEIGHTS.len()];

            for trial in 0..TRIALS {
                let mut rng = StdRng::seed_from_u64(trial as u64);
                let scenario = build_scenario(&mut rng, anisotropic);
                let (feedback, relevant, irrelevant) =
                    split_feedback(&scenario, per_class, &mut rng);
                let weights = sefr_weight_vector(&feedback);

                for (slot, target_weight) in per_weight.iter_mut().zip(TARGET_WEIGHTS) {
                    let blend = blended(&weights, &scenario.target, target_weight);
                    slot.push(evaluate(&blend, &relevant, &irrelevant));
                }
            }

            write!(report, "{:>8} |", per_class * 2).unwrap();
            for slot in &per_weight {
                write!(report, "{:>9.3}", mean(slot).average_precision).unwrap();
            }
            writeln!(report).unwrap();
        }
    }

    println!("{report}");
}
