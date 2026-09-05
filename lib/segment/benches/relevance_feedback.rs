//! Compares the cost of the two relevance feedback strategies as the number of
//! feedback items grows.
//!
//! The `naive` strategy expands the feedback into every ordered pair and walks
//! all of them for each candidate, so its per-candidate cost grows with the
//! square of the feedback size. SEFR fits once and then scores each candidate
//! with a single dot product, so its per-candidate cost is flat.
//!
//! Both measurements include the per-query setup, since that is what a client
//! pays for one request.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ordered_float::OrderedFloat;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::vectors::{DenseVector, VectorElementType};
use segment::spaces::metric::Metric;
use segment::spaces::simple::DotProductMetric;
use segment::vector_storage::query::{
    FeedbackItem, NaiveFeedbackCoefficients, NaiveFeedbackQuery, Query, fit_sefr,
};

const DIM: usize = 768;
const CANDIDATES: usize = 1_000;
const FEEDBACK_SIZES: [usize; 5] = [2, 4, 8, 16, 32];

fn random_vector(rng: &mut SmallRng) -> DenseVector {
    (0..DIM).map(|_| rng.random_range(0.0..1.0)).collect()
}

fn feedback_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("relevance-feedback");

    let mut rng = SmallRng::seed_from_u64(42);
    let candidates: Vec<DenseVector> = (0..CANDIDATES).map(|_| random_vector(&mut rng)).collect();
    let target = random_vector(&mut rng);

    for size in FEEDBACK_SIZES {
        // Half the items score high, half low, so SEFR always sees both classes.
        let feedback: Vec<FeedbackItem<DenseVector>> = (0..size)
            .map(|i| FeedbackItem {
                vector: random_vector(&mut rng),
                score: OrderedFloat(if i % 2 == 0 { 1.0 } else { 0.0 }),
            })
            .collect();

        group.bench_with_input(BenchmarkId::new("naive", size), &size, |b, _| {
            b.iter(|| {
                let query = NaiveFeedbackQuery {
                    target: target.clone(),
                    feedback: feedback.clone(),
                    coefficients: NaiveFeedbackCoefficients {
                        a: OrderedFloat(1.0),
                        b: OrderedFloat(1.0),
                        c: OrderedFloat(1.0),
                    },
                }
                .into_query();

                let mut total = 0.0;
                for candidate in &candidates {
                    total += query.score_by(|example: &DenseVector| {
                        <DotProductMetric as Metric<VectorElementType>>::similarity(
                            example, candidate,
                        )
                    });
                }
                black_box(total)
            });
        });

        group.bench_with_input(BenchmarkId::new("sefr", size), &size, |b, _| {
            b.iter(|| {
                let positives: Vec<&[VectorElementType]> = feedback
                    .iter()
                    .filter(|item| item.score.0 > 0.5)
                    .map(|item| item.vector.as_slice())
                    .collect();
                let negatives: Vec<&[VectorElementType]> = feedback
                    .iter()
                    .filter(|item| item.score.0 <= 0.5)
                    .map(|item| item.vector.as_slice())
                    .collect();
                let model = fit_sefr(&positives, &negatives).unwrap();

                // Production searches the folded weights as a plain nearest
                // query, so scoring goes through the same metric the naive
                // strategy uses. The constant offset does not affect ranking.
                let mut total = 0.0;
                for candidate in &candidates {
                    total += <DotProductMetric as Metric<VectorElementType>>::similarity(
                        &model.weights,
                        candidate,
                    );
                }
                black_box(total)
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = feedback_bench
}

criterion_main!(benches);
