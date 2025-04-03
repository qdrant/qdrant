use itertools::Itertools;
use rand::Rng;

use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::fixtures::payload_fixtures::random_multi_vector;
use crate::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};

const MAX_EXAMPLE_PAIRS: usize = 4;

pub enum QueryVariant {
    Nearest,
    RecoBestScore,
    RecoSumScores,
    Discovery,
    Context,
}

pub fn random_query<R: Rng + ?Sized>(
    variant: &QueryVariant,
    rng: &mut R,
    rand_vec: impl Fn(&mut R) -> VectorInternal,
) -> QueryVector {
    match variant {
        QueryVariant::Nearest => rand_vec(rng).into(),
        QueryVariant::Discovery => random_discovery_query(rng, rand_vec),
        QueryVariant::Context => random_context_query(rng, rand_vec),
        QueryVariant::RecoBestScore => {
            QueryVector::RecommendBestScore(random_reco_query(rng, rand_vec))
        }
        QueryVariant::RecoSumScores => {
            QueryVector::RecommendSumScores(random_reco_query(rng, rand_vec))
        }
    }
}

pub fn random_multi_vec_query<R: Rng + ?Sized>(
    variant: &QueryVariant,
    rng: &mut R,
    dim: usize,
    num_vector_per_points: usize,
) -> QueryVector {
    let rand_vec = move |rng: &mut R| -> VectorInternal {
        random_multi_vector(rng, dim, num_vector_per_points).into()
    };

    random_query(variant, rng, rand_vec)
}

fn random_discovery_query<R: Rng + ?Sized>(
    rng: &mut R,
    rand_vec: impl Fn(&mut R) -> VectorInternal,
) -> QueryVector {
    let num_pairs: usize = rng.random_range(1..MAX_EXAMPLE_PAIRS);

    let target = rand_vec(rng);

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = rand_vec(rng);
            let negative = rand_vec(rng);
            ContextPair { positive, negative }
        })
        .collect_vec();

    DiscoveryQuery::new(target, pairs).into()
}

fn random_context_query<R: Rng + ?Sized>(
    rng: &mut R,
    rand_vec: impl Fn(&mut R) -> VectorInternal,
) -> QueryVector {
    let num_pairs: usize = rng.random_range(0..MAX_EXAMPLE_PAIRS);

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = rand_vec(rng);
            let negative = rand_vec(rng);
            ContextPair { positive, negative }
        })
        .collect_vec();

    QueryVector::Context(ContextQuery::new(pairs))
}

fn random_reco_query<R: Rng + ?Sized>(
    rng: &mut R,
    rand_vec: impl Fn(&mut R) -> VectorInternal,
) -> RecoQuery<VectorInternal> {
    let num_examples: usize = rng.random_range(1..MAX_EXAMPLE_PAIRS);

    let positive = (0..num_examples).map(|_| rand_vec(rng)).collect_vec();
    let negative = (0..num_examples).map(|_| rand_vec(rng)).collect_vec();

    RecoQuery::new(positive, negative)
}
