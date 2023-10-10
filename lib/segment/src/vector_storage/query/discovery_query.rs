use common::types::ScoreType;

type RankType = i32;

#[derive(Debug, Clone)]
pub struct DiscoveryPair<T> {
    positive: T,
    negative: T,
}

impl<T> DiscoveryPair<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.positive).chain(std::iter::once(&self.negative))
    }

    pub fn transform<F, U>(self, mut f: F) -> DiscoveryPair<U>
    where
        F: FnMut(T) -> U,
    {
        DiscoveryPair {
            positive: f(self.positive),
            negative: f(self.negative),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryQuery<T> {
    pub target: T,
    pub pairs: Vec<DiscoveryPair<T>>,
}

impl<T> DiscoveryQuery<T> {
    pub fn new(target: T, pairs: Vec<DiscoveryPair<T>>) -> Self {
        Self { target, pairs }
    }

    pub fn iter_all(&self) -> impl Iterator<Item = &T> {
        let pairs_iter = self.pairs.iter().flat_map(|pair| pair.iter());

        std::iter::once(&self.target).chain(pairs_iter)
    }

    pub fn transform<F, U>(self, mut f: F) -> DiscoveryQuery<U>
    where
        F: FnMut(T) -> U,
    {
        DiscoveryQuery::new(
            f(self.target),
            self.pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .collect(),
        )
    }

    /// Compares the query against a single vector using a similarity function.
    ///
    /// Similarity function must be able to compare one vector against another to produce an intermediate score.
    ///
    /// Returns the score with regards to this query
    pub fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let rank = self.rank_by(&similarity);

        let target_similarity = similarity(&self.target);
        let sigmoid_similarity = 0.5 * (fast_sigmoid(target_similarity) + 1.0);

        rank as ScoreType + sigmoid_similarity
    }

    pub fn rank_by(&self, similarity: impl Fn(&T) -> ScoreType) -> RankType {
        self.pairs
            .iter()
            .map(|pair| {
                // get similarity to positive and negative
                let positive_similarity = similarity(&pair.positive);
                let negative_similarity = similarity(&pair.negative);

                // if closer to positive, return 1, else -1
                if positive_similarity > negative_similarity {
                    1
                } else {
                    -1
                }
            })
            // get overall rank
            .sum()
    }
}

/// Acts as a substitute for sigmoid function, but faster because it doesn't do exponent.
///
/// Range of output is (-1, 1)
fn fast_sigmoid(x: ScoreType) -> ScoreType {
    // from https://stackoverflow.com/questions/10732027/fast-sigmoid-algorithm
    x / (1.0 + x.abs())
}

// impl From<DiscoveryQuery<VectorType>> for QueryVector {
//     fn from(query: DiscoveryQuery<VectorType>) -> Self {
//         QueryVector::Discovery(query)
//     }
// }
