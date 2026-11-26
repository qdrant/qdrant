//! Quantized codes read by an [`F32VectorReader`]: each as its
//! [`SymmetricVectors::symmetric_vector`], the vector whose geometry the symmetric score scores,
//! with its [`SymmetricVectors::duplicate_key`].

use common::types::PointOffsetType;
use quantization::{DuplicateKey, EncodedVectors, SymmetricVectors};

use super::F32VectorReader;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};

/// Reads `quantized` as f32, `None` for product quantization, which has no symmetric vector, and
/// for multivectors.
pub(super) fn f32_reader(quantized: &QuantizedVectors) -> Option<Box<dyn F32VectorReader + '_>> {
    let reader = match quantized.get_storage() {
        QuantizedVectorStorage::ScalarRam(q) => codes(q),
        QuantizedVectorStorage::ScalarMmap(q) => codes(q),
        QuantizedVectorStorage::ScalarChunkedMmap(q) => codes(q),
        QuantizedVectorStorage::BinaryRam(q) => codes(q),
        QuantizedVectorStorage::BinaryMmap(q) => codes(q),
        QuantizedVectorStorage::BinaryChunkedMmap(q) => codes(q),
        QuantizedVectorStorage::TQRam(q) => codes(q),
        QuantizedVectorStorage::TQMmap(q) => codes(q),
        QuantizedVectorStorage::TQChunkedMmap(q) => codes(q),
        QuantizedVectorStorage::PQRam(_)
        | QuantizedVectorStorage::PQMmap(_)
        | QuantizedVectorStorage::PQChunkedMmap(_)
        | QuantizedVectorStorage::ScalarRamMulti(_)
        | QuantizedVectorStorage::ScalarMmapMulti(_)
        | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
        | QuantizedVectorStorage::PQRamMulti(_)
        | QuantizedVectorStorage::PQMmapMulti(_)
        | QuantizedVectorStorage::PQChunkedMmapMulti(_)
        | QuantizedVectorStorage::BinaryRamMulti(_)
        | QuantizedVectorStorage::BinaryMmapMulti(_)
        | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
        | QuantizedVectorStorage::TQRamMulti(_)
        | QuantizedVectorStorage::TQMmapMulti(_)
        | QuantizedVectorStorage::TQChunkedMmapMulti(_) => return None,
    };
    Some(reader)
}

/// The codes of one quantization.
struct CodeReader<'a, E>(&'a E);

fn codes<E>(encoded: &E) -> Box<dyn F32VectorReader + '_>
where
    E: EncodedVectors + SymmetricVectors + Sync,
{
    Box::new(CodeReader(encoded))
}

impl<E> F32VectorReader for CodeReader<'_, E>
where
    E: EncodedVectors + SymmetricVectors + Sync,
{
    fn dim(&self) -> usize {
        self.0.symmetric_dim()
    }

    fn count(&self) -> usize {
        self.0.vectors_count()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], DuplicateKey<'_>),
    ) -> OperationResult<()> {
        let mut vector = Vec::with_capacity(self.dim());
        self.0.for_each_batch(ids, |i, code| {
            vector.clear();
            vector.extend(self.0.symmetric_vector(&code));
            f(i, &vector, self.0.duplicate_key(&code));
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::types::PointOffsetType;
    use rstest::rstest;

    use super::super::test_fixture::{Fixture, POINTS, binary, scalar, turbo};
    use crate::types::{
        BinaryQuantizationEncoding as Encoding, Distance, QuantizationConfig,
        TurboQuantBitSize as Bits, TurboQuantization,
    };

    /// Pair scores are an affine map of the dot product of the decoded codes, or for Euclid of
    /// their negative squared distance once each vector's self-score is taken out. That removes
    /// any offset per vector, so for Euclid this checks the cross term, not the stored lengths:
    /// at TQ+ 1 and 1.5 bits the decoded distance itself is only a proxy for the score. Only
    /// approximately at 2 and 4 bits of TurboQuant, whose score adds a stored TQ+ correction:
    /// there the decoded codes still explain the scores ten times better than the original
    /// vectors do.
    #[rstest]
    fn decoded_codes_score_as_the_scorer(
        #[values(
            scalar(),
            binary(Encoding::OneBit),
            binary(Encoding::OneAndHalfBits),
            binary(Encoding::TwoBits),
            turbo(Bits::Bits1),
            turbo(Bits::Bits1_5),
            turbo(Bits::Bits2),
            turbo(Bits::Bits4),
            turbo(Bits::Bits8)
        )]
        config: QuantizationConfig,
        #[values(Distance::Cosine, Distance::Dot, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(&config, distance, false);
        let decoded = fixture.decoded();
        let scorer = fixture.scorer();
        let score = |a: usize, b: usize| {
            f64::from(scorer.score_internal(a as PointOffsetType, b as PointOffsetType))
        };
        let geometry = |u: &[f32], v: &[f32]| {
            let pairs = u.iter().zip(v).map(|(&x, &y)| (f64::from(x), f64::from(y)));
            match distance {
                Distance::Euclid => -pairs.map(|(x, y)| (x - y).powi(2)).sum::<f64>(),
                Distance::Cosine | Distance::Dot => pairs.map(|(x, y)| x * y).sum(),
                Distance::Manhattan => unreachable!("no Manhattan case"),
            }
        };
        let (mut scores, mut of_decoded, mut of_originals) = (Vec::new(), Vec::new(), Vec::new());
        for a in 0..POINTS {
            for b in a + 1..POINTS {
                // The scores of each vector with itself cancel the lengths L2 stores
                scores.push(if distance == Distance::Euclid {
                    score(a, b) - (score(a, a) + score(b, b)) / 2.0
                } else {
                    score(a, b)
                });
                of_decoded.push(geometry(&decoded[a], &decoded[b]));
                of_originals.push(geometry(&fixture.vectors[a], &fixture.vectors[b]));
            }
        }

        let approximate = match &config {
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                match turbo.bits.unwrap_or_default() {
                    Bits::Bits2 | Bits::Bits4 => true,
                    Bits::Bits1 | Bits::Bits1_5 | Bits::Bits8 => false,
                }
            }
            QuantizationConfig::Scalar(_)
            | QuantizationConfig::Product(_)
            | QuantizationConfig::Binary(_) => false,
        };
        let agreement = correlation(&scores, &of_decoded);
        let least = if approximate {
            1.0 - (1.0 - correlation(&scores, &of_originals)) / 10.0
        } else {
            1.0 - 1e-5
        };
        assert!(agreement >= least, "{agreement} below {least}");
    }

    /// Pearson's correlation coefficient.
    fn correlation(a: &[f64], b: &[f64]) -> f64 {
        let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
        let (mean_a, mean_b) = (mean(a), mean(b));
        let centered = |v: &[f64], m: f64| v.iter().map(move |x| x - m).collect::<Vec<_>>();
        let (a, b) = (centered(a, mean_a), centered(b, mean_b));
        let dot = |x: &[f64], y: &[f64]| x.iter().zip(y).map(|(x, y)| x * y).sum::<f64>();
        dot(&a, &b) / (dot(&a, &a) * dot(&b, &b)).sqrt()
    }
}
