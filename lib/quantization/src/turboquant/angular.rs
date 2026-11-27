//! TQ+ symmetric scoring of the sign widths (1 and 1.5 bits) for Euclid.
//!
//! TQ+ stores the signs of `X − M`, the vector rescaled to length `sqrt(d)` and centered by
//! the per-coordinate shift. Their agreement measures the angle between two centered vectors,
//! not the dot of the vectors themselves, and Euclid needs that dot on the same scale as the
//! norms it is subtracted from. So the dot is rebuilt around the centering:
//!
//! `⟨X_a, X_b⟩ = ⟨X_a − M, X_b − M⟩ + ⟨X_a, M⟩ + ⟨X_b, M⟩ − ⟨M, M⟩`
//!
//! `xm = ⟨X, M⟩` is stored per vector, so the radius `r = ‖X − M‖` is exact as well:
//! `r² = d − 2·xm + ⟨M, M⟩`. With it the last three terms are `d − (r_a² + r_b²) / 2`, and the
//! first one is `r_a · r_b · cos θ`. The signs of two vectors at angle `θ` differ in `θ / π` of
//! the `n` coordinates the rotation fills, so `cos θ` is `cos(π · h / n)` of the Hamming distance
//! `h`. `n` is `d`, except with [`TQRotation::Unpadded`], where the padding stays zero in every
//! vector and its bits agree in every pair. The linear `1 − 2h / n` that a sign dot computes
//! underestimates it, by `2 / π` for vectors near orthogonal.
//!
//! The popcount of a short code is cheaper than a square root or a cosine, so neither is
//! computed per pair: the radius is found once per vector as it is read, and kept as
//! [`SymmetricValues::correction`] in place of `xm`, and `cos(π · h / n)` is read from a table
//! of the `d + 1` values `h` takes. Nothing is added to what is stored per vector.
//!
//! [`TQRotation::Unpadded`]: crate::turboquant::TQRotation::Unpadded

use std::f64::consts::PI;

use crate::turboquant::quantization::SymmetricValues;
use crate::turboquant::simd::query1bit;

/// How TQ+ turns the sign score of two vectors of 1 or 1.5 bits into the dot of the vectors as
/// rescaled to length `sqrt(d)`, for Euclid. See the module.
#[derive(Clone, Debug)]
pub(crate) struct AngularCorrection {
    /// `d`: the squared length of every rescaled vector, and the bits of a code
    dim: f32,
    /// `1 / sqrt(d)`
    inv_sqrt_dim: f32,
    /// `1 / sqrt(n)`, with `n` the coordinates the rotation fills, see [`Self::new`]
    inv_sqrt_rotation_dim: f32,
    /// `⟨M, M⟩`
    mm: f32,
    /// The sign score of two equal vectors
    full_score: f32,
    /// Differing bits per unit of sign score lost
    differing_per_score: f32,
    /// `cos(π · h / n)` by the Hamming distance `h` in `0..=d`
    cos_by_differing: Box<[f32]>,
}

impl AngularCorrection {
    /// For codes of `padded_dim` bits, of which the rotation fills `rotation_dim`, `n`. The
    /// others are zero in every vector, so their signs agree in every pair and say nothing of
    /// the angle.
    pub(crate) fn new(padded_dim: usize, rotation_dim: usize, mm: f32) -> Self {
        debug_assert!(
            0 < rotation_dim && rotation_dim <= padded_dim,
            "a rotation of {rotation_dim} coordinates of {padded_dim}",
        );
        let dim = padded_dim as f32;
        let full_score = query1bit::differing_to_score(dim, 0.0);
        // At most `rotation_dim` bits differ; past that the cosine stays at its least, rather
        // than turn back up beyond `π`
        let cos_by_differing = (0..=padded_dim)
            .map(|differing| {
                (PI * differing.min(rotation_dim) as f64 / rotation_dim as f64).cos() as f32
            })
            .collect();
        AngularCorrection {
            dim,
            inv_sqrt_dim: dim.sqrt().recip(),
            inv_sqrt_rotation_dim: (rotation_dim as f32).sqrt().recip(),
            mm,
            full_score,
            differing_per_score: dim / (full_score - query1bit::differing_to_score(dim, dim)),
            cos_by_differing,
        }
    }

    /// Heap memory of the table.
    pub(crate) fn heap_size_bytes(&self) -> usize {
        size_of_val::<[f32]>(&self.cos_by_differing)
    }

    /// The factor that turns a rescaled vector back into the vector of length `l2_length`.
    #[inline(always)]
    pub(crate) fn scale(&self, l2_length: f32) -> f32 {
        l2_length * self.inv_sqrt_dim
    }

    /// `‖X − M‖` of a rescaled vector from its stored `xm`: what the rescaled dot reads of the
    /// vector as [`SymmetricValues::correction`].
    #[inline]
    pub(crate) fn radius(&self, xm: f32) -> f32 {
        (self.dim - 2.0 * xm + self.mm).max(0.0).sqrt()
    }

    /// How far every coordinate the rotation fills of a rescaled vector is from `M`, if the
    /// vector is taken to be its signs at its [`Self::radius`].
    pub(crate) fn step(&self, xm: f32) -> f32 {
        self.radius(xm) * self.inv_sqrt_rotation_dim
    }

    /// `⟨X_a, X_b⟩` of the rescaled vectors from `sign_score`, what
    /// [`query1bit::score_1bit_internal`] returns for their codes, and their [`Self::radius`]
    /// as [`SymmetricValues::correction`].
    #[inline(always)]
    pub(crate) fn rescaled_dot(
        &self,
        sign_score: f32,
        values_a: &SymmetricValues,
        values_b: &SymmetricValues,
    ) -> f32 {
        let differing = ((self.full_score - sign_score) * self.differing_per_score).round();
        self.rescaled_dot_of_differing(differing, values_a, values_b)
    }

    /// [`Self::rescaled_dot`] from the number of `differing` bits itself, as the matrix kernels
    /// count them.
    #[inline(always)]
    pub(crate) fn rescaled_dot_of_differing(
        &self,
        differing: f32,
        values_a: &SymmetricValues,
        values_b: &SymmetricValues,
    ) -> f32 {
        let (radius_a, radius_b) = (values_a.correction, values_b.correction);
        let cos = self.cos_by_differing[differing as usize];
        radius_a * radius_b * cos + self.dim - 0.5 * (radius_a * radius_a + radius_b * radius_b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sign score maps back to the Hamming distance it came from, for every distance.
    #[test]
    fn sign_score_to_differing() {
        for dim in [64, 128, 192, 960, 1536, 4096] {
            let angular = AngularCorrection::new(dim, dim, 0.0);
            let values = SymmetricValues {
                scale: 1.0,
                l2_sq: 1.0,
                correction: 1.0,
            };
            for differing in 0..=dim {
                let sign_score = query1bit::differing_to_score(dim as f32, differing as f32);
                assert_eq!(
                    angular.rescaled_dot(sign_score, &values, &values),
                    angular.rescaled_dot_of_differing(differing as f32, &values, &values),
                    "dim={dim} differing={differing}",
                );
            }
        }
    }
}
