use common::bitpacking::{BitReader, BitWriter};

use crate::encoded_vectors::DistanceType;
use crate::turboquant::quantization::TurboQuantizer;
use crate::turboquant::{TQBits, TQMode};

/// Lazy view over the encoded extra-data attached to a TurboQuant-quantized
/// vector. Holds nothing but the underlying byte slice; the metadata required
/// to interpret those bytes is supplied per-call by passing a
/// [`TurboQuantizer`] reference to the getters.
#[derive(Clone, Copy)]
pub struct TqVectorExtras<'a> {
    src: &'a [u8],
}

impl<'a> TqVectorExtras<'a> {
    /// Wrap previously-encoded extras bytes for lazy decoding. Validity of
    /// `src.len()` against the configuration is the caller's responsibility —
    /// in practice the bytes come from [`TurboQuantizer::split_vector`] or
    /// [`TurboQuantizer::pack_extras_into`].
    pub(super) fn from_bytes(src: &'a [u8]) -> Self {
        Self { src }
    }

    /// Raw bytes backing this view. Intended for re-emitting the encoded
    /// extras into a packed vector — prefer the typed getters for inspection.
    #[inline]
    pub(super) fn as_bytes(&self) -> &'a [u8] {
        self.src
    }

    /// Returns the size (in bytes) required for the extras.
    pub(super) fn size_for(_bits: TQBits, distance: DistanceType, mode: TQMode) -> usize {
        let scaling_factor_size = match distance {
            // 4 Bytes for merged l2 with re-normalization applied.
            DistanceType::Dot => size_of::<f32>(),
            // 4 Bytes for re-normalization.
            DistanceType::Cosine => size_of::<f32>(),
            DistanceType::L1 | DistanceType::L2 => size_of::<f32>(),
        };
        // L2 folds renormalization into `scaling_factor = l2 / cn` (like
        // Dot/Cosine) but still needs the raw `l2_length` for the `||v||²`
        // term in the L2 score formula — store it as a second f32.
        let l2_length_size = match distance {
            DistanceType::L2 => size_of::<f32>(),
            DistanceType::Dot | DistanceType::Cosine | DistanceType::L1 => 0,
        };
        let ec_correction_size = match mode {
            // TQ+ stores `xm = ⟨X, M⟩` per vector for the symmetric-scoring
            // slow path.
            TQMode::Plus => size_of::<f32>(),
            TQMode::Normal => 0,
        };
        scaling_factor_size + l2_length_size + ec_correction_size
    }

    /// Per-vector scaling factor that scoring multiplies into the centroid dot.
    /// Combined from `l2_length` and `centroid_norm` at quantize time —
    /// see [`TurboQuantizer::pack_extras_into`] for the per-distance formula.
    pub fn scaling_factor(&self) -> f32 {
        let bytes: [u8; 4] = self.src[..size_of::<f32>()]
            .try_into()
            .expect("expected at least 4 bytes for scaling_factor");
        f32::from_le_bytes(bytes)
    }

    /// Original L2 length of the source vector (pre-rotation, pre-rescale).
    /// Only present for [`DistanceType::L2`] — stored at offset 4 immediately
    /// after `scaling_factor`. Needed because L2's `scaling_factor` already
    /// folds in the renormalization (`l2 / cn`, matching Dot/Cosine), so the
    /// raw `||v||²` term in the L2 score formula has to be recovered from a
    /// separate field.
    pub fn l2_length(&self) -> f32 {
        debug_assert!(
            self.src.len() >= 2 * size_of::<f32>(),
            "l2_length is only stored for DistanceType::L2"
        );
        let off = size_of::<f32>();
        let bytes: [u8; 4] = self.src[off..off + size_of::<f32>()]
            .try_into()
            .expect("expected 4 bytes for l2_length");
        f32::from_le_bytes(bytes)
    }

    /// TQ+ per-vector `⟨X, M⟩` correction. Only present when the quantizer
    /// was configured with [`TQMode::Plus`]; the caller is responsible for
    /// only calling this getter in that mode (validated via `debug_assert`).
    /// Stored as the trailing 4 bytes of the extras blob — the only field
    /// whose offset can shift (L2 stores an extra `centroid_norm` before it).
    pub fn ec_correction(&self) -> f32 {
        debug_assert!(
            self.src.len() >= 2 * size_of::<f32>(),
            "ec_correction is only stored for TQMode::Plus"
        );
        let off = self.src.len() - size_of::<f32>();
        let bytes: [u8; 4] = self.src[off..off + size_of::<f32>()]
            .try_into()
            .expect("expected 4 bytes for ec_correction");
        f32::from_le_bytes(bytes)
    }
}

impl TurboQuantizer {
    /// Bit-pack a sequence of rotated, Lloyd-Max-scaled values into a compact
    /// byte vector. Each value is mapped to its nearest centroid index via the
    /// bit-width's decision boundaries before packing. The `extras` view is
    /// appended verbatim and must already match [`TqVectorExtras::size_for`].
    ///
    /// Symmetric inverse of [`Self::unpack_vector`]: feeding its yielded f64s
    /// back through `pack_vector` reproduces the same byte layout.
    pub(super) fn pack_vector<I>(&self, scaled: I, extras: TqVectorExtras<'_>) -> Vec<u8>
    where
        I: IntoIterator<Item = f64>,
    {
        debug_assert_eq!(
            extras.as_bytes().len(),
            TqVectorExtras::size_for(self.bits, self.distance, self.mode),
            "extras must match the configured extras size",
        );

        let mut out = Vec::with_capacity(self.quantized_size());
        let mut bit_writer = BitWriter::new(&mut out);

        let boundaries = self.bits.get_centroid_boundaries();
        let bits = self.bits.bit_size();
        for val in scaled {
            let idx = boundaries.partition_point(|&b| (val as f32) > b) as u8;
            bit_writer.write(idx, bits);
        }
        bit_writer.finish();
        out.extend_from_slice(extras.as_bytes());
        out
    }

    /// Splits an encoded vector into its packed dimension bytes and a lazy
    /// view over the extras stored alongside them. Shared between
    /// [`Self::unpack_vector`] (scalar scoring) and the SIMD scoring paths
    /// that consume the raw packed bytes directly.
    pub(super) fn split_vector<'a>(&self, vec: &'a [u8]) -> (&'a [u8], TqVectorExtras<'a>) {
        let extra_len = TqVectorExtras::size_for(self.bits, self.distance, self.mode);
        let (dim_part, extra_part) = vec.split_at(vec.len() - extra_len);
        (dim_part, TqVectorExtras::from_bytes(extra_part))
    }

    /// Unpacks `vec` into an iterator of `dim` centroid values and returns a
    /// lazy [`TqVectorExtras`] view over the bytes stored alongside them.
    ///
    /// The iterator lazily decodes one centroid per step, so callers scoring a
    /// vector can `zip` it with a query and compute the dot in a single pass
    /// without materializing an intermediate buffer.
    ///
    /// Does not apply the inverse rotation — the iterator yields values in the
    /// rotated space.
    pub fn unpack_vector<'a>(
        &self,
        vec: &'a [u8],
    ) -> (impl Iterator<Item = f64> + 'a, TqVectorExtras<'a>) {
        let (dim_part, extras) = self.split_vector(vec);

        let centroids = self.bits.get_centroids();
        let mut reader = BitReader::new(dim_part);
        reader.set_bits(self.bits.bit_size());

        let iter = (0..self.padded_dim).map(move |_| {
            let idx: u8 = reader.read();
            f64::from(centroids[idx as usize])
        });

        (iter, extras)
    }

    /// Size in bytes of a vector quantized by this quantizer.
    pub fn quantized_size(&self) -> usize {
        Self::quantized_size_for(self.padded_dim, self.bits, self.distance, self.mode)
    }

    /// Total size in bytes of a quantized vector, including both the packed
    /// dimensions and any extras..
    pub(super) fn quantized_size_for(
        dim: usize,
        bits: TQBits,
        distance: DistanceType,
        mode: TQMode,
    ) -> usize {
        let vector_data_size =
            Self::padded_dim(dim, bits) * bits.bit_size() as usize / u8::BITS as usize;
        let extras_size = TqVectorExtras::size_for(bits, distance, mode);
        vector_data_size + extras_size
    }

    // Padded dimension for the vector
    pub(crate) fn padded_dim(dim: usize, bits: TQBits) -> usize {
        match bits {
            TQBits::Bits1 => dim.next_multiple_of(8), // 8 elements per byte
            TQBits::Bits1_5 => (dim * 3 / 2).next_multiple_of(8), // // 16 elements per 3 bytes
            TQBits::Bits2 => dim.next_multiple_of(4), // 4 elements per byte
            TQBits::Bits4 => dim.next_multiple_of(2), // 2 elements per byte
        }
    }

    /// Computes the L2 length of `rotated_vec` for the distance metrics that
    /// store it. Returns `None` for metrics that don't (Cosine).
    pub(super) fn compute_l2_length(&self, rotated_vec: &[f64]) -> Option<f32> {
        match self.distance {
            DistanceType::Dot | DistanceType::L1 | DistanceType::L2 => {
                Some(rotated_vec.iter().map(|&i| i * i).sum::<f64>().sqrt() as f32)
            }
            DistanceType::Cosine => None,
        }
    }

    /// Encodes the given raw extras values and appends them to `buf`.
    /// `ec_correction` (`xm = ⟨X, M⟩`) is required when the quantizer is in
    /// [`TQMode::Plus`] and ignored otherwise. `centroid_norm` is required for
    /// Dot/Cosine/L2 — for all three it's folded into the merged
    /// `scaling_factor = l2_length / centroid_norm`. L2 additionally stores
    /// the raw `l2_length` separately so scoring can recover the `||v||²` term.
    pub(super) fn pack_extras_into(
        &self,
        l2_length: Option<f32>,
        centroid_norm: Option<f32>,
        ec_correction: Option<f32>,
        buf: &mut Vec<u8>,
    ) {
        let scaling_factor = match self.distance {
            DistanceType::Dot | DistanceType::Cosine | DistanceType::L2 => {
                l2_length.unwrap_or(1.0) / centroid_norm.unwrap()
            }
            DistanceType::L1 => l2_length.unwrap(),
        };

        buf.extend(&scaling_factor.to_le_bytes());

        // L2 stores the raw `l2_length` separately: `scaling_factor` already
        // folds in renormalization, but the `||v_a||² + ||v_b||²` part of the
        // score formula still needs the un-divided lengths.
        if matches!(self.distance, DistanceType::L2) {
            buf.extend(&l2_length.unwrap().to_le_bytes());
        }

        if matches!(self.mode, TQMode::Plus) {
            buf.extend(&ec_correction.unwrap_or(0.0).to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bit widths exercised by the extras tests. `Bits1_5` is excluded: its
    /// `bit_size` is not yet implemented and panics.
    const SUPPORTED_BITS: &[TQBits] = &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4];
    /// Distances exercised by the extras tests. `L1`/`L2` are excluded: their
    /// `size_for` arms panic with `unimplemented!`.
    const SUPPORTED_DISTANCES: &[DistanceType] = &[
        DistanceType::Dot,
        DistanceType::Cosine,
        DistanceType::L1,
        DistanceType::L2,
    ];
    const ALL_MODES: &[TQMode] = &[TQMode::Normal, TQMode::Plus];

    /// `(l2_length, centroid_norm, expected_scaling_factor)` per distance:
    /// the first two are the raw inputs to `pack_extras_into`; the third is
    /// the merged f32 that `scaling_factor()` should read back. For L2 the
    /// merged `scaling_factor` is `l2/cn` (like Dot/Cosine) and the raw
    /// `l2_length` is roundtripped via the dedicated `l2_length()` getter.
    fn example_extras(distance: DistanceType) -> (Option<f32>, Option<f32>, f32) {
        match distance {
            DistanceType::Dot => (Some(1.25), Some(15.5), 1.25 / 15.5),
            DistanceType::Cosine => (None, Some(0.875), 1.0 / 0.875),
            DistanceType::L2 => (Some(1.3), Some(8.0), 1.3 / 8.0),
            DistanceType::L1 => (Some(1.3), None, 1.3),
        }
    }

    /// For every supported (bits, mode, distance) combo, assert:
    ///   1. `pack_extras_into` writes exactly `size_for` bytes.
    ///   2. A `TqVectorExtras` view over those bytes recovers the packed values
    ///      via its getters.
    ///
    /// When a new field is added to `TqVectorExtras`, extend this test with an
    /// assertion for its getter — there is no struct destructuring to force it.
    #[test]
    fn extras_size_matches_pack_and_roundtrips_unpack() {
        let dim = 64;

        for &bits in SUPPORTED_BITS {
            for &mode in ALL_MODES {
                for &distance in SUPPORTED_DISTANCES {
                    let predicted_extra_size = TqVectorExtras::size_for(bits, distance, mode);

                    let tq = TurboQuantizer::new(dim, bits, mode, distance, None);
                    let (l2_length, centroid_norm, expected_scaling) = example_extras(distance);
                    let expected_ec = match mode {
                        TQMode::Plus => Some(-0.5),
                        TQMode::Normal => None,
                    };

                    let mut buf = Vec::new();
                    tq.pack_extras_into(l2_length, centroid_norm, expected_ec, &mut buf);
                    assert_eq!(
                        buf.len(),
                        predicted_extra_size,
                        "pack size mismatch (bits={bits:?}, mode={mode:?}, distance={distance:?})",
                    );

                    if predicted_extra_size == 0 {
                        continue;
                    }

                    let extras = TqVectorExtras::from_bytes(&buf);
                    assert_eq!(
                        extras.scaling_factor(),
                        expected_scaling,
                        "scaling_factor roundtrip (bits={bits:?}, mode={mode:?}, distance={distance:?})",
                    );
                    if matches!(distance, DistanceType::L2) {
                        let expected_l2 = l2_length.expect("L2 must supply l2_length");
                        assert_eq!(
                            extras.l2_length(),
                            expected_l2,
                            "l2_length roundtrip (bits={bits:?}, mode={mode:?}, distance={distance:?})",
                        );
                    }
                    if let Some(expected) = expected_ec {
                        assert_eq!(
                            extras.ec_correction(),
                            expected,
                            "ec_correction roundtrip (bits={bits:?}, mode={mode:?}, distance={distance:?})",
                        );
                    }
                }
            }
        }
    }
}
