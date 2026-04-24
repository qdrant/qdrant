use common::bitpacking::{BitReader, BitWriter};

use crate::encoded_vectors::DistanceType;
use crate::turboquant::quantization::TurboQuantizer;
use crate::turboquant::{TQBits, TQMode};

/// Additional data that needs to be stored along quantized vectors.
#[derive(Default, Copy, Clone)]
pub struct TqVectorExtras {
    // TODO(turbo): add all fields.
    pub(super) l2_length: Option<f32>,
}

impl TqVectorExtras {
    /// Returns the size (in bytes) required for the extras.
    pub(super) fn size_for(_bits: TQBits, distance: DistanceType, _mode: TQMode) -> usize {
        let mut size = 0;

        match distance {
            DistanceType::Dot => {
                // For Dot product we need to additionally store the original vectors l2.
                size += size_of::<f32>();
            }
            DistanceType::Cosine => {}
            DistanceType::L1 => {
                size += size_of::<f32>();
            }
            DistanceType::L2 => {
                size += size_of::<f32>();
            }
        }

        size
    }
}

impl TurboQuantizer {
    /// Bit-pack a sequence of rotated, Lloyd-Max-scaled values into a compact
    /// byte vector. Each value is mapped to its nearest centroid index via the
    /// bit-width's decision boundaries before packing.
    ///
    /// Symmetric inverse of [`Self::unpack_vector`]: feeding its yielded f64s
    /// back through `pack_vector` reproduces the same byte layout.
    pub(super) fn pack_vector<I>(&self, scaled: I, extras: TqVectorExtras) -> Vec<u8>
    where
        I: IntoIterator<Item = f64>,
    {
        let mut out = Vec::with_capacity(self.quantized_size());
        let mut bit_writer = BitWriter::new(&mut out);

        let boundaries = self.bits.get_centroid_boundaries();
        let bits = self.bits.bit_size();
        for val in scaled {
            let idx = boundaries.partition_point(|&b| (val as f32) > b) as u8;
            bit_writer.write(idx, bits);
        }
        bit_writer.finish();
        self.pack_extras_into(&extras, &mut out);
        out
    }

    /// Split an encoded vector into its dimension bytes and the decoded extras.
    /// Shared between [`Self::unpack_vector`] (scalar scoring) and the SIMD
    /// scoring paths that consume the raw packed bytes directly.
    pub(super) fn split_vector<'a>(&self, vec: &'a [u8]) -> (TqVectorExtras, &'a [u8]) {
        let extra_len = TqVectorExtras::size_for(self.bits, self.distance, self.mode);
        let (dim_part, extra_part) = vec.split_at(vec.len() - extra_len);
        (self.unpack_extras_from(extra_part), dim_part)
    }

    /// Unpacks `vec` into an iterator of `dim` centroid values and returns any
    /// `Extras` stored alongside.
    ///
    /// The iterator lazily decodes one centroid per step, so callers scoring a
    /// vector can `zip` it with a query and compute the dot in a single pass
    /// without materializing an intermediate buffer.
    ///
    /// Does not apply the inverse rotation — the iterator yields values in the
    /// rotated space.
    pub fn unpack_vector(&self, vec: &[u8]) -> (TqVectorExtras, impl Iterator<Item = f64>) {
        let (extras, dim_part) = self.split_vector(vec);

        let centroids = self.bits.get_centroids();
        let mut reader = BitReader::new(dim_part);
        reader.set_bits(self.bits.bit_size());

        let iter = (0..self.padded_dim).map(move |_| {
            let idx: u8 = reader.read();
            f64::from(centroids[idx as usize])
        });

        (extras, iter)
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

    /// Generates extra data that is required to store together with the quantized dimensions.
    /// This depends on the  metadata of the `TurboQuantizer`, such as distance-type or TQ-Mode.
    ///
    /// Not all configurations need extras.
    pub(super) fn calculate_extras(&self, rotated_vec: &[f64]) -> TqVectorExtras {
        let mut extras = TqVectorExtras::default();

        match self.distance {
            DistanceType::Dot | DistanceType::L1 | DistanceType::L2 => {
                // For Dot product we need to additionally store the original vectors l2.
                extras.l2_length =
                    Some(rotated_vec.iter().map(|&i| i * i).sum::<f64>().sqrt() as f32);
            }
            DistanceType::Cosine => {}
        }

        extras
    }

    /// Packs (encodes) the extras into the given buffer.
    fn pack_extras_into(&self, extras: &TqVectorExtras, buf: &mut Vec<u8>) {
        let extra_len =
            Self::quantized_size_for(self.padded_dim, self.bits, self.distance, self.mode);

        if extra_len == 0 {
            return;
        }

        buf.reserve(extra_len);

        // Additional l2 for dot vectors.
        if let Some(l2) = extras.l2_length {
            buf.extend(&l2.to_le_bytes());
        }
    }

    /// Decodes `src` containing encoded 'extra'-data and returns the decoded `Extras`.
    ///
    /// This function requires `src` to have extra data encoded.
    fn unpack_extras_from(&self, src: &[u8]) -> TqVectorExtras {
        debug_assert_eq!(
            src.len(),
            TqVectorExtras::size_for(self.bits, self.distance, self.mode),
            "src must contain encoded extras data of the expected size"
        );

        let mut extras = TqVectorExtras::default();

        // Additional l2 for dot vectors.
        match self.distance {
            DistanceType::Dot | DistanceType::L1 | DistanceType::L2 => {
                let l2_bytes: [u8; 4] = src[..4].try_into().unwrap(); // TODO(turbo): maybe add error handling.
                extras.l2_length = Some(f32::from_le_bytes(l2_bytes));
            }
            DistanceType::Cosine => {}
        }

        extras
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
    const SUPPORTED_DISTANCES: &[DistanceType] = &[DistanceType::Dot, DistanceType::Cosine];
    const ALL_MODES: &[TQMode] = &[TQMode::Normal, TQMode::Plus];

    fn example_extras(distance: DistanceType) -> TqVectorExtras {
        match distance {
            DistanceType::Dot => TqVectorExtras {
                l2_length: Some(1.25),
            },
            DistanceType::Cosine => TqVectorExtras { l2_length: None },
            DistanceType::L1 | DistanceType::L2 => {
                unreachable!("unsupported distance in test")
            }
        }
    }

    /// For every supported (bits, mode, distance) combo, assert:
    ///   1. `pack_extras_into` writes exactly `size_for` bytes.
    ///   2. `unpack_extras_from` recovers the packed extras field-for-field.
    ///
    /// Unpacking uses a destructuring pattern so adding a new field to
    /// `TqVectorExtras` will force this test to be updated.
    #[test]
    fn extras_size_matches_pack_and_roundtrips_unpack() {
        let dim = 64;

        for &bits in SUPPORTED_BITS {
            for &mode in ALL_MODES {
                for &distance in SUPPORTED_DISTANCES {
                    let predicted_extra_size = TqVectorExtras::size_for(bits, distance, mode);

                    let tq = TurboQuantizer::new(dim, bits, mode, distance);
                    let expected = example_extras(distance);

                    let mut buf = Vec::new();
                    tq.pack_extras_into(&expected, &mut buf);
                    assert_eq!(
                        buf.len(),
                        predicted_extra_size,
                        "pack size mismatch (bits={bits:?}, mode={mode:?}, distance={distance:?})",
                    );

                    if predicted_extra_size == 0 {
                        continue;
                    }

                    let TqVectorExtras { l2_length } = tq.unpack_extras_from(&buf);
                    let TqVectorExtras {
                        l2_length: expected_l2,
                    } = expected;
                    assert_eq!(
                        l2_length, expected_l2,
                        "l2_length roundtrip (bits={bits:?}, mode={mode:?}, distance={distance:?})",
                    );
                }
            }
        }
    }
}
