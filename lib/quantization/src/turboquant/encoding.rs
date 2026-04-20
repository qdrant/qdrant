use common::bitpacking::{BitReader, BitWriter};

use crate::DistanceType;
use crate::turboquant::quantization::TurboQuantizer;
use crate::turboquant::{TQBits, TQMode};

/// Additional data that needs to be stored along quantized vectors.
#[derive(Default, Copy, Clone)]
pub struct TqVectorExtras {
    // TODO(turbo): add all fields.
    // l2_length: Option<f32>,
}

impl TqVectorExtras {
    /// Returns the size (in bytes) required for the extras.
    pub(super) fn size_for(_bits: TQBits, _distance: DistanceType, _mode: TQMode) -> usize {
        // TODO(turbo): fully implement
        0
    }
}

impl TurboQuantizer {
    /// Bit-pack an iterator of centroid indices into a compact byte vector.
    pub(super) fn pack_vector<I>(&self, centroids: I, extras: TqVectorExtras) -> Vec<u8>
    where
        I: Iterator<Item = u8>,
    {
        let mut out = Vec::with_capacity(self.quantized_size());
        let mut bit_writer = BitWriter::new(&mut out);

        let bits = self.bits.bit_size();
        for item in centroids {
            bit_writer.write(item, bits);
        }
        bit_writer.finish();
        self.pack_extras_into(&extras, &mut out);
        out
    }

    /// Unpacks `vec` into `out` and returns `Extras`.
    ///
    /// `out` must have length equal to the quantizer's `dim`; one centroid
    /// value is written per slot.
    ///
    /// Does not apply the inverse rotation — `out` holds values in the
    /// rotated space.
    pub fn unpack_vector(&self, vec: &[u8], out: &mut [f64]) -> Option<TqVectorExtras> {
        let extra_len = TqVectorExtras::size_for(self.bits, self.distance, self.mode);

        let dim_part = &vec[..vec.len() - extra_len];
        let extra_part = &vec[vec.len() - extra_len..];

        debug_assert_eq!(out.len(), self.rotation.dim());

        // Unpack dimensions.
        let centroids = self.bits.get_centroids();
        let mut reader = BitReader::new(dim_part);
        reader.set_bits(self.bits.bit_size());
        for slot in out.iter_mut() {
            let idx: u8 = reader.read();
            *slot = f64::from(centroids[idx as usize]);
        }

        // Unpack and return extras if available.
        (!extra_part.is_empty()).then(|| self.unpack_extras_from(extra_part))
    }

    /// Size in bytes of a vector quantized by this quantizer.
    pub fn quantized_size(&self) -> usize {
        Self::quantized_size_for(self.rotation.dim(), self.bits, self.distance, self.mode)
    }

    /// Total size in bytes of a quantized vector, including both the packed
    /// dimensions and any extras..
    pub(super) fn quantized_size_for(
        dim: usize,
        bits: TQBits,
        distance: DistanceType,
        mode: TQMode,
    ) -> usize {
        Self::quantized_dim_size_for(dim, bits, distance)
            + TqVectorExtras::size_for(bits, distance, mode)
    }

    /// Size in bytes of the quantized dimensions alone (without extras) for a
    /// vector of `dim` dimensions under the given `bits` and `distance`.
    fn quantized_dim_size_for(dim: usize, bits: TQBits, distance: DistanceType) -> usize {
        Self::assert_supported_distance(distance);

        (dim * bits.bit_size() as usize).div_ceil(u8::BITS as usize)
    }

    /// Generates extra data that is required to store together with the quantized dimensions.
    /// This depends on the  metadata of the `TurboQuantizer`, such as distance-type or TQ-Mode.
    ///
    /// Not all configurations need extras.
    pub(super) fn calculate_extras(&self, _rotated_vec: &[f64]) -> TqVectorExtras {
        // TODO(turbo): fully implement
        Self::assert_supported_distance(self.distance);

        TqVectorExtras::default()
    }

    /// Decodes `src` containing encoded 'extra'-data and returns the decoded `Extras`.
    #[allow(clippy::unused_self)]
    fn unpack_extras_from(&self, _src: &[u8]) -> TqVectorExtras {
        // TODO(turbo): implement unpacking of extras.
        todo!()
    }

    /// Packs (encodes) the extras into the given buffer.
    #[allow(clippy::unused_self)]
    fn pack_extras_into(&self, _extras: &TqVectorExtras, _buf: &mut Vec<u8>) {
        // TODO(turbo): implement packing of extras.
    }

    /// TODO(turbo): remove once all distance have been implemented.
    fn assert_supported_distance(distance: DistanceType) {
        if !matches!(distance, DistanceType::Dot) {
            // TODO(turbo): implement for other metrics too.
            unimplemented!("Quantization currently only implemented for dot product");
        }
    }
}
