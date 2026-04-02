use nalgebra::DMatrix;
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rand_distr::StandardNormal;

/// Quantized Johnson-Lindenstrauss transform.
/// Reduces vectors to sign bits while preserving inner products in expectation.
pub struct Qjl {
    pub projection: DMatrix<f32>,
    pub d: usize,
}

impl Qjl {
    /// Create a new QJL transform for dimension d.
    pub fn new(d: usize, seed: u64) -> Self {
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        let data: Vec<f32> = (0..d * d).map(|_| rng.sample(StandardNormal)).collect();
        let projection = DMatrix::from_vec(d, d, data);
        Qjl { projection, d }
    }

    /// Quantize: signs = sign(S * r). Zero values map to +1.
    pub fn quantize(&self, r: &[f32]) -> Vec<u8> {
        let r_vec = nalgebra::DVector::from_column_slice(r);
        let z = &self.projection * r_vec;
        z.iter().map(|&v| if v >= 0.0 { 1u8 } else { 0 }).collect()
    }

    /// Dequantize: Q_qjl^{-1}(z) = (sqrt(pi/2) / d) * S^T * z
    pub fn dequantize(&self, signs: &[u8]) -> Vec<f32> {
        let scale = (std::f32::consts::PI / 2.0).sqrt() / self.d as f32;
        let z =
            nalgebra::DVector::from_iterator(self.d, signs.iter().map(|&s| s as f32 * 2.0 - 1.0));
        let result = scale * (self.projection.transpose() * z);
        result.as_slice().to_vec()
    }
}
