use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Chunk size for chunked Walsh-Hadamard Transform. Must be a power of 2.
const WHT_CHUNK_SIZE: usize = 256;

pub(crate) struct HadamardRotation {
    signs1: Vec<f32>,
    signs2: Vec<f32>,
    padded_dim: usize,
}

impl HadamardRotation {
    pub fn new(seed: u64, dim: usize) -> Self {
        let padded_dim = compute_padded_dim(dim);
        let (signs1, signs2) = generate_signs(seed, padded_dim);
        Self {
            signs1,
            signs2,
            padded_dim,
        }
    }

    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        let mut buf = vec![0.0f32; self.padded_dim];
        buf[..x.len()].copy_from_slice(x);
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }
        for chunk in buf.chunks_mut(WHT_CHUNK_SIZE) {
            walsh_hadamard_transform(chunk);
        }
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }
        buf
    }

    pub fn apply_inverse(&self, y: &[f32], original_dim: usize) -> Vec<f32> {
        let mut buf = vec![0.0f32; self.padded_dim];
        let len = y.len().min(self.padded_dim);
        buf[..len].copy_from_slice(&y[..len]);
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }
        for chunk in buf.chunks_mut(WHT_CHUNK_SIZE) {
            walsh_hadamard_transform(chunk);
        }
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }
        buf[..original_dim].to_vec()
    }
}

/// Round up to the nearest multiple of `WHT_CHUNK_SIZE`.
pub(crate) fn compute_padded_dim(dim: usize) -> usize {
    (dim + WHT_CHUNK_SIZE - 1) / WHT_CHUNK_SIZE * WHT_CHUNK_SIZE
}

/// Generate two random sign vectors (±1.0) from a deterministic seed.
fn generate_signs(seed: u64, padded_dim: usize) -> (Vec<f32>, Vec<f32>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let signs1: Vec<f32> = (0..padded_dim)
        .map(|_| if rng.random::<bool>() { 1.0f32 } else { -1.0f32 })
        .collect();
    let signs2: Vec<f32> = (0..padded_dim)
        .map(|_| if rng.random::<bool>() { 1.0f32 } else { -1.0f32 })
        .collect();
    (signs1, signs2)
}

/// In-place normalized Walsh-Hadamard Transform. Self-inverse: WHT(WHT(x)) = x.
/// Input length must be a power of 2.
fn walsh_hadamard_transform(x: &mut [f32]) {
    let n = x.len();
    debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");
    let mut h = 1;
    while h < n {
        for i in (0..n).step_by(h * 2) {
            for j in i..i + h {
                let a = x[j];
                let b = x[j + h];
                x[j] = a + b;
                x[j + h] = a - b;
            }
        }
        h *= 2;
    }
    let norm = (n as f32).sqrt();
    for val in x.iter_mut() {
        *val /= norm;
    }
}
