use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Alignment for padded dimension.
const PADDED_DIM_ALIGNMENT: usize = 128;

pub(crate) struct HadamardRotation {
    signs1: Vec<f64>,
    signs2: Vec<f64>,
    padded_dim: usize,
    /// Power-of-2 chunk sizes for the decomposed WHT (largest first).
    /// E.g. for padded_dim=384: [256, 128].
    chunks: Vec<usize>,
    /// Whether a second (reversed) WHT pass is needed.
    /// True when padded_dim is not a power of 2.
    needs_reverse_pass: bool,
}

impl HadamardRotation {
    pub fn new(seed: u64, dim: usize) -> Self {
        let padded_dim = compute_padded_dim(dim);
        let (signs1, signs2) = generate_signs(seed, padded_dim);
        let chunks = decompose_into_powers_of_two(padded_dim);
        let needs_reverse_pass = !padded_dim.is_power_of_two();
        Self {
            signs1,
            signs2,
            padded_dim,
            chunks,
            needs_reverse_pass,
        }
    }

    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        let mut buf = vec![0.0f64; self.padded_dim];
        for (b, &v) in buf.iter_mut().zip(x.iter()) {
            *b = v as f64;
        }
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }
        apply_chunked_wht(&mut buf, &self.chunks);
        if self.needs_reverse_pass {
            buf.reverse();
            apply_chunked_wht(&mut buf, &self.chunks);
        }
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }
        buf.iter().map(|&v| v as f32).collect()
    }

    pub fn apply_inverse(&self, y: &[f32], original_dim: usize) -> Vec<f32> {
        let mut buf = vec![0.0f64; self.padded_dim];
        let len = y.len().min(self.padded_dim);
        for (b, &v) in buf.iter_mut().zip(y[..len].iter()) {
            *b = v as f64;
        }
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }
        if self.needs_reverse_pass {
            apply_chunked_wht(&mut buf, &self.chunks);
            buf.reverse();
        }
        apply_chunked_wht(&mut buf, &self.chunks);
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }
        buf[..original_dim].iter().map(|&v| v as f32).collect()
    }
}

/// Round up to the nearest multiple of `PADDED_DIM_ALIGNMENT`.
pub(crate) fn compute_padded_dim(dim: usize) -> usize {
    (dim + PADDED_DIM_ALIGNMENT - 1) / PADDED_DIM_ALIGNMENT * PADDED_DIM_ALIGNMENT
}

/// Decompose `n` into a sum of powers of 2, largest first.
/// E.g. 384 → [256, 128], 512 → [512], 640 → [512, 128].
fn decompose_into_powers_of_two(mut n: usize) -> Vec<usize> {
    let mut chunks = Vec::new();
    while n > 0 {
        let largest = 1 << (usize::BITS - 1 - n.leading_zeros());
        chunks.push(largest);
        n -= largest;
    }
    chunks
}

/// Apply WHT to consecutive power-of-2 chunks of `buf`.
fn apply_chunked_wht(buf: &mut [f64], chunks: &[usize]) {
    let mut offset = 0;
    for &chunk_size in chunks {
        walsh_hadamard_transform(&mut buf[offset..offset + chunk_size]);
        offset += chunk_size;
    }
}

/// Generate two random sign vectors (±1.0) from a deterministic seed.
fn generate_signs(seed: u64, padded_dim: usize) -> (Vec<f64>, Vec<f64>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let signs1: Vec<f64> = (0..padded_dim)
        .map(|_| if rng.random::<bool>() { 1.0f64 } else { -1.0f64 })
        .collect();
    let signs2: Vec<f64> = (0..padded_dim)
        .map(|_| if rng.random::<bool>() { 1.0f64 } else { -1.0f64 })
        .collect();
    (signs1, signs2)
}

/// In-place normalized Walsh-Hadamard Transform in f64. Self-inverse: WHT(WHT(x)) = x.
/// Input length must be a power of 2.
fn walsh_hadamard_transform(x: &mut [f64]) {
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
    let norm = (n as f64).sqrt();
    for val in x.iter_mut() {
        *val /= norm;
    }
}
