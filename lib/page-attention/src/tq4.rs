/// Bits per coordinate.
pub const BITS: usize = 4;
/// Codebook size.
pub const LEVELS: usize = 16;
/// Hadamard rounds in the shared orthogonal transform.
pub const ROUNDS: usize = 3;

/// 16-level Lloyd-Max (MSE-optimal) quantiser for `N(0, 1)`, computed analytically as the
/// fixed point of the centroid condition on truncated-Gaussian cells (distortion 0.0094968).
///
/// The research code fitted the same quantiser on 2e6 samples
/// (`multistart/tq_cluster.py::lloyd_max`) and landed within 0.012 of these values; the analytic
/// table is used because it is exactly reproducible in both languages.
// The literals are the decimal expansions the Python side prints for the same f32 values, kept
// verbatim so the two tables can be compared by eye as well as by the fixture test.
#[allow(clippy::excessive_precision)]
pub const CODEBOOK4: [f32; LEVELS] = [
    -2.7325894832611084,
    -2.069017171859741,
    -1.6180464029312134,
    -1.2562311887741089,
    -0.9423404335975647,
    -0.6567591428756714,
    -0.38804829120635986,
    -0.12839503586292267,
    0.12839503586292267,
    0.38804829120635986,
    0.6567591428756714,
    0.9423404335975647,
    1.2562311887741089,
    1.6180464029312134,
    2.069017171859741,
    2.7325894832611084,
];

/// Decision thresholds (midpoints of neighbouring levels): `code = thresholds.partition_point(<= z)`.
#[inline]
pub fn thresholds() -> [f32; LEVELS - 1] {
    let mut t = [0.0f32; LEVELS - 1];
    for i in 0..LEVELS - 1 {
        t[i] = (CODEBOOK4[i] + CODEBOOK4[i + 1]) / 2.0;
    }
    t
}

/// Quantise one standardised coordinate to a 4-bit code (nearest codebook level).
#[inline]
pub fn quantise(z: f32) -> u8 {
    let t = thresholds();
    let mut code = 0u8;
    // 15 thresholds: a branchless-ish linear scan is faster than a binary search here.
    for &e in t.iter() {
        code += u8::from(z >= e);
    }
    code
}

// ---------------------------------------------------------------------------------------------
// the shared orthogonal transform
// ---------------------------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// The FNV-1a-64 offset basis: the initial state of an incremental hash.
pub const FNV_INIT: u64 = FNV_OFFSET;

/// FNV-1a-64 over bytes.
pub fn fnv1a64(data: &[u8]) -> u64 {
    fnv1a64_update(FNV_INIT, data)
}

/// One more chunk into an FNV-1a-64 hash, so a file can be checksummed as it streams past
/// (`crate::persist` hashes every payload file on the way in and on the way out).
pub fn fnv1a64_update(mut h: u64, data: &[u8]) -> u64 {
    for &b in data {
        h = (h ^ b as u64).wrapping_mul(FNV_PRIME);
    }
    h
}

/// The documented per-session rotation seed: FNV-1a-64 of the session id.
///
/// `SessionInfo` has no seed field, so the client derives the very same seed from the session id
/// it already knows (`llm/kvstore_client/tq4.py::rotation_seed`).
pub fn rotation_seed(session_id: &str) -> u64 {
    fnv1a64(session_id.as_bytes())
}

/// The session's fixed orthogonal transform `R`, as a randomised Hadamard product
/// `R = D_2 H D_1 H D_0 H / sqrt(d)^3`.
///
/// `H` is the Sylvester-Hadamard matrix of order `d` and `D_r` are sign diagonals from
/// `splitmix64(seed)` (one 64-bit word per coordinate per round, sign `-1` when the top bit is
/// set). Every intermediate value is a small integer (`|entry| <= d^2`), so the matrix is computed
/// exactly and the single final scaling is correctly rounded — which is what makes the Rust and
/// Python matrices **bit identical**. Three rounds is the usual mixing depth; measured on
/// `llama31-8b-L04-H1` it is indistinguishable from the research's QR-of-a-Gaussian rotation
/// (4-bit quantised recall@128 0.913 vs 0.915, round-trip error 0.0494 vs 0.0495) while being
/// orthogonal to 2e-16 instead of 2e-8 and O(d log d) to apply.
pub struct Rotation {
    pub dim: usize,
    pub seed: u64,
    /// Sign diagonals, `ROUNDS * dim`.
    signs: Vec<f32>,
    /// Row-major `dim * dim` matrix (rows = axes), also used by the fixture test.
    matrix: Vec<f32>,
}

impl Rotation {
    /// Build the transform for `dim` (a power of two).
    pub fn new(seed: u64, dim: usize) -> Rotation {
        assert!(
            dim > 0 && dim.is_power_of_two(),
            "head_dim must be a power of two"
        );
        let mut rng = crate::index::SplitMix64::new(seed);
        let mut signs = vec![0.0f32; ROUNDS * dim];
        for r in 0..ROUNDS {
            for i in 0..dim {
                signs[r * dim + i] = if rng.next_u64() >> 63 == 1 { -1.0 } else { 1.0 };
            }
        }
        let matrix = Rotation::build_matrix_f64(&signs, dim)
            .iter()
            .map(|&v| v as f32)
            .collect();
        Rotation {
            dim,
            seed,
            signs,
            matrix,
        }
    }

    /// The transform of a session (seed derived from the session id).
    pub fn for_session(session_id: &str, dim: usize) -> Rotation {
        Rotation::new(rotation_seed(session_id), dim)
    }

    /// Row-major `dim x dim` matrix in f64, computed EXACTLY (integer intermediates) and scaled
    /// by one correctly-rounded multiply — the numbers the Python client reproduces bit for bit.
    fn build_matrix_f64(signs: &[f32], dim: usize) -> Vec<f64> {
        // acc = I, then acc <- D_r H acc for every round; all intermediates stay integral.
        let mut acc = vec![0.0f64; dim * dim];
        for i in 0..dim {
            acc[i * dim + i] = 1.0;
        }
        let mut tmp = vec![0.0f64; dim * dim];
        for r in 0..ROUNDS {
            // fast Walsh-Hadamard along the row axis (only additions of exact integers)
            let mut h = 1usize;
            while h < dim {
                let mut start = 0usize;
                while start < dim {
                    for i in start..start + h {
                        for c in 0..dim {
                            let (x, y) = (acc[i * dim + c], acc[(i + h) * dim + c]);
                            tmp[i * dim + c] = x + y;
                            tmp[(i + h) * dim + c] = x - y;
                        }
                    }
                    start += 2 * h;
                }
                acc.copy_from_slice(&tmp);
                h *= 2;
            }
            for i in 0..dim {
                if signs[r * dim + i] < 0.0 {
                    for c in 0..dim {
                        acc[i * dim + c] = -acc[i * dim + c];
                    }
                }
            }
        }
        let s = (dim as f64).sqrt();
        let mut scale = 1.0f64;
        for _ in 0..ROUNDS {
            scale /= s;
        }
        for v in acc.iter_mut() {
            *v *= scale;
        }
        acc
    }

    /// The f64 matrix in the fixture's convention (bit-comparable with numpy).
    pub fn matrix_f64(&self) -> Vec<f64> {
        Rotation::build_matrix_f64(&self.signs, self.dim)
    }

    /// Row `i` of the matrix (the `i`-th rotation axis).
    pub fn row(&self, i: usize) -> &[f32] {
        &self.matrix[i * self.dim..(i + 1) * self.dim]
    }

    /// `out = R x` — the fast path: three Hadamard rounds and sign flips, `O(d log d)`.
    pub fn apply(&self, x: &[f32], out: &mut [f32]) {
        let dim = self.dim;
        debug_assert_eq!(x.len(), dim);
        out[..dim].copy_from_slice(x);
        let scale = 1.0f32 / (dim as f32).sqrt();
        for r in 0..ROUNDS {
            wht(&mut out[..dim]);
            let s = &self.signs[r * dim..(r + 1) * dim];
            for i in 0..dim {
                out[i] = out[i] * s[i] * scale;
            }
        }
    }

    /// `out = R^T u` — the transpose (the decoder's direction), same cost.
    pub fn apply_t(&self, u: &[f32], out: &mut [f32]) {
        let dim = self.dim;
        debug_assert_eq!(u.len(), dim);
        out[..dim].copy_from_slice(u);
        let scale = 1.0f32 / (dim as f32).sqrt();
        for r in (0..ROUNDS).rev() {
            let s = &self.signs[r * dim..(r + 1) * dim];
            for i in 0..dim {
                out[i] = out[i] * s[i] * scale;
            }
            wht(&mut out[..dim]);
        }
    }
}

/// In-place unnormalised fast Walsh-Hadamard transform (`len` a power of two).
fn wht(a: &mut [f32]) {
    let n = a.len();
    let mut h = 1usize;
    while h < n {
        let mut start = 0usize;
        while start < n {
            for i in start..start + h {
                let (x, y) = (a[i], a[i + h]);
                a[i] = x + y;
                a[i + h] = x - y;
            }
            start += 2 * h;
        }
        h *= 2;
    }
}
