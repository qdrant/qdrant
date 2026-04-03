use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

pub(crate) struct MatrixRotation {
    /// Row-major orthogonal matrix of size dim × dim.
    matrix: Vec<f32>,
    dim: usize,
}

impl MatrixRotation {
    pub fn new(seed: u64, dim: usize) -> Self {
        let matrix = generate_orthogonal_matrix(seed, dim);
        Self { matrix, dim }
    }

    pub fn padded_dim(&self) -> usize {
        self.dim
    }

    /// y = Q * x
    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        let mut y = vec![0.0f32; self.dim];
        for i in 0..self.dim {
            let row = &self.matrix[i * self.dim..(i + 1) * self.dim];
            let mut sum = 0.0f32;
            for j in 0..x.len().min(self.dim) {
                sum += row[j] * x[j];
            }
            y[i] = sum;
        }
        y
    }

    /// x = Q^T * y (Q is orthogonal, so Q^-1 = Q^T)
    pub fn apply_inverse(&self, y: &[f32], original_dim: usize) -> Vec<f32> {
        let mut x = vec![0.0f32; original_dim];
        for j in 0..original_dim.min(self.dim) {
            let mut sum = 0.0f32;
            for i in 0..y.len().min(self.dim) {
                sum += self.matrix[i * self.dim + j] * y[i];
            }
            x[j] = sum;
        }
        x
    }
}

/// Generate a random orthogonal matrix using QR decomposition of a random
/// Gaussian matrix via modified Gram-Schmidt.
fn generate_orthogonal_matrix(seed: u64, dim: usize) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate random Gaussian matrix (row-major, dim × dim)
    let mut matrix = vec![0.0f32; dim * dim];
    for val in matrix.iter_mut() {
        *val = random_normal(&mut rng);
    }

    // Modified Gram-Schmidt orthogonalization (column-wise)
    for i in 0..dim {
        // Normalize column i
        let norm = column_norm(&matrix, dim, i);
        if norm > 1e-10 {
            for row in 0..dim {
                matrix[row * dim + i] /= norm;
            }
        }

        // Subtract projection of column i from all subsequent columns
        for j in (i + 1)..dim {
            let dot = column_dot(&matrix, dim, i, j);
            for row in 0..dim {
                matrix[row * dim + j] -= dot * matrix[row * dim + i];
            }
        }
    }

    matrix
}

/// Dot product of columns `a` and `b` in a row-major matrix.
fn column_dot(matrix: &[f32], dim: usize, a: usize, b: usize) -> f32 {
    let mut sum = 0.0f32;
    for row in 0..dim {
        sum += matrix[row * dim + a] * matrix[row * dim + b];
    }
    sum
}

/// L2 norm of column `col` in a row-major matrix.
fn column_norm(matrix: &[f32], dim: usize, col: usize) -> f32 {
    column_dot(matrix, dim, col, col).sqrt()
}

/// Generate a standard normal random number using Box-Muller transform.
fn random_normal(rng: &mut StdRng) -> f32 {
    loop {
        let u1: f32 = rng.random();
        let u2: f32 = rng.random();
        if u1 > 1e-10 {
            return (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
        }
    }
}
