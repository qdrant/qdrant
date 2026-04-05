mod hadamard;
mod matrix;

use super::TqRotation;

/// Default seed for generating the random rotation.
pub(super) const ROTATION_SEED: u64 = 42;

/// Runtime rotation state, selected by `TqRotation`.
pub(super) enum RotationImpl {
    None { dim: usize },
    Hadamard(hadamard::HadamardRotation),
    Matrix(matrix::MatrixRotation),
}

impl RotationImpl {
    pub fn new(rotation: TqRotation, seed: u64, dim: usize) -> Self {
        match rotation {
            TqRotation::NoRotation => RotationImpl::None { dim },
            TqRotation::Hadamard => {
                RotationImpl::Hadamard(hadamard::HadamardRotation::new(seed, dim))
            }
            TqRotation::RotationMatrix => {
                RotationImpl::Matrix(matrix::MatrixRotation::new(seed, dim))
            }
        }
    }

    /// Dimension of the rotated output (may be larger than input for Hadamard).
    pub fn padded_dim(&self) -> usize {
        match self {
            RotationImpl::None { dim } => *dim,
            RotationImpl::Hadamard(h) => h.padded_dim(),
            RotationImpl::Matrix(m) => m.padded_dim(),
        }
    }

    /// Forward rotation.
    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        match self {
            RotationImpl::None { .. } => x.to_vec(),
            RotationImpl::Hadamard(h) => h.apply(x),
            RotationImpl::Matrix(m) => m.apply(x),
        }
    }

    /// Inverse rotation, truncated to `original_dim`.
    pub fn apply_inverse(&self, y: &[f32], original_dim: usize) -> Vec<f32> {
        match self {
            RotationImpl::None { .. } => y[..original_dim].to_vec(),
            RotationImpl::Hadamard(h) => h.apply_inverse(y, original_dim),
            RotationImpl::Matrix(m) => m.apply_inverse(y, original_dim),
        }
    }

    /// Compute `padded_dim` for a given rotation type and dimension,
    /// without constructing the full rotation state.
    pub fn compute_padded_dim(rotation: TqRotation, dim: usize) -> usize {
        match rotation {
            TqRotation::NoRotation | TqRotation::RotationMatrix => dim,
            TqRotation::Hadamard => hadamard::compute_padded_dim(dim),
        }
    }
}
