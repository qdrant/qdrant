//! Scalar matrix scoring for CPUs without an accelerated kernel, and the reference the
//! accelerated kernels are tested against. It computes what they compute, one pair at a time:
//! no blocking, no packing, no SIMD.

use common::types::PointOffsetType;

use crate::combine::{CombineDispatch as _, ScorePair};
use crate::operands::{KernelElem, MatrixOperands};
use crate::{Cols, TopK};

pub(crate) fn top_k<F: MatrixOperands>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    let combine = operands.combine();
    let score = |dot, aux_a, aux_b| combine.dispatch(ScorePair { dot, aux_a, aux_b });
    let rows = Decoded::new(operands, rows)?;
    match cols {
        // One triangle: the inner product is symmetric, so each pair is computed once and
        // pushed both ways, combined once per direction.
        Cols::Rows => {
            for r in 0..rows.len() {
                for c in r + 1..rows.len() {
                    let dot = F::Elem::dot(rows.vector(r), rows.vector(c));
                    out.push(r, c as u32, score(dot, rows.aux[r], rows.aux[c]));
                    out.push(c, r as u32, score(dot, rows.aux[c], rows.aux[r]));
                }
            }
        }
        Cols::Other(cols) => {
            let cols = Decoded::new(operands, cols)?;
            for r in 0..rows.len() {
                for c in 0..cols.len() {
                    let dot = F::Elem::dot(rows.vector(r), cols.vector(c));
                    out.push(r, c as u32, score(dot, rows.aux[r], cols.aux[c]));
                }
            }
        }
    }
    Ok(())
}

/// Decoded vectors of a list of points, `depth` elements each.
struct Decoded<E> {
    depth: usize,
    vectors: Vec<E>,
    aux: Vec<f32>,
}

impl<E: KernelElem> Decoded<E> {
    fn new<F: MatrixOperands<Elem = E>>(
        operands: &F,
        ids: &[PointOffsetType],
    ) -> Result<Self, F::Error> {
        let depth = operands.depth();
        let mut vectors = vec![E::default(); ids.len() * depth];
        let mut aux = vec![0.0; ids.len()];
        operands.decode_batch(ids, &mut vectors, &mut aux)?;
        Ok(Self {
            depth,
            vectors,
            aux,
        })
    }

    fn len(&self) -> usize {
        self.aux.len()
    }

    fn vector(&self, i: usize) -> &[E] {
        &self.vectors[i * self.depth..(i + 1) * self.depth]
    }
}
