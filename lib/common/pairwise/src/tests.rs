//! Every kernel the CPU supports against the fallback: the same picks with bitwise the same
//! scores. Shapes cover the block, panel, and depth-slice boundaries of the kernels, and one
//! `TopK` per `k` is reused across all calls of a test, so stale scratch buffers would show.

use std::collections::HashMap;
use std::convert::Infallible;

use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::{
    Bits, Cols, Combine, CombineDispatch, CombineFn, Kernel, KernelElem, MatrixOperands, TopK,
    top_k_with,
};

/// Combining steps chosen at runtime, as a storage chooses by distance. `Affine` reads its
/// values asymmetrically, so values packed with the wrong vector, or a transposed pair combined
/// the wrong way round, change the scores.
#[derive(Clone, Copy, Debug)]
enum TestCombine {
    Plain,
    Affine(Affine),
}

/// `dot`
#[derive(Clone, Copy, Debug)]
struct Plain;

/// `mult · dot + (a − shift) + b`, as scalar quantization
#[derive(Clone, Copy, Debug)]
struct Affine {
    mult: f32,
    shift: f32,
}

impl Combine for Plain {
    #[inline(always)]
    fn score(self, dot: f32, _: f32, _: f32) -> f32 {
        dot
    }
}

impl Combine for Affine {
    #[inline(always)]
    fn score(self, dot: f32, a: f32, b: f32) -> f32 {
        self.mult * dot + (a - self.shift) + b
    }
}

impl CombineDispatch for TestCombine {
    #[inline(always)]
    fn dispatch<F: CombineFn>(self, f: F) -> F::Output {
        match self {
            TestCombine::Plain => f.call(Plain),
            TestCombine::Affine(affine) => f.call(affine),
        }
    }
}

struct TestOperands<E> {
    depth: usize,
    vectors: Vec<E>,
    aux: Vec<f32>,
    combine: TestCombine,
}

impl<E: KernelElem> MatrixOperands for TestOperands<E> {
    type Elem = E;
    type Combine = TestCombine;
    type Error = Infallible;

    fn depth(&self) -> usize {
        self.depth
    }

    fn combine(&self) -> TestCombine {
        self.combine
    }

    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [E],
        aux: &mut [f32],
    ) -> Result<(), Infallible> {
        let depth = self.depth;
        for (i, &id) in ids.iter().enumerate() {
            let id = id as usize;
            out[i * depth..(i + 1) * depth]
                .copy_from_slice(&self.vectors[id * depth..(id + 1) * depth]);
            aux[i] = self.aux[id];
        }
        Ok(())
    }
}

/// Points for a call: `rows` distinct points, and for [`Shape::Other`] that many columns drawn
/// with repeats, partly from the rows.
#[derive(Clone, Copy, Debug)]
enum Shape {
    Rows(usize),
    Other(usize, usize),
}

const POINTS: usize = 1500;

impl<E: KernelElem> TestOperands<E> {
    fn new(
        rng: &mut StdRng,
        depth: usize,
        combine: TestCombine,
        element: impl Fn(&mut StdRng) -> E,
    ) -> Self {
        Self {
            depth,
            vectors: (0..POINTS * depth).map(|_| element(rng)).collect(),
            aux: (0..POINTS).map(|_| rng.random_range(-2.0..2.0)).collect(),
            combine,
        }
    }

    fn check(&self, rng: &mut StdRng, shape: Shape, k: usize, reused: &mut HashMap<usize, TopK>) {
        let (rows, cols) = match shape {
            Shape::Rows(m) => (distinct_points(rng, m), None),
            Shape::Other(m, n) => {
                let rows = distinct_points(rng, m);
                let cols: Vec<_> = (0..n)
                    .map(|_| {
                        if rng.random_bool(0.1) {
                            rows[rng.random_range(0..m)]
                        } else {
                            rng.random_range(0..POINTS as PointOffsetType)
                        }
                    })
                    .collect();
                (rows, Some(cols))
            }
        };
        let cols = cols.as_deref().map_or(Cols::Rows, Cols::Other);

        let mut expected = TopK::new(k);
        top_k_with(Kernel::Fallback, self, &rows, cols, &mut expected).unwrap();
        let actual = reused.entry(k).or_insert_with(|| TopK::new(k));
        for kernel in Kernel::available() {
            top_k_with(kernel, self, &rows, cols, actual).unwrap();
            assert_eq!(actual.rows(), rows.len());
            for r in 0..rows.len() {
                let (want_cols, want_scores) = expected.row(r);
                let (got_cols, got_scores) = actual.row(r);
                let bits = |s: &[f32]| s.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
                assert_eq!(
                    (got_cols, bits(got_scores)),
                    (want_cols, bits(want_scores)),
                    "{kernel:?}, {shape:?}, depth {}, k {k}, {:?}, row {r}: scores {got_scores:?}, \
                     expected {want_scores:?}",
                    self.depth,
                    self.combine,
                );
            }
        }
    }
}

fn distinct_points(rng: &mut StdRng, count: usize) -> Vec<PointOffsetType> {
    let mut all: Vec<PointOffsetType> = (0..POINTS as PointOffsetType).collect();
    for i in 0..count {
        let j = rng.random_range(i..all.len());
        all.swap(i, j);
    }
    all.truncate(count);
    all
}

/// Shapes around the f32 and u8 kernels' boundaries: tiles of 4, 6, and 8 rows, panels of 12
/// and 16 columns, row blocks of 48, and column blocks of 128 and 132.
const GEMM_SHAPES: &[Shape] = &[
    Shape::Other(1, 1),
    Shape::Rows(2),
    Shape::Rows(49),
    Shape::Rows(130),
    Shape::Rows(600),
    Shape::Other(47, 513),
    Shape::Other(6, 1100),
    Shape::Other(100, 17),
    Shape::Other(49, 700),
];

#[test]
fn f32_kernels_match_fallback() {
    let mut rng = StdRng::seed_from_u64(1);
    let mut reused = HashMap::new();
    let combines = [
        TestCombine::Plain,
        TestCombine::Affine(Affine {
            mult: 2.0,
            shift: 0.5,
        }),
        TestCombine::Affine(Affine {
            mult: -1.0,
            shift: -0.25,
        }),
    ];
    // Depth slices of 256
    for (i, depth) in [1, 7, 16, 255, 256, 257, 600].into_iter().enumerate() {
        let operands = TestOperands::new(&mut rng, depth, combines[i % 3], |rng| {
            rng.random_range(-1.0f32..1.0)
        });
        for (j, &shape) in GEMM_SHAPES.iter().enumerate() {
            let k = [1, 4, 10, 2000][j % 4];
            operands.check(&mut rng, shape, k, &mut reused);
        }
    }
}

#[test]
fn u8_kernels_match_fallback() {
    let mut rng = StdRng::seed_from_u64(2);
    let mut reused = HashMap::new();
    let combine = TestCombine::Affine(Affine {
        mult: 0.37,
        shift: 1.5,
    });
    // Groups of 4, and depth slices of 1024
    for depth in [1, 3, 4, 5, 64, 257, 1100] {
        // Few distinct values give many ties
        let max = if depth % 2 == 0 { 127 } else { 2 };
        let operands = TestOperands::new(&mut rng, depth, combine, |rng| rng.random_range(0..=max));
        for (j, &shape) in GEMM_SHAPES.iter().enumerate() {
            let k = [4, 1, 10][j % 3];
            operands.check(&mut rng, shape, k, &mut reused);
        }
    }
}

#[test]
fn bits_kernels_match_fallback() {
    let mut rng = StdRng::seed_from_u64(3);
    let mut reused = HashMap::new();
    // Row blocks of 12, column groups of 64, and nibble blocks of 63 (x86); tiles of 4 x 4,
    // depth padded to 16 bytes, and depth slices of 496 bytes (NEON)
    let shapes = [
        Shape::Other(1, 1),
        Shape::Rows(13),
        Shape::Rows(130),
        Shape::Other(11, 65),
        Shape::Other(100, 1000),
        Shape::Other(25, 7),
    ];
    for (i, bytes) in [1, 7, 31, 32, 33, 100, 500].into_iter().enumerate() {
        let combine = match i % 3 {
            0 => TestCombine::Plain,
            _ => TestCombine::Affine(Affine {
                mult: if i % 2 == 0 { -2.0 } else { 2.0 },
                shift: (bytes * 8) as f32,
            }),
        };
        let operands = TestOperands::new(&mut rng, bytes, combine, |rng| Bits(rng.random()));
        for (j, &shape) in shapes.iter().enumerate() {
            let k = [4, 10, 1][j % 3];
            operands.check(&mut rng, shape, k, &mut reused);
        }
    }
}
