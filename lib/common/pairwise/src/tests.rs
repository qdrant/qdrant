//! Every kernel the CPU supports against the fallback: the same picks with bitwise the same
//! scores. Shapes cover the block, panel, and depth-slice boundaries of the kernels, and one
//! `TopK` per `k` is reused across all calls of a test, so stale scratch buffers would show.

use std::collections::HashMap;
use std::convert::Infallible;

use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::{
    Aux, Bits, Codebook, Codes2, Codes4, Codes8, Cols, Combine, Kernel, KernelElem, MatrixOperands,
    TopK, TurboDot, top_k_with,
};

struct TestOperands<E> {
    depth: usize,
    vectors: Vec<E>,
    aux: Vec<Aux>,
    combine: Combine,
    /// For the codebook types
    values: Vec<i8>,
}

/// An element type with the parameter of its inner product in these tests.
trait TestElem: KernelElem {
    fn test_param(operands: &TestOperands<Self>) -> Self::Param<'_>;
}

impl TestElem for f32 {
    fn test_param(_: &TestOperands<Self>) {}
}

impl TestElem for u8 {
    fn test_param(_: &TestOperands<Self>) {}
}

impl TestElem for Bits {
    fn test_param(_: &TestOperands<Self>) {}
}

impl TestElem for Codes2 {
    fn test_param(operands: &TestOperands<Self>) -> Codebook {
        Codebook::new(&operands.values)
    }
}

impl TestElem for Codes4 {
    fn test_param(operands: &TestOperands<Self>) -> Codebook {
        Codebook::new(&operands.values)
    }
}

impl TestElem for Codes8 {
    fn test_param(_: &TestOperands<Self>) {}
}

impl<E: TestElem> MatrixOperands for TestOperands<E> {
    type Elem = E;
    type Error = Infallible;

    fn depth(&self) -> usize {
        self.depth
    }

    fn combine(&self) -> Combine {
        self.combine
    }

    fn param(&self) -> E::Param<'_> {
        E::test_param(self)
    }

    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [E],
        aux: &mut [Aux],
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

impl<E: TestElem> TestOperands<E> {
    fn new(
        rng: &mut StdRng,
        depth: usize,
        combine: Combine,
        element: impl Fn(&mut StdRng) -> E,
    ) -> Self {
        Self {
            depth,
            vectors: (0..POINTS * depth).map(|_| element(rng)).collect(),
            aux: (0..POINTS)
                .map(|_| std::array::from_fn(|_| rng.random_range(-2.0..2.0)))
                .collect(),
            combine,
            values: Vec::new(),
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
        Combine::Plain,
        Combine::Euclid,
        Combine::Cos,
        Combine::Turbo {
            dot: TurboDot::Scaled { div: 1.0 },
            l2: false,
            invert: false,
        },
        Combine::Turbo {
            dot: TurboDot::Scaled { div: 3.0 },
            l2: true,
            invert: true,
        },
        Combine::Turbo {
            dot: TurboDot::Corrected { div: 7.0, mm: 0.3 },
            l2: true,
            invert: false,
        },
        Combine::Turbo {
            dot: TurboDot::Corrected { div: 0.5, mm: -1.0 },
            l2: false,
            invert: true,
        },
    ];
    // Depth slices of 256
    for (i, depth) in [1, 7, 16, 255, 256, 257, 600].into_iter().enumerate() {
        let operands = TestOperands::new(&mut rng, depth, combines[i], |rng| {
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
    let combine = Combine::Affine {
        mult: 0.37,
        shift: 1.5,
    };
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
            0 => Combine::Hamming {
                dim: (bytes * 8) as f32,
                negate: i % 2 == 0,
            },
            // 1-bit TurboQuant
            _ => Combine::Turbo {
                dot: TurboDot::Signs {
                    bits: (bytes * 8) as f32,
                    centroid_sq: 0.6,
                },
                l2: i % 3 == 2,
                invert: i.is_multiple_of(2),
            },
        };
        let operands = TestOperands::new(&mut rng, bytes, combine, |rng| Bits(rng.random()));
        for (j, &shape) in shapes.iter().enumerate() {
            let k = [4, 10, 1][j % 3];
            operands.check(&mut rng, shape, k, &mut reused);
        }
    }
}

/// Codebook values with both extremes, the rest random.
fn with_codebook<E>(
    mut operands: TestOperands<E>,
    rng: &mut StdRng,
    values: usize,
) -> TestOperands<E> {
    operands.values = (0..values)
        .map(|q| match q {
            0 => -128,
            1 => 127,
            _ => rng.random(),
        })
        .collect();
    operands
}

/// [`GEMM_SHAPES`] with fewer pairs, for the slower scalar inner products: tiles of 4
/// rows, panels of 16 columns, row blocks of 48, and column blocks of 128.
const CODE_SHAPES: &[Shape] = &[
    Shape::Other(1, 1),
    Shape::Rows(2),
    Shape::Rows(49),
    Shape::Rows(131),
    Shape::Other(47, 300),
    Shape::Other(6, 140),
    Shape::Other(51, 17),
];

fn code_combine(i: usize) -> Combine {
    Combine::Turbo {
        dot: TurboDot::Scaled { div: 1.3 },
        l2: i.is_multiple_of(2),
        invert: i.is_multiple_of(3),
    }
}

/// The shapes with all kernels, at depths in bytes around the tiles' groups of two pair steps
/// and their depth `slice`.
fn check_codes<E: TestElem>(seed: u64, values: usize, slice: usize, element: fn(&mut StdRng) -> E) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut reused = HashMap::new();
    for (i, depth) in [1, 3, 8, slice - 1, slice, slice + 3]
        .into_iter()
        .enumerate()
    {
        let operands = TestOperands::new(&mut rng, depth, code_combine(i), element);
        let operands = with_codebook(operands, &mut rng, values);
        for (j, &shape) in CODE_SHAPES.iter().enumerate() {
            let k = [4, 1, 10][j % 3];
            operands.check(&mut rng, shape, k, &mut reused);
        }
    }
}

/// Mostly random codes, a quarter of the vectors all code 0: with the value −128, the largest
/// sums.
fn extreme_or<E>(rng: &mut StdRng, all_zero: E, random: impl Fn(&mut StdRng) -> E) -> E {
    if rng.random_bool(0.25) {
        all_zero
    } else {
        random(rng)
    }
}

#[test]
fn codes2_kernels_match_fallback() {
    check_codes(4, 4, 256, |rng| {
        extreme_or(rng, Codes2(0), |rng| Codes2(rng.random()))
    });
}

#[test]
fn codes4_kernels_match_fallback() {
    check_codes(5, 16, 512, |rng| {
        extreme_or(rng, Codes4(0), |rng| Codes4(rng.random()))
    });
}

#[test]
fn codes8_kernels_match_fallback() {
    // 0 is the value −128
    check_codes(6, 0, 1024, |rng| {
        extreme_or(rng, Codes8(0), |rng| Codes8(rng.random()))
    });
}

/// Vectors of the value −128 only, `codes` per element, with the most codes the i32 kernels
/// take and twice that: sums of 2³⁰ and 2³¹, which the kernels must leave to i64.
fn check_i32_limit<E: TestElem>(zero: E, codes: usize) {
    let mut rng = StdRng::seed_from_u64(7);
    let mut reused = HashMap::new();
    for positions in [1 << 16, 1 << 17] {
        let depth = positions / codes;
        let operands = TestOperands {
            depth,
            vectors: vec![zero; POINTS * depth],
            aux: vec![[1.0, 0.0, 0.0]; POINTS],
            combine: code_combine(1),
            values: vec![-128; 16],
        };
        operands.check(&mut rng, Shape::Other(2, 3), 2, &mut reused);
    }
}

#[test]
fn code_sums_near_the_i32_limit() {
    check_i32_limit(Codes2(0), 4);
    check_i32_limit(Codes4(0), 2);
    // 0 is the value −128
    check_i32_limit(Codes8(0), 1);
}
