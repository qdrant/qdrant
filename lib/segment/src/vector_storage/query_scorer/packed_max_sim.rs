//! Batched MaxSim for dense multi-vectors.
//!
//! [`score_max_similarity`](super::score_max_similarity) computed each of the
//! `query x document` token pairs with its own similarity call: one dispatch,
//! one horizontal sum and one pass over both vectors per pair. Here the query
//! is packed once, transposed so that SIMD lanes hold query tokens:
//!
//! ```text
//! floats[(block * dim + k) * BLOCK + lane]           = query[block * BLOCK + lane][k]
//! pairs[((block * pairs + p) * BLOCK + lane) * 2 + i] = query[block * BLOCK + lane][2 * p + i]
//! ```
//!
//! `f32` and `f16` queries are packed as floats: each document element is
//! broadcast into a tile of `R` document tokens x [`BLOCK`] query tokens, and
//! the tile goes straight into the per-token running maxima. `u8` queries are
//! packed as pairs of `i16` and folded in with `madd`, exactly in integers.
//! There is no horizontal sum per pair, and every document element is loaded
//! once per [`BLOCK`] query tokens instead of once per query token.
//!
//! `u8` scores are the same as pair by pair; float scores match up to the
//! summation order within each similarity.

use std::any::TypeId;
use std::cell::RefCell;

use common::types::ScoreType;
use half::f16;
use quantization::packed_multi_query::crossover::{self, Crossover};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    TypedMultiDenseVectorRef, VectorElementType, VectorElementTypeByte, VectorElementTypeHalf,
};
use crate::types::Distance;

/// Query tokens per packed block: two AVX registers of 8 lanes.
const BLOCK: usize = 16;

/// Largest `u8` dimension whose sums of products fit an `i32`, like the
/// per-pair kernels' accumulators.
const MAX_BYTE_DIM: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Element {
    Float,
    Half,
    Byte,
}

impl Element {
    fn of<T: 'static>() -> Option<Self> {
        let id = TypeId::of::<T>();
        if id == TypeId::of::<VectorElementType>() {
            Some(Element::Float)
        } else if id == TypeId::of::<VectorElementTypeHalf>() {
            Some(Element::Half)
        } else if id == TypeId::of::<VectorElementTypeByte>() {
            Some(Element::Byte)
        } else {
            None
        }
    }
}

/// What a lane accumulates over the dims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    /// `Σ q · x`: Dot, and Cosine of floats (normalized on the way in).
    Dot,
    /// `−Σ (q − x)²`.
    Euclid,
    /// `−Σ |q − x|`.
    Manhattan,
    /// `Σ q · x / sqrt(Σ q² · Σ x²)`, Cosine of bytes.
    Cosine,
}

impl Op {
    fn new(element: Element, distance: Distance) -> Self {
        match (distance, element) {
            (Distance::Cosine, Element::Byte) => Op::Cosine,
            (Distance::Cosine | Distance::Dot, _) => Op::Dot,
            (Distance::Euclid, _) => Op::Euclid,
            (Distance::Manhattan, _) => Op::Manhattan,
        }
    }
}

#[derive(Debug)]
pub struct PackedMultiQuery {
    element: Element,
    op: Op,
    vectors_count: usize,
    dim: usize,
    /// Float and half queries, see the module docs.
    floats: Vec<f32>,
    /// Byte queries, see the module docs.
    pairs: Vec<i16>,
    /// Byte Cosine: `Σ q²` per lane.
    norms: Vec<i32>,
}

/// Fewest vectors worth packing as a query, or (`stored`) per call, measured
/// like the quantized ones in [`crossover`]; 8 at least, the smallest query
/// packed before. Float Cosine is Dot of normalized vectors.
fn crossover(element: Element, op: Op, stored: bool) -> Crossover {
    match (element, op, stored) {
        (Element::Float, Op::Dot, false) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(8)),
            (1024, Some(8)),
        ],
        (Element::Float, Op::Dot, true) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(8)),
            (1024, Some(10)),
        ],
        (Element::Float, Op::Euclid, false) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(8)),
            (1024, Some(8)),
        ],
        (Element::Float, Op::Euclid, true) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(8)),
            (1024, Some(8)),
        ],
        (Element::Float, Op::Manhattan, false) => &[
            (128, Some(12)),
            (384, Some(12)),
            (768, Some(12)),
            (1024, Some(12)),
        ],
        (Element::Float, Op::Manhattan, true) => &[
            (128, Some(8)),
            (384, Some(16)),
            (768, Some(16)),
            (1024, Some(16)),
        ],
        (Element::Half, Op::Dot, false) => &[
            (128, Some(8)),
            (384, Some(10)),
            (768, Some(12)),
            (1024, Some(16)),
        ],
        (Element::Half, Op::Dot, true) => &[
            (128, Some(8)),
            (384, Some(16)),
            (768, Some(32)),
            (1024, Some(32)),
        ],
        (Element::Half, Op::Euclid, false) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(8)),
            (1024, Some(10)),
        ],
        (Element::Half, Op::Euclid, true) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(16)),
            (1024, Some(16)),
        ],
        (Element::Half, Op::Manhattan, false) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(12)),
            (1024, Some(12)),
        ],
        (Element::Half, Op::Manhattan, true) => &[
            (128, Some(8)),
            (384, Some(12)),
            (768, Some(16)),
            (1024, Some(16)),
        ],
        (Element::Byte, Op::Dot, false) => &[
            (128, Some(8)),
            (384, Some(12)),
            (768, Some(32)),
            (1024, Some(32)),
        ],
        (Element::Byte, Op::Dot, true) => &[
            (128, Some(12)),
            (384, Some(32)),
            (768, Some(64)),
            (1024, None),
        ],
        (Element::Byte, Op::Cosine, false) => &[
            (128, Some(8)),
            (384, Some(8)),
            (768, Some(8)),
            (1024, Some(12)),
        ],
        (Element::Byte, Op::Cosine, true) => &[
            (128, Some(8)),
            (384, Some(10)),
            (768, Some(10)),
            (1024, Some(12)),
        ],
        (Element::Byte, Op::Euclid, false) => &[
            (128, Some(8)),
            (384, Some(12)),
            (768, Some(16)),
            (1024, Some(32)),
        ],
        (Element::Byte, Op::Euclid, true) => {
            &[(128, Some(12)), (384, Some(16)), (768, None), (1024, None)]
        }
        (Element::Byte, Op::Manhattan, false) => {
            &[(128, Some(16)), (384, None), (768, None), (1024, None)]
        }
        (Element::Byte, Op::Manhattan, true) => {
            &[(128, None), (384, None), (768, None), (1024, None)]
        }
        // Float Cosine packs as Dot.
        (Element::Float | Element::Half, Op::Cosine, _) => &[],
    }
}

/// Whether a multi-vector of `vectors_count` vectors of `dim` dims, packed
/// once as a query or (`stored`) per call by [`score_max_similarity_packed`],
/// scores faster than pair by pair on this CPU.
pub fn pays<T: PrimitiveVectorElement>(
    distance: Distance,
    dim: usize,
    vectors_count: usize,
    stored: bool,
) -> bool {
    Element::of::<T>().is_some_and(|element| {
        let op = Op::new(element, distance);
        crossover::pays(crossover(element, op, stored), dim, vectors_count)
    })
}

impl PackedMultiQuery {
    /// `None` when there is no batched kernel for `T` and this query; callers
    /// then score pair by pair.
    pub fn new<T: PrimitiveVectorElement>(
        query: TypedMultiDenseVectorRef<T>,
        distance: Distance,
    ) -> Option<Self> {
        let mut packed = Self::empty(Element::of::<T>()?, distance);
        packed.pack(query).then_some(packed)
    }

    /// [`Self::new`] when it is faster than scoring pair by pair.
    pub fn new_if_faster<T: PrimitiveVectorElement>(
        query: TypedMultiDenseVectorRef<T>,
        distance: Distance,
    ) -> Option<Self> {
        if !pays::<T>(distance, query.dim, query.vectors_count(), false) {
            return None;
        }
        Self::new(query, distance)
    }

    fn empty(element: Element, distance: Distance) -> Self {
        Self {
            element,
            op: Op::new(element, distance),
            vectors_count: 0,
            dim: 0,
            floats: Vec::new(),
            pairs: Vec::new(),
            norms: Vec::new(),
        }
    }

    /// Re-pack `query` into this buffer, keeping the allocation; `false` when
    /// there is no batched kernel for it.
    fn pack<T: PrimitiveVectorElement>(&mut self, query: TypedMultiDenseVectorRef<T>) -> bool {
        debug_assert_eq!(Element::of::<T>(), Some(self.element));
        let dim = query.dim;
        let vectors_count = query.vectors_count();
        if vectors_count == 0 || dim == 0 || (self.element == Element::Byte && dim > MAX_BYTE_DIM) {
            return false;
        }
        self.vectors_count = vectors_count;
        self.dim = dim;
        let lanes = vectors_count.div_ceil(BLOCK) * BLOCK;
        match self.element {
            Element::Float | Element::Half => {
                self.floats.clear();
                self.floats.resize(lanes * dim, 0.0);
                for (token, vector) in query.multi_vectors().enumerate() {
                    let base = (token / BLOCK) * dim * BLOCK + token % BLOCK;
                    for (k, value) in as_floats(vector).enumerate() {
                        self.floats[base + k * BLOCK] = value;
                    }
                }
            }
            Element::Byte => {
                let pairs = dim.div_ceil(2);
                self.pairs.clear();
                self.pairs.resize(lanes * pairs * 2, 0);
                self.norms.clear();
                self.norms.resize(lanes, 0);
                for (token, vector) in query.multi_vectors().enumerate() {
                    let vector: &[u8] = bytemuck::cast_slice(vector);
                    let (block, lane) = (token / BLOCK, token % BLOCK);
                    for (k, &value) in vector.iter().enumerate() {
                        let at = ((block * pairs + k / 2) * BLOCK + lane) * 2 + k % 2;
                        self.pairs[at] = i16::from(value);
                    }
                    self.norms[token] = vector.iter().map(|&v| i32::from(v) * i32::from(v)).sum();
                }
            }
        }
        true
    }

    /// Number of query vectors.
    pub fn vectors_count(&self) -> usize {
        self.vectors_count
    }

    fn blocks(&self) -> usize {
        self.vectors_count.div_ceil(BLOCK)
    }

    /// Sum over query tokens of the best similarity with any document token.
    pub fn score_max_similarity<T: PrimitiveVectorElement>(
        &self,
        document: TypedMultiDenseVectorRef<T>,
    ) -> ScoreType {
        debug_assert_eq!(Element::of::<T>(), Some(self.element));
        debug_assert_eq!(document.dim, self.dim);
        let mut maxima = vec![f32::NEG_INFINITY; self.blocks() * BLOCK];
        match self.element {
            Element::Float => self.float_max_similarity(
                bytemuck::cast_slice(document.flattened_vectors),
                &mut maxima,
            ),
            Element::Half => {
                thread_local! {
                    static DOCUMENT: RefCell<Vec<f32>> = const { RefCell::new(Vec::new()) };
                }
                DOCUMENT.with_borrow_mut(|floats| {
                    halves_to_floats(bytemuck::cast_slice(document.flattened_vectors), floats);
                    self.float_max_similarity(floats, &mut maxima);
                });
            }
            Element::Byte => self.byte_max_similarity(
                bytemuck::cast_slice(document.flattened_vectors),
                &mut maxima,
            ),
        }
        // Same order as `score_max_similarity`: query tokens in sequence.
        let mut sum = 0.0;
        for &maximum in &maxima[..self.vectors_count] {
            sum += maximum;
        }
        sum
    }

    fn float_max_similarity(&self, document: &[f32], maxima: &mut [f32]) {
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512f") {
            // SAFETY: AVX-512 F is available.
            unsafe {
                match self.op {
                    Op::Dot | Op::Cosine => {
                        x86::float_max_similarity_avx512::<{ x86::DOT }>(self, document, maxima)
                    }
                    Op::Euclid => {
                        x86::float_max_similarity_avx512::<{ x86::EUCLID }>(self, document, maxima)
                    }
                    Op::Manhattan => x86::float_max_similarity_avx512::<{ x86::MANHATTAN }>(
                        self, document, maxima,
                    ),
                }
            }
            return;
        }
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            // SAFETY: AVX2 and FMA are available.
            unsafe {
                match self.op {
                    Op::Dot | Op::Cosine => {
                        x86::float_max_similarity::<{ x86::DOT }>(self, document, maxima)
                    }
                    Op::Euclid => {
                        x86::float_max_similarity::<{ x86::EUCLID }>(self, document, maxima)
                    }
                    Op::Manhattan => {
                        x86::float_max_similarity::<{ x86::MANHATTAN }>(self, document, maxima)
                    }
                }
            }
            return;
        }
        self.float_max_similarity_portable(document, maxima);
    }

    fn float_max_similarity_portable(&self, document: &[f32], maxima: &mut [f32]) {
        let dim = self.dim;
        for block in 0..self.blocks() {
            let packed = &self.floats[block * dim * BLOCK..][..dim * BLOCK];
            let maxima = &mut maxima[block * BLOCK..][..BLOCK];
            for vector in document.chunks_exact(dim) {
                let mut acc = [0f32; BLOCK];
                for (&x, q) in vector.iter().zip(packed.as_chunks::<BLOCK>().0) {
                    for lane in 0..BLOCK {
                        acc[lane] += match self.op {
                            Op::Dot | Op::Cosine => q[lane] * x,
                            Op::Euclid => (q[lane] - x) * (q[lane] - x),
                            Op::Manhattan => (q[lane] - x).abs(),
                        };
                    }
                }
                for lane in 0..BLOCK {
                    let score = match self.op {
                        Op::Dot | Op::Cosine => acc[lane],
                        Op::Euclid | Op::Manhattan => -acc[lane],
                    };
                    if score > maxima[lane] {
                        maxima[lane] = score;
                    }
                }
            }
        }
    }

    fn byte_max_similarity(&self, document: &[u8], maxima: &mut [f32]) {
        thread_local! {
            static DOCUMENT: RefCell<Vec<i16>> = const { RefCell::new(Vec::new()) };
        }
        let pairs = self.dim.div_ceil(2);
        DOCUMENT.with_borrow_mut(|values| {
            values.clear();
            values.resize(pairs * 2, 0);
            for vector in document.chunks_exact(self.dim) {
                for (value, &byte) in values.iter_mut().zip(vector) {
                    *value = i16::from(byte);
                }
                let norm = match self.op {
                    Op::Cosine => vector.iter().map(|&v| i32::from(v) * i32::from(v)).sum(),
                    Op::Dot | Op::Euclid | Op::Manhattan => 0,
                };
                for block in 0..self.blocks() {
                    let mut acc = [0i32; BLOCK];
                    self.byte_block(block, values, &mut acc);
                    self.byte_finish(block, &acc, norm, &mut maxima[block * BLOCK..][..BLOCK]);
                }
            }
        });
    }

    /// Integer sums of one block of query tokens against one document vector.
    #[inline]
    fn byte_block(&self, block: usize, document: &[i16], acc: &mut [i32; BLOCK]) {
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512vnni")
        {
            // SAFETY: AVX-512 F, BW, VL and VNNI are available.
            unsafe {
                match self.op {
                    Op::Dot | Op::Cosine => {
                        x86::byte_block_avx512::<{ x86::DOT }>(self, block, document, acc)
                    }
                    Op::Euclid => {
                        x86::byte_block_avx512::<{ x86::EUCLID }>(self, block, document, acc)
                    }
                    Op::Manhattan => {
                        x86::byte_block_avx512::<{ x86::MANHATTAN }>(self, block, document, acc)
                    }
                }
            }
            return;
        }
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 is available.
            unsafe {
                match self.op {
                    Op::Dot | Op::Cosine => {
                        x86::byte_block::<{ x86::DOT }>(self, block, document, acc)
                    }
                    Op::Euclid => x86::byte_block::<{ x86::EUCLID }>(self, block, document, acc),
                    Op::Manhattan => {
                        x86::byte_block::<{ x86::MANHATTAN }>(self, block, document, acc)
                    }
                }
            }
            return;
        }
        let pairs = self.dim.div_ceil(2);
        let packed = &self.pairs[block * pairs * BLOCK * 2..][..pairs * BLOCK * 2];
        for (x, q) in document
            .as_chunks::<2>()
            .0
            .iter()
            .zip(packed.as_chunks::<{ BLOCK * 2 }>().0)
        {
            for lane in 0..BLOCK {
                for i in 0..2 {
                    let (q, x) = (i32::from(q[lane * 2 + i]), i32::from(x[i]));
                    acc[lane] += match self.op {
                        Op::Dot | Op::Cosine => q * x,
                        Op::Euclid => (q - x) * (q - x),
                        Op::Manhattan => (q - x).abs(),
                    };
                }
            }
        }
    }

    /// Scores of one block from its integer sums, as the per-pair `u8` metrics
    /// compute them, into the running maxima.
    #[inline]
    fn byte_finish(&self, block: usize, acc: &[i32; BLOCK], norm: i32, maxima: &mut [f32]) {
        let mut score = [0f32; BLOCK];
        match self.op {
            Op::Dot => {
                for lane in 0..BLOCK {
                    score[lane] = acc[lane] as f32;
                }
            }
            Op::Euclid | Op::Manhattan => {
                for lane in 0..BLOCK {
                    score[lane] = -acc[lane] as f32;
                }
            }
            Op::Cosine => {
                let norms = &self.norms[block * BLOCK..][..BLOCK];
                for lane in 0..BLOCK {
                    score[lane] = if norms[lane] == 0 || norm == 0 {
                        0.0
                    } else {
                        acc[lane] as f32 / (norms[lane] as f32 * norm as f32).sqrt()
                    };
                }
            }
        }
        for lane in 0..BLOCK {
            if score[lane] > maxima[lane] {
                maxima[lane] = score[lane];
            }
        }
    }
}

/// `halves` as `f32`, into `floats`; the conversion is exact.
fn halves_to_floats(halves: &[f16], floats: &mut Vec<f32>) {
    floats.clear();
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx") && is_x86_feature_detected!("f16c") {
        // SAFETY: AVX and F16C are available.
        unsafe { x86::halves_to_floats(halves, floats) };
        return;
    }
    floats.extend(halves.iter().map(|value| value.to_f32()));
}

/// The values of a float or half vector as `f32`.
fn as_floats<T: PrimitiveVectorElement>(vector: &[T]) -> impl Iterator<Item = f32> + '_ {
    let (floats, halves): (&[f32], &[f16]) = match Element::of::<T>() {
        Some(Element::Float) => (bytemuck::cast_slice(vector), &[]),
        Some(Element::Half) => (&[], bytemuck::cast_slice(vector)),
        _ => unreachable!("not a float element"),
    };
    floats
        .iter()
        .copied()
        .chain(halves.iter().map(|value| value.to_f32()))
}

/// MaxSim of two multi-vectors with `a` packed into a per-thread buffer, or
/// `None` without a batched kernel for them.
///
/// For scoring without a query to pack once (stored points against each
/// other, custom queries). Packing is linear in `a`, scoring is `|a| x |b|`,
/// so the buffer costs a small fraction of the call.
pub fn score_max_similarity_packed<T: PrimitiveVectorElement>(
    a: TypedMultiDenseVectorRef<T>,
    b: TypedMultiDenseVectorRef<T>,
    distance: Distance,
) -> Option<ScoreType> {
    thread_local! {
        static SCRATCH: RefCell<Option<PackedMultiQuery>> = const { RefCell::new(None) };
    }
    let element = Element::of::<T>()?;
    SCRATCH.with_borrow_mut(|scratch| {
        let op = Op::new(element, distance);
        let packed = match scratch {
            Some(packed) if packed.element == element && packed.op == op => packed,
            _ => scratch.insert(PackedMultiQuery::empty(element, distance)),
        };
        packed.pack(a).then(|| packed.score_max_similarity(b))
    })
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{BLOCK, PackedMultiQuery};

    pub(super) const DOT: u8 = 0;
    pub(super) const EUCLID: u8 = 1;
    pub(super) const MANHATTAN: u8 = 2;

    /// Document tokens per tile: 2 accumulators each, plus 2 query registers,
    /// a broadcast and, for Euclid and Manhattan, the differences.
    const fn tile(op: u8) -> usize {
        if op == DOT { 6 } else { 4 }
    }

    /// # Safety
    /// CPU must support AVX2 and FMA.
    #[target_feature(enable = "avx2,fma")]
    pub(super) unsafe fn float_max_similarity<const OP: u8>(
        query: &PackedMultiQuery,
        document: &[f32],
        maxima: &mut [f32],
    ) {
        let dim = query.dim;
        let doc_count = document.len() / dim;
        for block in 0..query.blocks() {
            // SAFETY: `floats` holds `dim` rows of 16 lanes per block, `document`
            // `doc_count` vectors of `dim` and `maxima` 16 lanes per block.
            unsafe {
                let packed = query.floats.as_ptr().add(block * dim * BLOCK);
                let at = maxima.as_mut_ptr().add(block * BLOCK);
                if query.vectors_count - block * BLOCK <= 8 {
                    // A last block of at most 8 tokens uses one register per row.
                    let max = half_block::<OP>(packed, document, dim, _mm256_loadu_ps(at));
                    _mm256_storeu_ps(at, max);
                    continue;
                }
                let mut max = (_mm256_loadu_ps(at), _mm256_loadu_ps(at.add(8)));
                let mut j = 0;
                while j + tile(OP) <= doc_count {
                    let rows = document.as_ptr().add(j * dim);
                    max = if OP == DOT {
                        tile_rows::<OP, { tile(DOT) }>(packed, rows, dim, max)
                    } else {
                        tile_rows::<OP, { tile(EUCLID) }>(packed, rows, dim, max)
                    };
                    j += tile(OP);
                }
                while j < doc_count {
                    max = tile_rows::<OP, 1>(packed, document.as_ptr().add(j * dim), dim, max);
                    j += 1;
                }
                _mm256_storeu_ps(at, max.0);
                _mm256_storeu_ps(at.add(8), max.1);
            }
        }
    }

    /// The first 8 query tokens of a block against every document token, with
    /// tiles twice as tall as [`tile_rows`].
    ///
    /// # Safety
    /// CPU must support AVX2 and FMA; as in [`float_max_similarity`].
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn half_block<const OP: u8>(
        packed: *const f32,
        document: &[f32],
        dim: usize,
        mut max: __m256,
    ) -> __m256 {
        let doc_count = document.len() / dim;
        let rows = 2 * tile(OP);
        let mut j = 0;
        // SAFETY: rows `j..j + R` are within `document`.
        unsafe {
            while j + rows <= doc_count {
                let at = document.as_ptr().add(j * dim);
                max = if OP == DOT {
                    half_rows::<OP, { 2 * tile(DOT) }>(packed, at, dim, max)
                } else {
                    half_rows::<OP, { 2 * tile(EUCLID) }>(packed, at, dim, max)
                };
                j += rows;
            }
            while j < doc_count {
                max = half_rows::<OP, 1>(packed, document.as_ptr().add(j * dim), dim, max);
                j += 1;
            }
        }
        max
    }

    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn half_rows<const OP: u8, const R: usize>(
        packed: *const f32,
        document: *const f32,
        dim: usize,
        max: __m256,
    ) -> __m256 {
        let sign = _mm256_set1_ps(-0.0);
        let mut acc = [_mm256_setzero_ps(); R];
        for k in 0..dim {
            // SAFETY: `k < dim`, within the packed rows and every document vector.
            unsafe {
                let q = _mm256_loadu_ps(packed.add(k * BLOCK));
                for (r, acc) in acc.iter_mut().enumerate() {
                    let x = _mm256_broadcast_ss(&*document.add(r * dim + k));
                    *acc = match OP {
                        DOT => _mm256_fmadd_ps(x, q, *acc),
                        EUCLID => {
                            let d = _mm256_sub_ps(q, x);
                            _mm256_fmadd_ps(d, d, *acc)
                        }
                        _ => _mm256_add_ps(*acc, _mm256_andnot_ps(sign, _mm256_sub_ps(q, x))),
                    };
                }
            }
        }
        let mut max = max;
        for &acc in &acc {
            let score = if OP == DOT {
                acc
            } else {
                _mm256_xor_ps(acc, sign)
            };
            max = _mm256_max_ps(score, max);
        }
        max
    }

    /// `halves` as `f32`, appended to the empty `floats`.
    ///
    /// # Safety
    /// CPU must support AVX and F16C.
    #[target_feature(enable = "avx,f16c")]
    pub(super) unsafe fn halves_to_floats(halves: &[half::f16], floats: &mut Vec<f32>) {
        debug_assert!(floats.is_empty());
        floats.resize(halves.len(), 0.0);
        let (chunks, tail) = halves.as_chunks::<8>();
        for (i, chunk) in chunks.iter().enumerate() {
            // SAFETY: chunk `i` covers `floats[8 i..8 i + 8]`.
            unsafe {
                let values = _mm256_cvtph_ps(_mm_loadu_si128(chunk.as_ptr().cast()));
                _mm256_storeu_ps(floats.as_mut_ptr().add(i * 8), values);
            }
        }
        let done = chunks.len() * 8;
        for (float, half) in floats[done..].iter_mut().zip(tail) {
            *float = half.to_f32();
        }
    }

    /// `R` document tokens at `document` against the [`BLOCK`] query tokens at
    /// `packed`; returns the running maxima updated with this tile.
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn tile_rows<const OP: u8, const R: usize>(
        packed: *const f32,
        document: *const f32,
        dim: usize,
        max: (__m256, __m256),
    ) -> (__m256, __m256) {
        let sign = _mm256_set1_ps(-0.0);
        let mut acc = [[_mm256_setzero_ps(); 2]; R];
        for k in 0..dim {
            // SAFETY: `k < dim`, within the packed rows and every document vector.
            unsafe {
                let q0 = _mm256_loadu_ps(packed.add(k * BLOCK));
                let q1 = _mm256_loadu_ps(packed.add(k * BLOCK + 8));
                for (r, acc) in acc.iter_mut().enumerate() {
                    let x = _mm256_broadcast_ss(&*document.add(r * dim + k));
                    match OP {
                        DOT => {
                            acc[0] = _mm256_fmadd_ps(x, q0, acc[0]);
                            acc[1] = _mm256_fmadd_ps(x, q1, acc[1]);
                        }
                        EUCLID => {
                            let d0 = _mm256_sub_ps(q0, x);
                            let d1 = _mm256_sub_ps(q1, x);
                            acc[0] = _mm256_fmadd_ps(d0, d0, acc[0]);
                            acc[1] = _mm256_fmadd_ps(d1, d1, acc[1]);
                        }
                        _ => {
                            let d0 = _mm256_andnot_ps(sign, _mm256_sub_ps(q0, x));
                            let d1 = _mm256_andnot_ps(sign, _mm256_sub_ps(q1, x));
                            acc[0] = _mm256_add_ps(acc[0], d0);
                            acc[1] = _mm256_add_ps(acc[1], d1);
                        }
                    }
                }
            }
        }
        let (mut max0, mut max1) = max;
        for acc in &acc {
            // Distances are similarities negated.
            let (s0, s1) = if OP == DOT {
                (acc[0], acc[1])
            } else {
                (_mm256_xor_ps(acc[0], sign), _mm256_xor_ps(acc[1], sign))
            };
            // Keeps the maximum unless the score is larger, like `score > maximum`.
            max0 = _mm256_max_ps(s0, max0);
            max1 = _mm256_max_ps(s1, max1);
        }
        (max0, max1)
    }

    /// # Safety
    /// CPU must support AVX2; `document` holds `dim` rounded up to pairs.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn byte_block<const OP: u8>(
        query: &PackedMultiQuery,
        block: usize,
        document: &[i16],
        acc: &mut [i32; BLOCK],
    ) {
        // SAFETY: AVX2 is available and `document` holds the pairs, by this
        // function's contract; `acc` holds 16 lanes.
        unsafe {
            if query.vectors_count - block * BLOCK <= 8 {
                // A last block of at most 8 tokens uses one register.
                let [acc0] = byte_sums::<OP, 1>(query, block, document);
                _mm256_storeu_si256(acc.as_mut_ptr().cast(), acc0);
            } else {
                let [acc0, acc1] = byte_sums::<OP, 2>(query, block, document);
                _mm256_storeu_si256(acc.as_mut_ptr().cast(), acc0);
                _mm256_storeu_si256(acc.as_mut_ptr().add(8).cast(), acc1);
            }
        }
    }

    /// Integer sums of the first `HALVES` x 8 query tokens of one block.
    ///
    /// # Safety
    /// CPU must support AVX2; `document` holds `dim` rounded up to pairs.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn byte_sums<const OP: u8, const HALVES: usize>(
        query: &PackedMultiQuery,
        block: usize,
        document: &[i16],
    ) -> [__m256i; HALVES] {
        let pairs = query.dim.div_ceil(2);
        debug_assert!(document.len() >= pairs * 2);
        let ones = _mm256_set1_epi16(1);
        let mut acc = [_mm256_setzero_si256(); HALVES];
        // SAFETY: `pairs` pairs of 16 lanes per block, and `pairs` document pairs.
        unsafe {
            let packed = query.pairs.as_ptr().add(block * pairs * BLOCK * 2);
            let doc = document.as_ptr().cast::<i32>();
            for p in 0..pairs {
                let x = _mm256_set1_epi32(doc.add(p).read_unaligned());
                for (h, acc) in acc.iter_mut().enumerate() {
                    let q = _mm256_loadu_si256(packed.add(p * BLOCK * 2 + h * 16).cast());
                    let products = match OP {
                        DOT => _mm256_madd_epi16(x, q),
                        EUCLID => {
                            let d = _mm256_sub_epi16(q, x);
                            _mm256_madd_epi16(d, d)
                        }
                        _ => _mm256_madd_epi16(_mm256_abs_epi16(_mm256_sub_epi16(q, x)), ones),
                    };
                    *acc = _mm256_add_epi32(*acc, products);
                }
            }
        }
        acc
    }

    /// Document tokens per AVX-512 tile: one accumulator each of the 32
    /// registers, plus the query, a broadcast and the differences.
    const fn tile_avx512(op: u8) -> usize {
        if op == DOT { 12 } else { 8 }
    }

    /// [`float_max_similarity`] with one 16-lane register per block.
    ///
    /// # Safety
    /// CPU must support AVX-512 F.
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn float_max_similarity_avx512<const OP: u8>(
        query: &PackedMultiQuery,
        document: &[f32],
        maxima: &mut [f32],
    ) {
        let dim = query.dim;
        let doc_count = document.len() / dim;
        for block in 0..query.blocks() {
            // SAFETY: as in `float_max_similarity`.
            unsafe {
                let packed = query.floats.as_ptr().add(block * dim * BLOCK);
                let at = maxima.as_mut_ptr().add(block * BLOCK);
                let mut max = _mm512_loadu_ps(at);
                let mut j = 0;
                while j + tile_avx512(OP) <= doc_count {
                    let rows = document.as_ptr().add(j * dim);
                    max = if OP == DOT {
                        tile_rows_avx512::<OP, { tile_avx512(DOT) }>(packed, rows, dim, max)
                    } else {
                        tile_rows_avx512::<OP, { tile_avx512(EUCLID) }>(packed, rows, dim, max)
                    };
                    j += tile_avx512(OP);
                }
                while j < doc_count {
                    max =
                        tile_rows_avx512::<OP, 1>(packed, document.as_ptr().add(j * dim), dim, max);
                    j += 1;
                }
                _mm512_storeu_ps(at, max);
            }
        }
    }

    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn tile_rows_avx512<const OP: u8, const R: usize>(
        packed: *const f32,
        document: *const f32,
        dim: usize,
        max: __m512,
    ) -> __m512 {
        let mut acc = [_mm512_setzero_ps(); R];
        for k in 0..dim {
            // SAFETY: `k < dim`, within the packed rows and every document vector.
            unsafe {
                let q = _mm512_loadu_ps(packed.add(k * BLOCK));
                for (r, acc) in acc.iter_mut().enumerate() {
                    let x = _mm512_set1_ps(*document.add(r * dim + k));
                    *acc = match OP {
                        DOT => _mm512_fmadd_ps(x, q, *acc),
                        EUCLID => {
                            let d = _mm512_sub_ps(q, x);
                            _mm512_fmadd_ps(d, d, *acc)
                        }
                        _ => _mm512_add_ps(*acc, _mm512_abs_ps(_mm512_sub_ps(q, x))),
                    };
                }
            }
        }
        let mut max = max;
        for &acc in &acc {
            let score = if OP == DOT {
                acc
            } else {
                _mm512_castsi512_ps(_mm512_xor_si512(
                    _mm512_castps_si512(acc),
                    _mm512_set1_epi32(i32::MIN),
                ))
            };
            max = _mm512_max_ps(score, max);
        }
        max
    }

    /// [`byte_block`] with `vpdpwssd`, which multiplies a pair and adds it in one
    /// instruction: one 16-lane register, or one 8-lane register for a last block
    /// of at most 8 tokens.
    ///
    /// # Safety
    /// CPU must support AVX-512 F, BW, VL and VNNI; `document` holds `dim`
    /// rounded up to pairs.
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,avx512vnni")]
    pub(super) unsafe fn byte_block_avx512<const OP: u8>(
        query: &PackedMultiQuery,
        block: usize,
        document: &[i16],
        acc: &mut [i32; BLOCK],
    ) {
        let pairs = query.dim.div_ceil(2);
        let whole = pairs - pairs % CHAINS;
        debug_assert!(document.len() >= pairs * 2);
        // SAFETY: as in `byte_block`.
        unsafe {
            let packed = query.pairs.as_ptr().add(block * pairs * BLOCK * 2);
            let doc = document.as_ptr().cast::<i32>();
            // Integer sums, so splitting them over chains changes nothing.
            if query.vectors_count - block * BLOCK <= 8 {
                let mut sums = [_mm256_setzero_si256(); CHAINS];
                for first in (0..whole).step_by(CHAINS) {
                    for (chain, sum) in sums.iter_mut().enumerate() {
                        *sum = byte_pair_avx512_half::<OP>(*sum, doc, packed, first + chain);
                    }
                }
                for p in whole..pairs {
                    sums[0] = byte_pair_avx512_half::<OP>(sums[0], doc, packed, p);
                }
                let sum = _mm256_add_epi32(
                    _mm256_add_epi32(sums[0], sums[1]),
                    _mm256_add_epi32(sums[2], sums[3]),
                );
                _mm256_storeu_si256(acc.as_mut_ptr().cast(), sum);
                return;
            }
            let mut sums = [_mm512_setzero_si512(); CHAINS];
            for first in (0..whole).step_by(CHAINS) {
                for (chain, sum) in sums.iter_mut().enumerate() {
                    *sum = byte_pair_avx512::<OP>(*sum, doc, packed, first + chain);
                }
            }
            for p in whole..pairs {
                sums[0] = byte_pair_avx512::<OP>(sums[0], doc, packed, p);
            }
            let sum = _mm512_add_epi32(
                _mm512_add_epi32(sums[0], sums[1]),
                _mm512_add_epi32(sums[2], sums[3]),
            );
            _mm512_storeu_si512(acc.as_mut_ptr().cast(), sum);
        }
    }

    /// Independent accumulator chains in [`byte_block_avx512`]: one chain waits
    /// out `vpdpwssd`'s latency on every pair.
    const CHAINS: usize = 4;

    /// One pair of document values against the same pair of dims of 16 query
    /// tokens.
    ///
    /// # Safety
    /// CPU must support AVX-512 F, BW and VNNI; `doc` holds pair `p` and
    /// `packed` its 16 lanes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn byte_pair_avx512<const OP: u8>(
        sum: __m512i,
        doc: *const i32,
        packed: *const i16,
        p: usize,
    ) -> __m512i {
        // SAFETY: by this function's contract.
        let (x, q) = unsafe {
            (
                _mm512_set1_epi32(doc.add(p).read_unaligned()),
                _mm512_loadu_si512(packed.add(p * BLOCK * 2).cast()),
            )
        };
        match OP {
            DOT => _mm512_dpwssd_epi32(sum, x, q),
            EUCLID => {
                let d = _mm512_sub_epi16(q, x);
                _mm512_dpwssd_epi32(sum, d, d)
            }
            _ => _mm512_dpwssd_epi32(
                sum,
                _mm512_abs_epi16(_mm512_sub_epi16(q, x)),
                _mm512_set1_epi16(1),
            ),
        }
    }

    /// [`byte_pair_avx512`] for the first 8 query tokens of a block.
    ///
    /// # Safety
    /// CPU must support AVX-512 VL and VNNI; `doc` holds pair `p` and `packed`
    /// its 16 lanes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,avx512vnni")]
    unsafe fn byte_pair_avx512_half<const OP: u8>(
        sum: __m256i,
        doc: *const i32,
        packed: *const i16,
        p: usize,
    ) -> __m256i {
        // SAFETY: by this function's contract.
        let (x, q) = unsafe {
            (
                _mm256_set1_epi32(doc.add(p).read_unaligned()),
                _mm256_loadu_si256(packed.add(p * BLOCK * 2).cast()),
            )
        };
        match OP {
            DOT => _mm256_dpwssd_epi32(sum, x, q),
            EUCLID => {
                let d = _mm256_sub_epi16(q, x);
                _mm256_dpwssd_epi32(sum, d, d)
            }
            _ => _mm256_dpwssd_epi32(
                sum,
                _mm256_abs_epi16(_mm256_sub_epi16(q, x)),
                _mm256_set1_epi16(1),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
    use crate::vector_storage::query_scorer::score_max_similarity_pairwise;

    /// `reference` scores pair by pair; `exact` asks for the same value.
    fn check<T: PrimitiveVectorElement, M: Metric<T>>(
        make: impl Fn(&mut StdRng) -> T,
        reference: impl Fn(TypedMultiDenseVectorRef<T>, TypedMultiDenseVectorRef<T>) -> f32,
        exact: bool,
    ) {
        let mut rng = StdRng::seed_from_u64(42);
        for dim in [1, 3, 8, 31, 32, 33, 96, 128, 129] {
            for nq in [1, 3, 8, 9, 15, 16, 17, 24, 32, 33] {
                for doc_len in [1, 5, 6, 7, 12, 13, 40] {
                    let query: Vec<T> = (0..nq * dim).map(|_| make(&mut rng)).collect();
                    let document: Vec<T> = (0..doc_len * dim).map(|_| make(&mut rng)).collect();
                    let query = TypedMultiDenseVectorRef::new(&query, dim);
                    let document = TypedMultiDenseVectorRef::new(&document, dim);
                    let want = reference(query, document);
                    let packed = PackedMultiQuery::new(query, M::distance()).unwrap();
                    let got = packed.score_max_similarity(document);
                    // The same through the per-thread buffer.
                    let scratch =
                        score_max_similarity_packed(query, document, M::distance()).unwrap();
                    assert_eq!(got.to_bits(), scratch.to_bits());
                    let what = format!("{:?} dim={dim} nq={nq} doc_len={doc_len}", M::distance());
                    if exact {
                        assert!(got == want, "{what}: {got} vs {want}");
                    } else {
                        // Float sums in another order, relative to the summed magnitudes.
                        let tolerance = 1e-5 * (nq * dim) as f32;
                        assert!((got - want).abs() <= tolerance, "{what}: {got} vs {want}");
                    }
                }
            }
        }
    }

    fn float(rng: &mut StdRng) -> f32 {
        rng.random_range(-1.0..1.0)
    }

    fn half(rng: &mut StdRng) -> f16 {
        f16::from_f32(rng.random_range(-1.0..1.0))
    }

    fn byte(rng: &mut StdRng) -> u8 {
        rng.random()
    }

    fn pairwise<T: PrimitiveVectorElement, M: Metric<T>>(
        a: TypedMultiDenseVectorRef<T>,
        b: TypedMultiDenseVectorRef<T>,
    ) -> f32 {
        score_max_similarity_pairwise::<T, M>(a, b)
    }

    /// `f16` vectors scored pair by pair as `f32`: the NEON `f16` kernels
    /// accumulate in `f16`, the batched kernel converts to `f32` first.
    fn pairwise_as_float<M: Metric<f32>>(
        a: TypedMultiDenseVectorRef<f16>,
        b: TypedMultiDenseVectorRef<f16>,
    ) -> f32 {
        let dim = a.dim;
        let a: Vec<f32> = a.flattened_vectors.iter().map(|x| x.to_f32()).collect();
        let b: Vec<f32> = b.flattened_vectors.iter().map(|x| x.to_f32()).collect();
        score_max_similarity_pairwise::<f32, M>(
            TypedMultiDenseVectorRef::new(&a, dim),
            TypedMultiDenseVectorRef::new(&b, dim),
        )
    }

    #[test]
    fn float_matches_pairwise() {
        check::<f32, DotProductMetric>(float, pairwise::<f32, DotProductMetric>, false);
        check::<f32, CosineMetric>(float, pairwise::<f32, CosineMetric>, false);
        check::<f32, EuclidMetric>(float, pairwise::<f32, EuclidMetric>, false);
        check::<f32, ManhattanMetric>(float, pairwise::<f32, ManhattanMetric>, false);
    }

    #[test]
    fn half_matches_pairwise() {
        check::<f16, DotProductMetric>(half, pairwise_as_float::<DotProductMetric>, false);
        check::<f16, CosineMetric>(half, pairwise_as_float::<CosineMetric>, false);
        check::<f16, EuclidMetric>(half, pairwise_as_float::<EuclidMetric>, false);
        check::<f16, ManhattanMetric>(half, pairwise_as_float::<ManhattanMetric>, false);
    }

    #[test]
    fn byte_matches_pairwise() {
        check::<u8, DotProductMetric>(byte, pairwise::<u8, DotProductMetric>, true);
        check::<u8, CosineMetric>(byte, pairwise::<u8, CosineMetric>, true);
        check::<u8, EuclidMetric>(byte, pairwise::<u8, EuclidMetric>, true);
        check::<u8, ManhattanMetric>(byte, pairwise::<u8, ManhattanMetric>, true);
    }

    #[test]
    fn few_query_vectors_are_not_packed() {
        let query = vec![0.5f32; 7 * 8];
        let query = TypedMultiDenseVectorRef::new(&query, 8);
        assert!(PackedMultiQuery::new_if_faster(query, Distance::Dot).is_none());
        assert!(PackedMultiQuery::new(query, Distance::Dot).is_some());
    }
}
