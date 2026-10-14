//! The cache-blocked matrix product shared by every register-tiled kernel. The rows are
//! decoded and packed once per call, then: a block of columns, packed once (in the per-thread
//! L3 share) → a block of rows (L2) → a depth slice → a panel of columns (L1) → a tile of rows,
//! in registers. Only one block of inner products is live: each finished block is combined
//! into scores and offered to the top-k lists.
//!
//! Everything here is `#[inline(always)]`, so it compiles into the `#[target_feature]`
//! function of each kernel.

use common::types::PointOffsetType;

use crate::combine::{Combine, CombineDispatch, CombineFn};
use crate::operands::{KernelElem, MatrixOperands};
use crate::scratch::{AlignedBuf, Plain};
use crate::top_k::Lists;
use crate::{Cols, TopK};

/// Vectors decoded per batch before packing: a multiple of every tile and panel width (4, 6,
/// 8, 12, 16, 32).
pub(crate) const GROUP: usize = 48;

/// A register tile: `MR` rows by `NR` columns, depth consumed `KU` elements at a time.
pub(crate) trait Micro {
    type E: KernelElem + Plain;
    type Acc: Accumulator;

    /// Rows of a tile.
    const MR: usize;
    /// Columns of a tile.
    const NR: usize;
    /// Consecutive depth elements of one vector in the packed layout: 1 for FMA, 4 for
    /// byte multiply-adds.
    const KU: usize;
    /// Depth slice in elements, a multiple of `KU`.
    const KC: usize;
    /// Rows per row block, a multiple of `MR`.
    const MC: usize;
    /// Columns per column block, a multiple of `NR`. Doesn't change any score.
    ///
    /// The packed column block has to stay in cache while every row block reads it, and all
    /// packed rows are read once per column block. Larger blocks spill, smaller ones read the
    /// rows more often; measured with f32 at dim 1536 on a Ryzen 5600X (32 MB L3, 12
    /// threads), both cost from 4x off the best value on.
    const NC: usize;

    /// `c[MR][NR] = (accumulate ? c : 0) + a · bᵀ` over `kc` depth elements (a multiple of
    /// `KU`), for the panels `a` (`[kc / KU][MR][KU]`) and `b` (`[kc / KU][NR][KU]`), both
    /// 32-byte aligned. Row `r` of `c` starts at `c + r * ldc`.
    ///
    /// # Safety
    ///
    /// The pointers must be valid for these sizes, and the CPU must support the kernel.
    unsafe fn tile(
        kc: usize,
        a: *const Self::E,
        b: *const Self::E,
        c: *mut Self::Acc,
        ldc: usize,
        accumulate: bool,
    );

    /// `dst[j * ds + i] = src[i * ss + j]` for 8 x 8 cells of 4 bytes.
    ///
    /// # Safety
    ///
    /// The pointers must be valid for these cells, and the CPU must support the kernel.
    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        for i in 0..8 {
            for j in 0..8 {
                unsafe { *dst.add(j * ds + i) = *src.add(i * ss + j) };
            }
        }
    }

    /// `dst[j * w + i] = src[i * ss + j]` for `w < 8` rows of 8 cells of 4 bytes. May write
    /// up to `8 - w` cells past each row `j`, but not past row 7 if `last`.
    ///
    /// # Safety
    ///
    /// As [`Self::transpose8x8`], with `dst` valid for `8 * w` cells, or `8 * w + 8 - w`
    /// unless `last`.
    #[inline(always)]
    unsafe fn transpose_narrow(src: *const u32, ss: usize, w: usize, dst: *mut u32, last: bool) {
        let _ = last;
        for i in 0..w {
            for j in 0..8 {
                unsafe { *dst.add(j * w + i) = *src.add(i * ss + j) };
            }
        }
    }
}

/// Accumulator of inner products, converted to the `f32` that [`Combine`] takes exactly as
/// [`KernelElem::dot`] converts it.
pub(crate) trait Accumulator: Plain {
    fn to_f32(self) -> f32;
}

impl Accumulator for f32 {
    #[inline(always)]
    fn to_f32(self) -> f32 {
        self
    }
}

impl Accumulator for i32 {
    #[inline(always)]
    fn to_f32(self) -> f32 {
        self as f32
    }
}

impl Accumulator for u16 {
    #[inline(always)]
    fn to_f32(self) -> f32 {
        f32::from(self)
    }
}

/// Layout of `n` vectors packed in panels of `w`: `[depth slice][panel][kc / ku][w][ku]`,
/// with the depth padded to a multiple of `ku` and the last panel padded with zeros.
struct Panels {
    w: usize,
    ku: usize,
    panels: usize,
    /// Padded depth.
    depth: usize,
    kc: usize,
}

impl Panels {
    fn new(n: usize, w: usize, depth: usize, kc: usize, ku: usize) -> Self {
        let depth = depth.next_multiple_of(ku);
        Self {
            w,
            ku,
            panels: n.div_ceil(w),
            depth,
            kc: kc.min(depth),
        }
    }

    fn len(&self) -> usize {
        self.panels * self.w * self.depth
    }

    fn slices(&self) -> usize {
        self.depth.div_ceil(self.kc)
    }

    /// Depth elements of slice `s`.
    fn slice_depth(&self, s: usize) -> usize {
        self.kc.min(self.depth - s * self.kc)
    }

    fn offset(&self, s: usize, panel: usize) -> usize {
        s * self.kc * self.panels * self.w + panel * self.slice_depth(s) * self.w
    }
}

/// Decodes `ids` and packs them in `layout` into `out`, their values for [`Combine`] into
/// `aux`.
#[inline(always)]
fn pack<M: Micro, F: MatrixOperands<Elem = M::E>>(
    operands: &F,
    ids: &[PointOffsetType],
    layout: &Panels,
    out: &mut [M::E],
    aux: &mut [f32],
    decoded: &mut AlignedBuf,
) -> Result<(), F::Error> {
    let depth = operands.depth();
    let decoded = decoded.get::<M::E>(GROUP * depth);
    for (g, ids) in ids.chunks(GROUP).enumerate() {
        let start = g * GROUP;
        operands.decode_batch(
            ids,
            &mut decoded[..ids.len() * depth],
            &mut aux[start..start + ids.len()],
        )?;
        put_group::<M>(layout, out, start, ids.len(), decoded, depth);
    }
    Ok(())
}

/// Packs `count` decoded vectors (`depth` elements each) as vectors `start..start + count` of
/// `layout`, `start` a multiple of the panel width. Zero-fills the depth padding, and the
/// rest of the last panel.
#[inline(always)]
fn put_group<M: Micro>(
    layout: &Panels,
    out: &mut [M::E],
    start: usize,
    count: usize,
    decoded: &[M::E],
    depth: usize,
) {
    let (w, ku) = (layout.w, layout.ku);
    debug_assert_eq!(start % w, 0);
    assert!(out.len() >= layout.len() && decoded.len() >= count * depth);
    // Cells of `ku` elements, 4 bytes each, which the transposes move
    let cells = (size_of::<M::E>() * ku == 4 && depth.is_multiple_of(ku)).then_some(depth / ku);
    for p0 in (0..count).step_by(w) {
        let panel = (start + p0) / w;
        let lanes = w.min(count - p0);
        for s in 0..layout.slices() {
            let slice_cells = layout.slice_depth(s) / ku;
            let first_cell = s * layout.kc / ku;
            let dst = &mut out[layout.offset(s, panel)..][..slice_cells * w * ku];
            let mut g = 0;
            if let Some(cells) = cells
                && lanes == w
            {
                // Whole chunks of 8 cells inside the vectors
                let fast = slice_cells.min(cells.saturating_sub(first_cell)) / 8 * 8;
                // SAFETY: reads cells `first_cell + g..+8` of the `w` vectors from `p0`, all
                // within `decoded`. Writes cells `g..g + 8` of the slice's `w` lanes, and the
                // narrow form's overflow stays inside `dst` (`last` on its final chunk).
                unsafe {
                    let src = decoded.as_ptr().add(p0 * depth).cast::<u32>();
                    let dst = dst.as_mut_ptr().cast::<u32>();
                    for g in (0..fast).step_by(8) {
                        let src = src.add(first_cell + g);
                        if w >= 8 {
                            // Windows of 8 lanes. Unless `w` is a multiple of 8, the last one
                            // overlaps the one before and rewrites its lanes with the same cells.
                            for l in (0..w).step_by(8) {
                                let l = l.min(w - 8);
                                M::transpose8x8(src.add(l * cells), cells, dst.add(g * w + l), w);
                            }
                        } else {
                            debug_assert!(w < 8);
                            let last = g + 8 == slice_cells;
                            M::transpose_narrow(src, cells, w, dst.add(g * w), last);
                        }
                    }
                }
                g = fast;
            }
            for g in g..slice_cells {
                for lane in 0..w {
                    for u in 0..ku {
                        let i = (first_cell + g) * ku + u;
                        dst[(g * w + lane) * ku + u] = if lane < lanes && i < depth {
                            decoded[(p0 + lane) * depth + i]
                        } else {
                            M::E::default()
                        };
                    }
                }
            }
        }
    }
}

/// Row stride of a block of inner products: not a multiple of 4 KB, whose rows would all map
/// to the same cache sets.
pub(crate) fn padded_stride(n: usize, size: usize) -> usize {
    if (n * size).is_multiple_of(4096) {
        n + 64 / size
    } else {
        n
    }
}

/// [`top_k`](crate::top_k) through the blocked product with the tile `M`.
#[inline(always)]
pub(crate) fn top_k<M: Micro, F: MatrixOperands<Elem = M::E>>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
) -> Result<(), F::Error> {
    let (cols, triangle) = match cols {
        Cols::Rows => (rows, true),
        Cols::Other(cols) => (cols, false),
    };
    let (m, n) = (rows.len(), cols.len());
    if m == 0 || n == 0 {
        return Ok(());
    }
    let depth = operands.depth();
    let combine = operands.combine();
    let (lists, s) = out.parts();
    s.row_aux.resize(m, 0.0);
    s.col_aux.resize(n, 0.0);
    let ldc = padded_stride(M::NC, size_of::<M::Acc>());
    let block = s.out.get::<M::Acc>(M::MC * ldc);
    // All rows once, instead of again for every column block, which made small blocks slow
    let a_layout = Panels::new(m, M::MR, depth, M::KC, M::KU);
    let a = s.rows.get::<M::E>(a_layout.len());
    pack::<M, F>(operands, rows, &a_layout, a, &mut s.row_aux, &mut s.decoded)?;
    for j0 in (0..n).step_by(M::NC) {
        let j1 = (j0 + M::NC).min(n);
        let b_layout = Panels::new(j1 - j0, M::NR, depth, M::KC, M::KU);
        let b = s.cols.get::<M::E>(b_layout.len());
        pack::<M, F>(
            operands,
            &cols[j0..j1],
            &b_layout,
            b,
            &mut s.col_aux[j0..j1],
            &mut s.decoded,
        )?;
        // The triangle needs only rows below the block's last column
        let r_end = if triangle { j1 } else { m };
        for r0 in (0..r_end).step_by(M::MC) {
            let r1 = (r0 + M::MC).min(m);
            let (rp0, rp1) = (r0 / M::MR, r1.div_ceil(M::MR));
            // In the triangle, only panels with columns right of the block's first row
            let p0 = (if triangle { r0.max(j0) } else { j0 }) / M::NR;
            let p1 = j1.div_ceil(M::NR);
            for slice in 0..b_layout.slices() {
                let kc = b_layout.slice_depth(slice);
                for p in p0..p1 {
                    for rp in rp0..rp1 {
                        // SAFETY: both panels are within their packed buffers, and the tile
                        // within `block`: rows `(rp - rp0) * MR..+MR <= MC`, columns
                        // `p * NR - j0..+NR <= NC <= ldc`. The caller checked the CPU.
                        unsafe {
                            M::tile(
                                kc,
                                a.as_ptr().add(a_layout.offset(slice, rp)),
                                b.as_ptr().add(b_layout.offset(slice, p - j0 / M::NR)),
                                block
                                    .as_mut_ptr()
                                    .add((rp - rp0) * M::MR * ldc + p * M::NR - j0),
                                ldc,
                                slice > 0,
                            );
                        }
                    }
                }
            }
            let c0 = p0 * M::NR;
            for r in r0..r1 {
                let dots = &block[(r - r0) * ldc + c0 - j0..(r - r0) * ldc + j1 - j0];
                emit(
                    lists,
                    combine,
                    triangle,
                    r,
                    s.row_aux[r],
                    c0,
                    dots,
                    &s.col_aux[c0..j1],
                    &mut s.scores,
                    &mut s.scores_t,
                );
            }
        }
    }
    Ok(())
}

/// Turns the inner products of row `r` with columns `c0..` into scores and offers them to the
/// lists. In a `triangle`, only columns right of `r` count, and each pair is offered both
/// ways, combined once per direction.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(crate) fn emit<A: Accumulator, D: CombineDispatch>(
    lists: &mut Lists,
    combine: D,
    triangle: bool,
    r: usize,
    row_aux: f32,
    c0: usize,
    dots: &[A],
    col_aux: &[f32],
    scores: &mut Vec<f32>,
    scores_t: &mut Vec<f32>,
) {
    let w = dots.len();
    scores.resize(w, 0.0);
    combine.dispatch(CombineRow::<false, A> {
        dots,
        fixed: row_aux,
        aux: col_aux,
        out: scores,
    });
    if triangle {
        lists.push_row(r, c0, scores, r + 1);
        scores_t.resize(w, 0.0);
        combine.dispatch(CombineRow::<true, A> {
            dots,
            fixed: row_aux,
            aux: col_aux,
            out: scores_t,
        });
        lists.push_col(r, c0, scores_t, r + 1);
    } else {
        lists.push_row(r, c0, scores, 0);
    }
}

/// `out[j] = combine(dots[j])` with the fixed row's value first, or with `TRANSPOSED`,
/// second. Only this loop is compiled once per formula a [`CombineDispatch`] can choose, so
/// the kernels around it don't grow with the formulas.
struct CombineRow<'a, const TRANSPOSED: bool, A> {
    dots: &'a [A],
    fixed: f32,
    aux: &'a [f32],
    out: &'a mut [f32],
}

impl<const TRANSPOSED: bool, A: Accumulator> CombineFn for CombineRow<'_, TRANSPOSED, A> {
    type Output = ();

    #[inline(always)]
    fn call<C: Combine>(self, combine: C) {
        let Self {
            dots,
            fixed,
            aux,
            out,
        } = self;
        let n = dots.len();
        for ((out, &dot), &aux) in out[..n].iter_mut().zip(dots).zip(&aux[..n]) {
            *out = if TRANSPOSED {
                combine.score(dot.to_f32(), aux, fixed)
            } else {
                combine.score(dot.to_f32(), fixed, aux)
            };
        }
    }
}
