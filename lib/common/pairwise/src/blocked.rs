//! The cache-blocked matrix product shared by every register-tiled kernel. The rows are
//! decoded and packed once per call, then: a block of columns, packed once (in the per-thread
//! L3 share) → a block of rows (L2) → a depth slice → a panel of columns (L1) → a tile of rows,
//! in registers. Only one block of inner products is live: each finished block is combined
//! into scores and offered to the top-k lists.
//!
//! Everything here is `#[inline(always)]`, so it compiles into the `#[target_feature]`
//! function of each kernel.

use common::types::PointOffsetType;

use crate::combine::{Aux, Combine, CombineDispatch, CombineFn};
use crate::operands::{KernelElem, MatrixOperands};
use crate::scratch::{AlignedBuf, Plain};
use crate::top_k::Lists;
use crate::{Cols, TopK};

/// Vectors decoded per batch before packing: a multiple of every tile and panel width (4, 6,
/// 8, 12, 16, 32).
pub(crate) const GROUP: usize = 96;

/// When a tile that unpacks (`UNPACK > 0`) unpacks one side.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unpacking {
    /// Never: the tile reads that side's packed panels, as `Operand` of `E`'s size.
    No,
    /// A row block's panels once per depth slice, a column panel once per row block, so they
    /// stay in L2 and L1.
    InBlocks,
    /// All rows once per call, and all panels of a column block once per block. Costs memory
    /// for the unpacked values, but no repeated unpacking: for tiles that read their operands
    /// fast enough for unpacking to show.
    Whole,
}

/// A register tile: `MR` rows by `NR` columns, depth consumed `KU` elements at a time.
pub(crate) trait Micro {
    type E: KernelElem + Plain;
    type Acc: Accumulator;
    /// What the tile reads: `E` itself, or for a tile that unpacks its panels, the values
    /// [`Self::unpack`] writes.
    type Operand: Plain;

    /// Values per element and vector that [`Self::unpack`] writes, or 0 for a tile that reads
    /// the packed panels themselves, whose `Operand` is then `E`.
    ///
    /// Unpacking widens the values of a row block's panels once per depth slice, and of a
    /// column panel once per row block, so the register tile only loads and multiplies.
    const UNPACK: usize = 0;
    /// When the rows are unpacked, if `UNPACK > 0`.
    const UNPACK_ROWS: Unpacking = Unpacking::InBlocks;
    /// When the columns are unpacked, if `UNPACK > 0`.
    const UNPACK_COLS: Unpacking = Unpacking::InBlocks;
    /// `Operand` values after each unpacked row panel, which [`Self::unpack`] writes for the
    /// tile: per-row sums, say.
    const ROW_EXTRA: usize = 0;

    /// Values per lane of a panel of `kc` elements after [`Self::unpack`]: `kc · UNPACK`,
    /// unless the tile rounds it up.
    #[inline(always)]
    fn unpacked_lane(kc: usize) -> usize {
        kc * Self::UNPACK
    }

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

    /// Writes the [`Self::unpacked_lane`] values per lane of a panel of `kc` elements, of `MR`
    /// lanes and then [`Self::ROW_EXTRA`] values if `rows`, else of `NR` lanes, to `dst`,
    /// 32-byte aligned.
    ///
    /// # Safety
    ///
    /// The pointers must be valid for these sizes, and the CPU must support the kernel.
    #[inline(always)]
    unsafe fn unpack(
        kc: usize,
        panel: *const Self::E,
        rows: bool,
        dst: *mut Self::Operand,
        param: <Self::E as KernelElem>::Param<'_>,
    ) {
        let _ = (kc, panel, rows, dst, param);
        unreachable!("only tiles with `UNPACK > 0` unpack");
    }

    /// `c[MR][NR] = (accumulate ? c : 0) + a · bᵀ` over `kc` depth elements (a multiple of
    /// `KU`) from element `k0` on, for the panels `a` (`[kc / KU][MR][KU]`) and `b`
    /// (`[kc / KU][NR][KU]`), both 32-byte aligned, or what [`Self::unpack`] made of them.
    /// Row `r` of `c` starts at `c + r * ldc`.
    ///
    /// # Safety
    ///
    /// The pointers must be valid for these sizes, `param` for depth `k0..k0 + kc`, and the
    /// CPU must support the kernel.
    #[allow(clippy::too_many_arguments)]
    unsafe fn tile(
        kc: usize,
        a: *const Self::Operand,
        b: *const Self::Operand,
        c: *mut Self::Acc,
        ldc: usize,
        accumulate: bool,
        param: <Self::E as KernelElem>::Param<'_>,
        k0: usize,
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

    /// First depth element of slice `s`.
    fn slice_start(&self, s: usize) -> usize {
        s * self.kc
    }

    fn offset(&self, s: usize, panel: usize) -> usize {
        s * self.kc * self.panels * self.w + panel * self.slice_depth(s) * self.w
    }

    /// Size of panel of slice `s` unpacked by `M`, with `extra` values after it.
    fn unpacked_size<M: Micro>(&self, s: usize, extra: usize) -> usize {
        self.w * M::unpacked_lane(self.slice_depth(s)) + extra
    }

    /// [`Self::offset`] in the whole unpacked side, see [`Unpacking::Whole`].
    fn unpacked_offset<M: Micro>(&self, s: usize, panel: usize, extra: usize) -> usize {
        // All slices before the last are full
        s * self.panels * self.unpacked_size::<M>(0, extra)
            + panel * self.unpacked_size::<M>(s, extra)
    }

    fn unpacked_len<M: Micro>(&self, extra: usize) -> usize {
        let last = self.slices() - 1;
        self.unpacked_offset::<M>(last, self.panels, extra)
    }
}

/// How the tile `M` gets the values of the rows, or of the columns.
const fn unpacking<M: Micro>(rows: bool) -> Unpacking {
    if M::UNPACK == 0 {
        Unpacking::No
    } else if rows {
        M::UNPACK_ROWS
    } else {
        M::UNPACK_COLS
    }
}

/// Unpacks all panels of `packed` in `layout` into `buf`, see [`Unpacking::Whole`].
#[inline(always)]
fn unpack_whole<'a, M: Micro>(
    layout: &Panels,
    packed: &[M::E],
    rows: bool,
    buf: &'a mut AlignedBuf,
    param: <M::E as KernelElem>::Param<'_>,
) -> &'a mut [M::Operand] {
    let extra = if rows { M::ROW_EXTRA } else { 0 };
    let unpacked = buf.get::<M::Operand>(layout.unpacked_len::<M>(extra));
    assert!(packed.len() >= layout.len());
    for s in 0..layout.slices() {
        for panel in 0..layout.panels {
            // SAFETY: the panel within the packed values, its unpacked values within the
            // buffer: `unpacked_len` sums up the sizes of all panels. The caller checked the CPU.
            unsafe {
                M::unpack(
                    layout.slice_depth(s),
                    packed.as_ptr().add(layout.offset(s, panel)),
                    rows,
                    unpacked
                        .as_mut_ptr()
                        .add(layout.unpacked_offset::<M>(s, panel, extra)),
                    param,
                );
            }
        }
    }
    unpacked
}

/// Decodes `ids` and packs them in `layout` into `out`, their values for [`Combine`] into
/// `aux`.
#[inline(always)]
fn pack<M: Micro, F: MatrixOperands<Elem = M::E>>(
    operands: &F,
    ids: &[PointOffsetType],
    layout: &Panels,
    out: &mut [M::E],
    aux: &mut [Aux],
    decoded: &mut AlignedBuf,
) -> Result<(), F::Error> {
    const { assert!(GROUP.is_multiple_of(M::MR) && GROUP.is_multiple_of(M::NR)) };
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
            } else if lanes == w {
                // The other cells inside the vectors, one vector at a time: contiguous reads,
                // without the padding checks
                let inside = slice_cells.min((depth / ku).saturating_sub(first_cell));
                for lane in 0..w {
                    let src = &decoded[(p0 + lane) * depth + first_cell * ku..][..inside * ku];
                    match ku {
                        1 => {
                            for (g, &value) in src.iter().enumerate() {
                                dst[g * w + lane] = value;
                            }
                        }
                        // Not `copy_from_slice`, which calls `memcpy` for a few elements
                        _ => {
                            for (g, cell) in src.chunks_exact(ku).enumerate() {
                                let dst = &mut dst[(g * w + lane) * ku..][..ku];
                                for (d, &value) in dst.iter_mut().zip(cell) {
                                    *d = value;
                                }
                            }
                        }
                    }
                }
                g = inside;
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
    let (unpack_rows, unpack_cols) = (
        const { unpacking::<M>(true) },
        const { unpacking::<M>(false) },
    );
    // The packed panels of a side are read as `Operand` unless the tile unpacks them
    const {
        assert!(
            (!matches!(unpacking::<M>(true), Unpacking::No)
                && !matches!(unpacking::<M>(false), Unpacking::No))
                || size_of::<M::Operand>() == size_of::<M::E>()
        )
    };
    let depth = operands.depth();
    let combine = operands.combine();
    let param = operands.param();
    let (lists, s) = out.parts();
    s.row_aux.resize(m, Aux::default());
    s.col_aux.resize(n, Aux::default());
    let ldc = padded_stride(M::NC, size_of::<M::Acc>());
    let block = s.out.get::<M::Acc>(M::MC * ldc);
    // All rows once, instead of again for every column block, which made small blocks slow
    let a_layout = Panels::new(m, M::MR, depth, M::KC, M::KU);
    let a = s.rows.get::<M::E>(a_layout.len());
    pack::<M, F>(operands, rows, &a_layout, a, &mut s.row_aux, &mut s.decoded)?;
    // Values per panel and lane after unpacking, of a full depth slice
    let full_lane = M::unpacked_lane(a_layout.kc);
    // Unpacked rows: all of them, or a row block's panels of one depth slice
    let rows_unpacked: &mut [M::Operand] = match unpack_rows {
        Unpacking::No => &mut [],
        Unpacking::InBlocks => s
            .unpacked_rows
            .get(M::MC.div_ceil(M::MR) * (M::MR * full_lane + M::ROW_EXTRA)),
        Unpacking::Whole => unpack_whole::<M>(&a_layout, a, true, &mut s.unpacked_rows, param),
    };
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
        // Unpacked columns: all of the block, or one panel of one depth slice
        let cols_unpacked: &mut [M::Operand] = match unpack_cols {
            Unpacking::No => &mut [],
            Unpacking::InBlocks => s.unpacked_cols.get(M::NR * full_lane),
            Unpacking::Whole => unpack_whole::<M>(&b_layout, b, false, &mut s.unpacked_cols, param),
        };
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
                let k0 = b_layout.slice_start(slice);
                // Values per panel and lane after unpacking, and of a row panel
                let lane = M::unpacked_lane(kc);
                let row_panel = M::MR * lane + M::ROW_EXTRA;
                if unpack_rows == Unpacking::InBlocks {
                    for rp in rp0..rp1 {
                        // SAFETY: the panel within the packed rows, `MR` lanes of values and
                        // the extra ones within the buffer, sized for `MC` rows of a full
                        // slice. The caller checked the CPU.
                        unsafe {
                            M::unpack(
                                kc,
                                a.as_ptr().add(a_layout.offset(slice, rp)),
                                true,
                                rows_unpacked.as_mut_ptr().add((rp - rp0) * row_panel),
                                param,
                            );
                        }
                    }
                }
                for p in p0..p1 {
                    let b = match unpack_cols {
                        // The tile reads `E`, see `UNPACK`
                        Unpacking::No => {
                            b[b_layout.offset(slice, p - j0 / M::NR)..].as_ptr().cast()
                        }
                        Unpacking::InBlocks => {
                            let b = b[b_layout.offset(slice, p - j0 / M::NR)..].as_ptr();
                            // SAFETY: as for the rows
                            unsafe { M::unpack(kc, b, false, cols_unpacked.as_mut_ptr(), param) };
                            cols_unpacked.as_ptr()
                        }
                        Unpacking::Whole => cols_unpacked
                            [b_layout.unpacked_offset::<M>(slice, p - j0 / M::NR, 0)..]
                            .as_ptr(),
                    };
                    for rp in rp0..rp1 {
                        let a = match unpack_rows {
                            Unpacking::No => a[a_layout.offset(slice, rp)..].as_ptr().cast(),
                            Unpacking::InBlocks => rows_unpacked[(rp - rp0) * row_panel..].as_ptr(),
                            Unpacking::Whole => rows_unpacked
                                [a_layout.unpacked_offset::<M>(slice, rp, M::ROW_EXTRA)..]
                                .as_ptr(),
                        };
                        // SAFETY: both panels are within their packed or unpacked buffers,
                        // and the tile within `block`: rows `(rp - rp0) * MR..+MR <= MC`,
                        // columns `p * NR - j0..+NR <= NC <= ldc`. The caller checked the CPU.
                        unsafe {
                            M::tile(
                                kc,
                                a,
                                b,
                                block
                                    .as_mut_ptr()
                                    .add((rp - rp0) * M::MR * ldc + p * M::NR - j0),
                                ldc,
                                slice > 0,
                                param,
                                k0,
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
                    &s.row_aux[r],
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
    row_aux: &Aux,
    c0: usize,
    dots: &[A],
    col_aux: &[Aux],
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
    fixed: &'a Aux,
    aux: &'a [Aux],
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
        for ((out, &dot), aux) in out[..n].iter_mut().zip(dots).zip(&aux[..n]) {
            *out = if TRANSPOSED {
                combine.score(dot.to_f32(), aux, fixed)
            } else {
                combine.score(dot.to_f32(), fixed, aux)
            };
        }
    }
}
