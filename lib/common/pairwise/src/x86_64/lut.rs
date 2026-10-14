//! Binary codes: the number of differing bits of every pair by nibble lookups, without XOR.
//!
//! Columns are nibble-sliced: per group of 32 columns and nibble position `q`, 32 bytes, byte
//! `j` holding nibble `q` of column `j`. Each nibble of a row selects a 16-entry table of
//! `popcount(row nibble ^ x)`, so one `vpshufb` of the table with the columns' nibbles counts
//! the differing bits of 32 nibble pairs, and one `vpaddb` sums them.

use std::arch::x86_64::*;

use common::types::PointOffsetType;

use crate::blocked::{GROUP, emit};
use crate::scratch::bits_as_bytes;
use crate::{Bits, Cols, MatrixOperands, TopK};

/// `POPCOUNT_XOR[v][x] = popcount(v ^ x)` for nibbles.
const POPCOUNT_XOR: [[u8; 16]; 16] = {
    let mut table = [[0; 16]; 16];
    let mut v = 0;
    while v < 16 {
        let mut x = 0;
        while x < 16 {
            table[v][x] = (v ^ x).count_ones() as u8;
            x += 1;
        }
        v += 1;
    }
    table
};

/// Rows of a tile.
const TILE_ROWS: usize = 6;
/// Groups of 32 columns in a tile.
const TILE_GROUPS: usize = 2;
const TILE_COLS: usize = TILE_GROUPS * 32;
/// Rows per block, whose tables stay in L1: a multiple of `TILE_ROWS`.
const BLOCK_ROWS: usize = 12;
const TILES_PER_BLOCK: usize = BLOCK_ROWS / TILE_ROWS;
/// Nibble positions per kernel call: byte counters gain at most 4 per step.
const MAX_NIBBLES: usize = 63;
/// Partial u16 counts of one tile: `[TILE_ROWS][TILE_GROUPS][low, high][16]`.
const TILE_PARTIAL: usize = TILE_ROWS * TILE_GROUPS * 2 * 16;

/// See the module. `TILE_ROWS` rows by `TILE_COLS` columns, over `nibbles <= 63` positions.
/// `tables` holds `[nibble][TILE_ROWS][16]` lookup tables, `cols` the sliced groups,
/// `group_stride` bytes apart. Byte counts are widened into the u16 partial counts at
/// `partial` (`TILE_PARTIAL`, 32-byte aligned), in unpack order, added unless `first`.
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn tile(
    nibbles: usize,
    tables: *const u8,
    cols: *const u8,
    group_stride: usize,
    partial: *mut u16,
    first: bool,
) {
    debug_assert!(nibbles <= MAX_NIBBLES);
    unsafe {
        let zero = _mm256_setzero_si256();
        let mut sums = [[_mm256_setzero_si256(); TILE_GROUPS]; TILE_ROWS];
        for q in 0..nibbles {
            let mut col = [_mm256_setzero_si256(); TILE_GROUPS];
            for (g, col) in col.iter_mut().enumerate() {
                *col = _mm256_load_si256(cols.add(g * group_stride + q * 32).cast());
            }
            for (r, sums) in sums.iter_mut().enumerate() {
                let table = _mm256_broadcastsi128_si256(_mm_loadu_si128(
                    tables.add((q * TILE_ROWS + r) * 16).cast(),
                ));
                for g in 0..TILE_GROUPS {
                    sums[g] = _mm256_add_epi8(sums[g], _mm256_shuffle_epi8(table, col[g]));
                }
            }
        }
        for (r, sums) in sums.iter().enumerate() {
            for (g, &sum) in sums.iter().enumerate() {
                let p = partial.add((r * TILE_GROUPS + g) * 2 * 16);
                let mut low = _mm256_unpacklo_epi8(sum, zero);
                let mut high = _mm256_unpackhi_epi8(sum, zero);
                if !first {
                    low = _mm256_add_epi16(low, _mm256_load_si256(p.cast()));
                    high = _mm256_add_epi16(high, _mm256_load_si256(p.add(16).cast()));
                }
                _mm256_store_si256(p.cast(), low);
                _mm256_store_si256(p.add(16).cast(), high);
            }
        }
    }
}

/// One group's `[low, high][16]` partial counts back in column order: `vpunpcklbw` holds
/// columns 0..8 and 16..24, `vpunpckhbw` 8..16 and 24..32.
#[inline(always)]
fn unpack_group(src: &[u16], dst: &mut [u16]) {
    let (src, dst) = (&src[..32], &mut dst[..32]);
    dst[0..8].copy_from_slice(&src[0..8]);
    dst[16..24].copy_from_slice(&src[8..16]);
    dst[8..16].copy_from_slice(&src[16..24]);
    dst[24..32].copy_from_slice(&src[24..32]);
}

/// Lookup tables of a block of `BLOCK_ROWS` rows (`bytes` each), `[tile][nibble][TILE_ROWS][16]`.
#[inline(always)]
fn fill_tables(rows: &[u8], bytes: usize, tables: &mut [u8]) {
    let nibbles = 2 * bytes;
    for r in 0..BLOCK_ROWS {
        let (t, tr) = (r / TILE_ROWS, r % TILE_ROWS);
        let row = &rows[r * bytes..(r + 1) * bytes];
        let base = t * nibbles * TILE_ROWS * 16 + tr * 16;
        for (p, &x) in row.iter().enumerate() {
            let low = base + 2 * p * TILE_ROWS * 16;
            let high = base + (2 * p + 1) * TILE_ROWS * 16;
            tables[low..low + 16].copy_from_slice(&POPCOUNT_XOR[usize::from(x & 0x0F)]);
            tables[high..high + 16].copy_from_slice(&POPCOUNT_XOR[usize::from(x >> 4)]);
        }
    }
}

/// Writes the nibbles of `vector` into lane `lane` of a sliced group (`[2 * bytes][32]`).
#[inline(always)]
fn slice_nibbles(vector: &[u8], group: &mut [u8], lane: usize) {
    for (p, &x) in vector.iter().enumerate() {
        group[2 * p * 32 + lane] = x & 0x0F;
        group[(2 * p + 1) * 32 + lane] = x >> 4;
    }
}

/// [`top_k`](crate::top_k) for binary codes. Blocked so the hot data stays in L1: a block of
/// `BLOCK_ROWS` rows, whose tables are built once → up to 63 nibble positions → tiles of 64
/// columns → tiles of 6 rows. Needs `bytes * 8 <= u16::MAX`.
#[inline(always)]
pub(super) fn top_k<F: MatrixOperands<Elem = Bits>>(
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
    let bytes = operands.depth();
    assert!(bytes * 8 <= usize::from(u16::MAX));
    let combine = operands.combine();
    let nibbles = 2 * bytes;
    let group_stride = nibbles * 32;
    let n_pad = n.next_multiple_of(TILE_COLS);
    let m_pad = m.next_multiple_of(BLOCK_ROWS);
    let (lists, s) = out.parts();
    s.row_aux.resize(m, 0.0);
    s.col_aux.resize(n, 0.0);

    // Rows as they are, padded with zeros to whole blocks
    let raw = s.rows.get::<Bits>(m_pad * bytes);
    for (g, ids) in rows.chunks(GROUP).enumerate() {
        let start = g * GROUP;
        operands.decode_batch(
            ids,
            &mut raw[start * bytes..(start + ids.len()) * bytes],
            &mut s.row_aux[start..start + ids.len()],
        )?;
    }
    raw[m * bytes..].fill(Bits(0));
    let raw = bits_as_bytes(raw);

    // Columns sliced, the lanes past `n` zero
    let sliced = s.cols.get::<u8>(n_pad / 32 * group_stride);
    sliced[n / 32 * group_stride..].fill(0);
    if triangle {
        s.col_aux.copy_from_slice(&s.row_aux);
        for (j, vector) in raw[..m * bytes].chunks(bytes).enumerate() {
            slice_nibbles(vector, &mut sliced[j / 32 * group_stride..], j % 32);
        }
    } else {
        let decoded = s.decoded.get::<Bits>(GROUP * bytes);
        for (g, ids) in cols.chunks(GROUP).enumerate() {
            let start = g * GROUP;
            let decoded = &mut decoded[..ids.len() * bytes];
            operands.decode_batch(ids, decoded, &mut s.col_aux[start..start + ids.len()])?;
            for (i, vector) in bits_as_bytes(decoded).chunks(bytes).enumerate() {
                let j = start + i;
                slice_nibbles(vector, &mut sliced[j / 32 * group_stride..], j % 32);
            }
        }
    }

    let tables = s
        .tables
        .get::<u8>(TILES_PER_BLOCK * nibbles * TILE_ROWS * 16);
    let col_tiles = n_pad / TILE_COLS;
    let partial = s
        .partial
        .get::<u16>(TILES_PER_BLOCK * col_tiles * TILE_PARTIAL);
    let counts = s.out.get::<u16>(BLOCK_ROWS * n_pad);
    for r0 in (0..m).step_by(BLOCK_ROWS) {
        // In the triangle, only column tiles from the one holding the block's first row
        let c0 = if triangle {
            r0 / TILE_COLS * TILE_COLS
        } else {
            0
        };
        fill_tables(&raw[r0 * bytes..(r0 + BLOCK_ROWS) * bytes], bytes, tables);
        // Nibble positions outside the columns: each slice of the tables stays in L1 while
        // every column tile streams through it once
        for q0 in (0..nibbles).step_by(MAX_NIBBLES) {
            let q_count = (nibbles - q0).min(MAX_NIBBLES);
            for c in (c0..n_pad).step_by(TILE_COLS) {
                for t in 0..TILES_PER_BLOCK {
                    // SAFETY: the tables of tile `t` from nibble `q0` hold `q_count` steps,
                    // the column tile's groups `q_count * 32` bytes each from `q0 * 32`, and
                    // its partial counts `TILE_PARTIAL` values. AVX2 is enabled by the caller.
                    unsafe {
                        tile(
                            q_count,
                            tables.as_ptr().add((t * nibbles + q0) * TILE_ROWS * 16),
                            sliced.as_ptr().add(c / 32 * group_stride + q0 * 32),
                            group_stride,
                            partial
                                .as_mut_ptr()
                                .add((t * col_tiles + c / TILE_COLS) * TILE_PARTIAL),
                            q0 == 0,
                        );
                    }
                }
            }
        }
        for t in 0..TILES_PER_BLOCK {
            for tr in 0..TILE_ROWS {
                let row = &mut counts[(t * TILE_ROWS + tr) * n_pad..][..n_pad];
                for c in (c0..n_pad).step_by(TILE_COLS) {
                    let tile = &partial[(t * col_tiles + c / TILE_COLS) * TILE_PARTIAL..];
                    for g in 0..TILE_GROUPS {
                        let src = &tile[(tr * TILE_GROUPS + g) * 2 * 16..];
                        unpack_group(src, &mut row[c + g * 32..]);
                    }
                }
            }
        }
        for r in r0..(r0 + BLOCK_ROWS).min(m) {
            let dots = &counts[(r - r0) * n_pad + c0..(r - r0) * n_pad + n];
            emit(
                lists,
                combine,
                triangle,
                r,
                s.row_aux[r],
                c0,
                dots,
                &s.col_aux[c0..n],
                &mut s.scores,
                &mut s.scores_t,
            );
        }
    }
    Ok(())
}
