use crate::scratch::Scratch;

/// Per-row top-k of a matrix call. Reused across calls, so its buffers are allocated once.
///
/// Not a list of [`common::top_k::TopK`], one per row, because:
/// - Ties there go to the point pushed first. The kernels here offer each row its columns in
///   increasing order, so that's the same rule today, but the picks, and with them the graph,
///   would then depend on the loop order of every kernel. Here ties go to the lower column,
///   in any order. Search relies on the tie rule there, so it can't change.
/// - It has no `clear()` and `into_vec` consumes it, so every call would allocate a list per
///   row.
///
/// Speed alone doesn't decide it: measured with a blocked kernel and an 8-wide prefilter in
/// front, a list per row cost 0.7% of an f32 leaf and 1.6% of a 1-bit leaf at dim 960.
#[derive(Clone, Debug)]
pub struct TopK {
    lists: Lists,
    scratch: Scratch,
}

/// The per-row lists of a [`TopK`].
#[derive(Clone, Debug)]
pub(crate) struct Lists {
    k: usize,
    /// Filled entries per row.
    len: Vec<u32>,
    /// Column positions, `k` per row, best first.
    cols: Vec<u32>,
    /// Scores, parallel to `cols`.
    scores: Vec<f32>,
    /// Per row, the score a candidate needs at least: the k-th best once the row is full,
    /// `-inf` before. Kernels compare 8 candidates against it at a time.
    thr: Vec<f32>,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        Self {
            lists: Lists {
                k,
                len: Vec::new(),
                cols: Vec::new(),
                scores: Vec::new(),
                thr: Vec::new(),
            },
            scratch: Scratch::default(),
        }
    }

    pub fn k(&self) -> usize {
        self.lists.k
    }

    /// Number of rows of the last call.
    pub fn rows(&self) -> usize {
        self.lists.len.len()
    }

    /// Row `r`'s column positions and scores, best first, ties to the lower column position.
    /// NaN scores are never picked.
    pub fn row(&self, r: usize) -> (&[u32], &[f32]) {
        let lists = &self.lists;
        let start = r * lists.k;
        let end = start + lists.len[r] as usize;
        (&lists.cols[start..end], &lists.scores[start..end])
    }

    /// Clears the lists for a call with `rows` rows.
    pub(crate) fn reset(&mut self, rows: usize) {
        self.lists.reset(rows);
    }

    /// Offers column `col` with `score` to row `r`.
    pub(crate) fn push(&mut self, r: usize, col: u32, score: f32) {
        self.lists.push(r, col, score);
    }

    pub(crate) fn parts(&mut self) -> (&mut Lists, &mut Scratch) {
        (&mut self.lists, &mut self.scratch)
    }
}

impl Lists {
    fn reset(&mut self, rows: usize) {
        let k = self.k;
        self.len.clear();
        self.len.resize(rows, 0);
        self.cols.resize(rows * k, 0);
        self.scores.resize(rows * k, 0.0);
        self.thr.clear();
        self.thr.resize(rows, f32::NEG_INFINITY);
    }

    /// Offers column `col` with `score` to row `r`.
    #[inline]
    pub(crate) fn push(&mut self, r: usize, col: u32, score: f32) {
        // Also rejects NaN
        if score
            .partial_cmp(&self.thr[r])
            .is_none_or(|order| order.is_lt())
        {
            return;
        }
        let k = self.k;
        let base = r * k;
        let len = self.len[r] as usize;
        let better =
            |s: f32, c: u32, than_s: f32, than_c: u32| s > than_s || (s == than_s && c < than_c);
        if len == k {
            if k == 0
                || !better(
                    score,
                    col,
                    self.scores[base + k - 1],
                    self.cols[base + k - 1],
                )
            {
                return;
            }
        } else {
            self.len[r] += 1;
        }
        let mut p = len.min(k - 1);
        while p > 0
            && better(
                score,
                col,
                self.scores[base + p - 1],
                self.cols[base + p - 1],
            )
        {
            self.scores[base + p] = self.scores[base + p - 1];
            self.cols[base + p] = self.cols[base + p - 1];
            p -= 1;
        }
        self.scores[base + p] = score;
        self.cols[base + p] = col;
        if self.len[r] as usize == k {
            self.thr[r] = self.scores[base + k - 1];
        }
    }

    /// Offers `scores[j]` as column `c0 + j` to row `r`, for every `c0 + j >= c_min`.
    #[inline(always)]
    pub(crate) fn push_row(&mut self, r: usize, c0: usize, scores: &[f32], c_min: usize) {
        let n = scores.len();
        let mut j = c_min.saturating_sub(c0).min(n);
        while j + 8 <= n {
            let chunk: [f32; 8] = scores[j..j + 8].try_into().unwrap();
            let mut mask = ge_mask(&chunk, &[self.thr[r]; 8]);
            while mask != 0 {
                let b = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                self.push(r, (c0 + j + b) as u32, chunk[b]);
            }
            j += 8;
        }
        for (j, &score) in scores.iter().enumerate().skip(j) {
            self.push(r, (c0 + j) as u32, score);
        }
    }

    /// Offers `scores[j]` as column `r` to row `c0 + j`, for every `c0 + j >= c_min`: the
    /// transposed pairs of a triangle.
    #[inline(always)]
    pub(crate) fn push_col(&mut self, r: usize, c0: usize, scores: &[f32], c_min: usize) {
        let n = scores.len();
        let mut j = c_min.saturating_sub(c0).min(n);
        while j + 8 <= n {
            let chunk: [f32; 8] = scores[j..j + 8].try_into().unwrap();
            let thr: [f32; 8] = self.thr[c0 + j..c0 + j + 8].try_into().unwrap();
            let mut mask = ge_mask(&chunk, &thr);
            while mask != 0 {
                let b = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                self.push(c0 + j + b, r as u32, chunk[b]);
            }
            j += 8;
        }
        for (j, &score) in scores.iter().enumerate().skip(j) {
            self.push(c0 + j, r as u32, score);
        }
    }
}

/// Bit `i` set if `scores[i] >= thr[i]`. Compiles to one vector compare where available.
#[inline(always)]
fn ge_mask(scores: &[f32; 8], thr: &[f32; 8]) -> u32 {
    let mut mask = 0;
    for i in 0..8 {
        mask |= u32::from(scores[i] >= thr[i]) << i;
    }
    mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_best_first_ties_to_lower_column() {
        let mut top = TopK::new(3);
        top.reset(1);
        for (col, score) in [(5, 1.0), (2, 3.0), (7, 3.0), (1, 0.5), (0, 2.0), (3, 3.0)] {
            top.push(0, col, score);
        }
        assert_eq!(top.row(0), (&[2, 3, 7][..], &[3.0, 3.0, 3.0][..]));
    }

    #[test]
    fn rows_shorter_than_k() {
        let mut top = TopK::new(4);
        top.reset(2);
        top.push(1, 9, 1.0);
        top.push(1, 4, 2.0);
        assert_eq!(top.row(0), (&[][..], &[][..]));
        assert_eq!(top.row(1), (&[4, 9][..], &[2.0, 1.0][..]));
    }
}
