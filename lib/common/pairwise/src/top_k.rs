/// Per-row top-k of a matrix call. Reused across calls, so its buffers are allocated once.
///
/// Not a list of [`common::top_k::TopK`], one per row, because:
/// - Ties there go to the point pushed first. Cache-blocked kernels push a row's columns out
///   of order, interleaving direct and transposed pushes by block, so the picks, and with them
///   the graph, would depend on block sizes tuned per CPU. Here ties go to the lower column.
///   Search relies on the tie rule there, so it can't change.
/// - It has no `clear()` and `into_vec` consumes it, so every call would allocate a list per
///   row.
///
/// Speed alone doesn't decide it: measured with a blocked kernel and an 8-wide prefilter in
/// front, a list per row cost 0.7% of an f32 leaf and 1.6% of a 1-bit leaf at dim 960.
#[derive(Clone, Debug)]
pub struct TopK {
    k: usize,
    /// Filled entries per row.
    len: Vec<u32>,
    /// Column positions, `k` per row, best first.
    cols: Vec<u32>,
    /// Scores, parallel to `cols`.
    scores: Vec<f32>,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            len: Vec::new(),
            cols: Vec::new(),
            scores: Vec::new(),
        }
    }

    pub fn k(&self) -> usize {
        self.k
    }

    /// Number of rows of the last call.
    pub fn rows(&self) -> usize {
        self.len.len()
    }

    /// Row `r`'s column positions and scores, best first, ties to the lower column position.
    pub fn row(&self, r: usize) -> (&[u32], &[f32]) {
        let start = r * self.k;
        let end = start + self.len[r] as usize;
        (&self.cols[start..end], &self.scores[start..end])
    }

    /// Clears the lists for a call with `rows` rows.
    pub(crate) fn reset(&mut self, rows: usize) {
        self.len.clear();
        self.len.resize(rows, 0);
        self.cols.resize(rows * self.k, 0);
        self.scores.resize(rows * self.k, 0.0);
    }

    /// Offers column `col` with `score` to row `r`.
    pub(crate) fn push(&mut self, r: usize, col: u32, score: f32) {
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
    }
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
