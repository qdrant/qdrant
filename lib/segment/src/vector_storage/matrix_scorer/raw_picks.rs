//! A part of a job's result as raw arrays, the way a scorer outside of the CPU kernels hands it
//! over, see [`MatrixPicks::fill_raw`].
//!
//! [`MatrixPicks::fill_raw`]: super::MatrixPicks::fill_raw

/// The column of an unused slot in [`RawPicks`]: a row with fewer picks than slots ends at the
/// first one. A column and not a score, since negative infinity is a legitimate score. A real
/// column position never equals it: a job would need more than 4,294,967,295 columns.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "marks unused slots of the GPU scorer's results, which comes later"
    )
)]
pub(crate) const NO_PICK: u32 = u32::MAX;

/// Consecutive rows of a job's result as two arrays, columns and scores, not interleaved:
/// `stride` slots per row, best first.
///
/// - Row `r` of the part is slots `r * stride..(r + 1) * stride` of `cols` and of `scores`.
/// - A row with fewer picks than slots ends at the first column equal to [`NO_PICK`]. The slots
///   after it are unspecified, and so are those after the `k` of the job.
/// - Picks follow the rules of [`MatrixScorer::top_k`]: best first, ties to the lower column
///   position, never a NaN score. So the first `k` of a row are its `k` best.
///
/// [`MatrixScorer::top_k`]: super::MatrixScorer::top_k
#[derive(Debug)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "filled by the GPU scorer, which comes later")
)]
pub(crate) struct RawPicks<'a> {
    /// Rows of the part. Explicit, because `stride` may be 0, and then the arrays are empty
    /// whatever the rows.
    rows: usize,
    /// Slots per row, at least the `k` of the job.
    stride: usize,
    /// Column positions, `stride` per row.
    cols: &'a [u32],
    /// Scores, parallel to `cols`.
    scores: &'a [f32],
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "filled by the GPU scorer, which comes later")
)]
impl<'a> RawPicks<'a> {
    /// `rows` rows of `stride` slots each in `cols` and in `scores`.
    ///
    /// Panics, in every build, unless both arrays have exactly `rows * stride` slots.
    pub(crate) fn new(rows: usize, stride: usize, cols: &'a [u32], scores: &'a [f32]) -> Self {
        let slots = rows.checked_mul(stride);
        assert_eq!(
            Some(cols.len()),
            slots,
            "a raw part of {rows} rows of {stride} slots needs as many columns",
        );
        assert_eq!(
            scores.len(),
            cols.len(),
            "a raw part needs a score per column",
        );
        Self {
            rows,
            stride,
            cols,
            scores,
        }
    }

    /// Number of rows of the part.
    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    /// Slots per row.
    pub(super) fn stride(&self) -> usize {
        self.stride
    }

    /// Row `r`'s picks among its first `k` slots, up to the first [`NO_PICK`], and their
    /// scores. `k` is at most the stride.
    ///
    /// Debug builds check the picks: best first, ties to the lower column, so no column twice
    /// on a tie, and no NaN.
    pub(super) fn row(&self, r: usize, k: usize) -> (&'a [u32], &'a [f32]) {
        let start = r * self.stride;
        let cols = &self.cols[start..start + k];
        let len = cols.iter().position(|&col| col == NO_PICK).unwrap_or(k);
        let cols = &cols[..len];
        let scores = &self.scores[start..start + len];
        debug_assert!(
            !scores.iter().any(|score| score.is_nan()),
            "row {r} of a raw part picks a NaN score",
        );
        debug_assert!(
            std::iter::zip(cols, scores).is_sorted_by(|(col_a, score_a), (col_b, score_b)| {
                score_a > score_b || (score_a == score_b && col_a < col_b)
            }),
            "row {r} of a raw part isn't best first with ties to the lower column",
        );
        (cols, scores)
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use rstest::rstest;

    use super::*;
    use crate::vector_storage::matrix_scorer::MatrixPicks;

    const NAN: f32 = f32::NAN;

    fn rows_of(picks: &MatrixPicks) -> Vec<(Vec<u32>, Vec<f32>)> {
        let row = |r| picks.row(r);
        (0..picks.rows())
            .map(|r| (row(r).0.to_vec(), row(r).1.to_vec()))
            .collect()
    }

    /// A result keeps the first `k` slots of each row, up to the first [`NO_PICK`], and reads
    /// none after either. A part replaces its rows, also by fewer picks, and no others.
    #[test]
    fn fill_keeps_the_first_k_up_to_no_pick() {
        let mut picks = MatrixPicks::new(2, 4);
        // Rows 1 to 3, of 3 slots: full with a tie, with one pick, without picks
        let cols = [4, 7, 9, 2, NO_PICK, 0, NO_PICK, 0, 1];
        let scores = [0.5, 0.5, NAN, -f32::INFINITY, NAN, 9.0, NAN, 9.0, 9.0];
        picks.fill_raw(1, &RawPicks::new(3, 3, &cols, &scores));
        let filled = vec![
            (vec![], vec![]),
            (vec![4, 7], vec![0.5, 0.5]),
            (vec![2], vec![-f32::INFINITY]),
            (vec![], vec![]),
        ];
        assert_eq!(rows_of(&picks), filled);

        // Row 1 again with one pick, and a part without rows at the end
        picks.fill_raw(1, &RawPicks::new(1, 2, &[5, NO_PICK], &[1.0, NAN]));
        picks.fill_raw(4, &RawPicks::new(0, 2, &[], &[]));
        let mut refilled = filled;
        refilled[1] = (vec![5], vec![1.0]);
        assert_eq!(rows_of(&picks), refilled);

        // A `k` of 0 keeps nothing, also of a part without slots
        let mut none = MatrixPicks::new(0, 2);
        none.fill_raw(0, &RawPicks::new(2, 0, &[], &[]));
        assert_eq!(rows_of(&none), vec![(vec![], vec![]); 2]);
    }

    /// The arrays of a part have a slot per row and stride each, no fewer and no more.
    #[rstest]
    #[case(5, 5)]
    #[case(7, 7)]
    #[case(6, 5)]
    #[case(6, 7)]
    #[should_panic(expected = "a raw part")]
    fn arrays_must_match_the_rows(#[case] cols: usize, #[case] scores: usize) {
        let _ = RawPicks::new(2, 3, &vec![0; cols], &vec![0.0; scores]);
    }

    /// A part that doesn't fit panics before anything is written: with fewer slots per row
    /// than `k`, or past the job's rows, also without rows.
    #[rstest]
    #[case::too_few_slots(0, 3, 1)]
    #[case::past_the_end(2, 2, 2)]
    #[case::empty_past_the_end(4, 0, 2)]
    fn a_part_that_does_not_fit_writes_nothing(
        #[case] at: usize,
        #[case] rows: usize,
        #[case] stride: usize,
    ) {
        let mut picks = MatrixPicks::new(2, 3);
        let scores = [3.0, 2.0, 3.0, 2.0, 3.0, 2.0];
        picks.fill_raw(0, &RawPicks::new(3, 2, &[0, 1, 1, 0, 2, 1], &scores));
        let before = rows_of(&picks);
        // Valid rows whatever the slots read: a tie in ascending columns
        let cols: Vec<u32> = (5..).take(rows * stride).collect();
        let scores = vec![9.0; rows * stride];
        let failed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            picks.fill_raw(at, &RawPicks::new(rows, stride, &cols, &scores));
        }));
        assert!(failed.is_err());
        assert_eq!(rows_of(&picks), before);
    }

    /// Debug builds check the picks of a row: best first, ties to the lower column, no NaN.
    #[cfg(debug_assertions)]
    #[rstest]
    #[case::worse_first(&[0, 1], &[1.0, 2.0])]
    #[case::tie_to_the_higher_column(&[3, 1], &[1.0, 1.0])]
    #[case::tie_of_one_column(&[2, 2], &[1.0, 1.0])]
    #[case::nan(&[0, 1], &[1.0, NAN])]
    #[should_panic(expected = "of a raw part")]
    fn picks_must_follow_the_rules(#[case] cols: &[u32], #[case] scores: &[f32]) {
        MatrixPicks::new(2, 1).fill_raw(0, &RawPicks::new(1, 2, cols, scores));
    }
}
