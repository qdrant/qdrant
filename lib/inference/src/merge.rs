use itertools::Itertools;

/// 2-way merge of lists with positions. Returns `None` if an item is left out.
pub(crate) fn merge_position_items<I>(
    left: impl IntoIterator<Item = I>,
    left_pos: Vec<usize>,
    right: impl IntoIterator<Item = I>,
    right_pos: Vec<usize>,
) -> Option<Vec<I>> {
    let left_iter = left.into_iter().zip(left_pos);
    let right_iter = right.into_iter().zip(right_pos);

    let mut i = 0; // Check that we cover all items and don't skip any.
    left_iter
        .merge_by(right_iter, |l: &(I, usize), r: &(I, usize)| l.1 < r.1)
        .map(|item| {
            if item.1 == i {
                i += 1;
                Some(item.0)
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_position_items() {
        let (left, right): ((Vec<_>, Vec<_>), (Vec<_>, Vec<_>)) =
            (0..1000).map(|i| (i, i)).partition(|i| i.0 % 7 == 0);
        let merged = merge_position_items(left.0, left.1, right.0, right.1);
        assert_eq!(merged, Some((0..1000).collect::<Vec<_>>()));
    }

    #[test]
    fn test_merge_position_items_fail() {
        let (left, mut right): ((Vec<_>, Vec<_>), (Vec<_>, Vec<_>)) =
            (0..1000).map(|i| (i, i)).partition(|i| i.0 % 7 == 0);

        right.0.remove(5);
        right.1.remove(5);

        let merged = merge_position_items(left.0, left.1, right.0, right.1);

        // We were missing an item and therefore expect `None`.
        assert_eq!(merged, None);
    }
}
