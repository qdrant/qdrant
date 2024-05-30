/// Convert a shape [N, M] to [M, N].
///
/// # Example
///
/// [
///    [1, 2, 3],
///    [4, 5, 6],
/// ]
///
/// to
///
/// [
///    [1, 4],
///    [2, 5],
///    [3, 6],
/// ]
///
/// # Panics
///
/// Panics if the input is not a rectangle.
pub fn transpose<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    assert!(!v.is_empty());
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
    let res = (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .map(|n| n.next().expect("Input is rectangular"))
                .collect()
        })
        .collect();

    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transpose() {
        let v = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let res = transpose(v);
        let expected = [vec![1, 4], vec![2, 5], vec![3, 6]];
        for (i, column) in res.iter().enumerate() {
            assert_eq!(column, &expected[i]);
        }
    }
}
