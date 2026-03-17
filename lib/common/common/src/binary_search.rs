use std::cmp::Ordering;

/// Binary search over an abstract range `0..len`, like [`slice::binary_search_by`]
/// but driven by an index-based fallible closure instead of a slice reference.
///
/// Returns `Ok(Ok(idx))` on exact match, `Ok(Err(idx))` for insertion point,
/// or `Err(e)` if the closure fails.
pub fn binary_search_by<E>(
    len: usize,
    mut f: impl FnMut(usize) -> Result<Ordering, E>,
) -> Result<Result<usize, usize>, E> {
    let mut lo = 0usize;
    let mut hi = len;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match f(mid)? {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(lo))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_found() {
        let data = [1, 3, 5, 7, 9];
        let r: Result<_, ()> = binary_search_by(data.len(), |i| Ok(data[i].cmp(&5)));
        assert_eq!(r, Ok(Ok(2)));
    }

    #[test]
    fn test_not_found() {
        let data = [1, 3, 5, 7, 9];
        let r: Result<_, ()> = binary_search_by(data.len(), |i| Ok(data[i].cmp(&4)));
        assert_eq!(r, Ok(Err(2)));
    }

    #[test]
    fn test_empty() {
        let r: Result<_, ()> = binary_search_by(0, |_| unreachable!());
        assert_eq!(r, Ok(Err(0)));
    }

    #[test]
    fn test_error_propagation() {
        let r = binary_search_by(10, |_| Err("boom"));
        assert_eq!(r, Err("boom"));
    }

    #[test]
    fn test_matches_std() {
        let data: Vec<i32> = (0..100).map(|i| i * 3).collect();
        for target in -5..310 {
            let expected = data.binary_search_by(|x| x.cmp(&target));
            let actual: Result<_, ()> = binary_search_by(data.len(), |i| Ok(data[i].cmp(&target)));
            assert_eq!(actual, Ok(expected), "mismatch for target={target}");
        }
    }
}
