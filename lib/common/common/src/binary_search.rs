use std::cmp::Ordering;
use std::ops::Range;

/// Binary search, similar to [`slice::binary_search_by`], but generalized to
/// work with fallible closures instead of slices.
pub fn binary_search_by<F, E>(mut range: Range<usize>, mut f: F) -> Result<Result<usize, usize>, E>
where
    F: FnMut(usize) -> Result<Ordering, E>,
{
    while range.start < range.end {
        let mid = range.start + (range.end - range.start) / 2;
        match f(mid)? {
            Ordering::Less => range.start = mid + 1,
            Ordering::Greater => range.end = mid,
            Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(range.start))
}

/// Partition point, similar to [`slice::partition_point`], but generalized to
/// work with fallible closures instead of slices.
pub fn partition_point<P, E>(range: Range<usize>, mut pred: P) -> Result<usize, E>
where
    P: FnMut(usize) -> Result<bool, E>,
{
    let f = |x| pred(x).map(|b| if b { Ordering::Less } else { Ordering::Greater });
    Ok(binary_search_by(range, f)?.unwrap_or_else(|i| i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_found() {
        let data = [1, 3, 5, 7, 9];
        let r: Result<_, ()> = binary_search_by(0..data.len(), |i| Ok(data[i].cmp(&5)));
        assert_eq!(r, Ok(Ok(2)));
    }

    #[test]
    fn test_not_found() {
        let data = [1, 3, 5, 7, 9];
        let r: Result<_, ()> = binary_search_by(0..data.len(), |i| Ok(data[i].cmp(&4)));
        assert_eq!(r, Ok(Err(2)));
    }

    #[test]
    fn test_empty() {
        let r: Result<_, ()> = binary_search_by(0..0, |_| unreachable!());
        assert_eq!(r, Ok(Err(0)));
    }

    #[test]
    fn test_error_propagation() {
        let r = binary_search_by(0..10, |_| Err("boom"));
        assert_eq!(r, Err("boom"));
    }
}
