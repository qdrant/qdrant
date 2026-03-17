use std::cmp::Ordering;
use std::ops::Range;

use num_traits::PrimInt;

/// Binary search, similar to [`slice::binary_search_by`], but generalized to
/// work with fallible closures instead of slices.
pub fn binary_search_by<Idx: PrimInt, F, E>(
    mut range: Range<Idx>,
    mut f: F,
) -> Result<Result<Idx, Idx>, E>
where
    F: FnMut(Idx) -> Result<Ordering, E>,
{
    let two = Idx::one() + Idx::one();
    while range.start < range.end {
        let mid = range.start + (range.end - range.start) / two;
        match f(mid)? {
            Ordering::Less => range.start = mid + Idx::one(),
            Ordering::Greater => range.end = mid,
            Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(range.start))
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
