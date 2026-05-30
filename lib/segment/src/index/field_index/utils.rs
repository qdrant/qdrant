use std::ops::Bound;
use std::ops::Bound::{Excluded, Included};

pub fn check_boundaries<T>(start: &Bound<T>, end: &Bound<T>) -> bool
where
    T: PartialOrd,
{
    match (&start, &end) {
        (Excluded(s), Excluded(e)) if s >= e => {
            // range start and end are equal and excluded in BTreeMap
            return false;
        }
        (Included(s) | Excluded(s), Included(e) | Excluded(e)) if s > e => {
            //range start is greater than range end
            return false;
        }
        _ => {}
    }
    true
}
