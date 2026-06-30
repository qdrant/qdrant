use std::ops::Bound;
use std::ops::Bound::{Excluded, Included};

use serde_json::Value;

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

pub fn value_to_integer(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value.as_f64().and_then(|v| {
            // Reject NaN, infinity, and values outside i64 range.
            // Without the range check, out-of-range f64 values saturate during
            // the `v as i64` cast and then pass the round-trip equality test
            // (e.g. 9223372036854775808.0 → i64::MAX → 9223372036854775808.0).
            if v.is_finite() && v >= (i64::MIN as f64) && v < (i64::MAX as f64) {
                let int = v as i64;
                (int as f64 == v).then_some(int)
            } else {
                None
            }
        })
    })
}
