//! Evaluate a [`TrigramQuery`] over the postings of a trigram index: the ids
//! of the keys that may match.

use std::borrow::Cow;

use super::query::TrigramQuery;
use super::trigram::Trigram;
use crate::common::operation_error::OperationResult;

/// Keys a trigram query admits, by key id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Candidates {
    /// The query has no trigrams to select by: every key is a candidate.
    All,
    /// Ascending, without duplicates.
    Ids(Vec<u32>),
}

/// Trigram postings of a key dictionary: for every trigram, the ids of the
/// keys containing it.
pub trait TrigramPostings {
    /// Number of keys containing `trigram`; orders intersections, so an upper
    /// bound will do.
    fn postings_len(&self, trigram: Trigram) -> usize;

    /// Ids of the keys containing `trigram`, ascending.
    fn postings(&self, trigram: Trigram) -> OperationResult<Cow<'_, [u32]>>;
}

/// Ids of the keys that satisfy `query`.
pub fn candidates(
    query: &TrigramQuery,
    postings: &impl TrigramPostings,
) -> OperationResult<Candidates> {
    Ok(match query {
        TrigramQuery::All => Candidates::All,
        TrigramQuery::None => Candidates::Ids(Vec::new()),
        TrigramQuery::Trigram(trigram) => {
            Candidates::Ids(postings.postings(*trigram)?.into_owned())
        }
        TrigramQuery::And(children) => {
            // Most selective first, so the running intersection shrinks early
            // and can stop as soon as it is empty.
            let mut children: Vec<_> = children.iter().collect();
            children.sort_by_cached_key(|child| cost(child, postings));

            let mut result: Option<Vec<u32>> = None;
            for child in children {
                if result.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
                let ids = match child {
                    // Avoid materializing a borrowed list just to intersect it.
                    TrigramQuery::Trigram(trigram) => match result {
                        Some(acc) => {
                            result = Some(intersect(&acc, &postings.postings(*trigram)?));
                            continue;
                        }
                        None => postings.postings(*trigram)?.into_owned(),
                    },
                    child @ (TrigramQuery::All
                    | TrigramQuery::None
                    | TrigramQuery::And(_)
                    | TrigramQuery::Or(_)) => match candidates(child, postings)? {
                        Candidates::All => continue,
                        Candidates::Ids(ids) => ids,
                    },
                };
                result = Some(match result {
                    Some(acc) => intersect(&acc, &ids),
                    None => ids,
                });
            }
            result.map_or(Candidates::All, Candidates::Ids)
        }
        TrigramQuery::Or(children) => {
            let mut ids = Vec::new();
            for child in children {
                match candidates(child, postings)? {
                    Candidates::All => return Ok(Candidates::All),
                    Candidates::Ids(child_ids) => ids.extend(child_ids),
                }
            }
            ids.sort_unstable();
            ids.dedup();
            Candidates::Ids(ids)
        }
    })
}

/// Upper bound of the candidates of `query`; `usize::MAX` if unbounded.
fn cost(query: &TrigramQuery, postings: &impl TrigramPostings) -> usize {
    match query {
        TrigramQuery::All => usize::MAX,
        TrigramQuery::None => 0,
        TrigramQuery::Trigram(trigram) => postings.postings_len(*trigram),
        TrigramQuery::And(children) => children
            .iter()
            .map(|child| cost(child, postings))
            .min()
            .unwrap_or(usize::MAX),
        TrigramQuery::Or(children) => children
            .iter()
            .map(|child| cost(child, postings))
            .fold(0, usize::saturating_add),
    }
}

/// Intersection of two ascending id lists; gallops through the longer one
/// when their sizes are far apart.
fn intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut result = Vec::with_capacity(small.len());
    if large.len() / 32 > small.len() {
        let mut rest = large;
        for &id in small {
            match rest.binary_search(&id) {
                Ok(pos) => {
                    result.push(id);
                    rest = &rest[pos + 1..];
                }
                Err(pos) => rest = &rest[pos..],
            }
        }
    } else {
        let (mut i, mut j) = (0, 0);
        while i < small.len() && j < large.len() {
            match small[i].cmp(&large[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    result.push(small[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct Postings(HashMap<Trigram, Vec<u32>>);

    impl TrigramPostings for Postings {
        fn postings_len(&self, trigram: Trigram) -> usize {
            self.0.get(&trigram).map_or(0, Vec::len)
        }

        fn postings(&self, trigram: Trigram) -> OperationResult<Cow<'_, [u32]>> {
            Ok(Cow::Borrowed(
                self.0.get(&trigram).map_or(&[], Vec::as_slice),
            ))
        }
    }

    fn tri(n: u8) -> Trigram {
        Trigram::new([n, n, n])
    }

    #[test]
    fn evaluates_boolean_structure() {
        let postings = Postings(HashMap::from([
            (tri(1), vec![1, 2, 3, 4]),
            (tri(2), vec![2, 4, 6]),
            (tri(3), vec![7]),
        ]));
        let t = |n| TrigramQuery::Trigram(tri(n));
        let eval = |query: &TrigramQuery| candidates(query, &postings).unwrap();

        assert_eq!(eval(&TrigramQuery::All), Candidates::All);
        assert_eq!(eval(&TrigramQuery::None), Candidates::Ids(vec![]));
        assert_eq!(eval(&t(9)), Candidates::Ids(vec![]));
        assert_eq!(
            eval(&TrigramQuery::And(vec![t(1), t(2)])),
            Candidates::Ids(vec![2, 4]),
        );
        assert_eq!(
            eval(&TrigramQuery::Or(vec![t(2), t(3)])),
            Candidates::Ids(vec![2, 4, 6, 7]),
        );
        assert_eq!(
            eval(&TrigramQuery::And(vec![
                t(1),
                TrigramQuery::Or(vec![t(2), t(3)]),
            ])),
            Candidates::Ids(vec![2, 4]),
        );
        assert_eq!(
            eval(&TrigramQuery::Or(vec![t(3), TrigramQuery::All])),
            Candidates::All,
        );
        assert_eq!(
            eval(&TrigramQuery::And(vec![t(3), TrigramQuery::All])),
            Candidates::Ids(vec![7]),
        );
    }

    #[test]
    fn intersect_dense_and_galloping() {
        let large: Vec<u32> = (0..10_000).collect();
        assert_eq!(intersect(&[5, 500, 20_000], &large), vec![5, 500]);
        assert_eq!(intersect(&[1, 3, 5], &[2, 3, 4, 5]), vec![3, 5]);
        assert_eq!(intersect(&[], &large), Vec::<u32>::new());
    }
}
