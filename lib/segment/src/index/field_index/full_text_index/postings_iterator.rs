pub fn intersect_btree_iterator<'a, T: Ord + Copy>(
    mut postings: Vec<&'a Vec<T>>,
) -> Box<dyn Iterator<Item = T> + 'a> {
    let smallest_posting_idx = postings
        .iter()
        .enumerate()
        .min_by_key(|(_idx, posting)| posting.len())
        .map(|(idx, _posting)| idx)
        .unwrap();
    let smallest_posting = postings.remove(smallest_posting_idx);

    let and_iter = smallest_posting
        .iter()
        .filter(move |doc_id| postings.iter().all(|posting| posting.contains(doc_id)))
        .copied();

    Box::new(and_iter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postings_iterator() {
        let v1: Vec<_> = vec![1, 2, 3, 4, 5];
        let v2: Vec<_> = vec![2, 4, 5, 6, 7];
        let v3: Vec<_> = vec![1, 2, 5, 6, 7];

        let postings = vec![&v1, &v2, &v3];
        let merged = intersect_btree_iterator(postings);

        let res = merged.collect::<Vec<_>>();

        assert_eq!(res, vec![2, 5]);
    }
}
