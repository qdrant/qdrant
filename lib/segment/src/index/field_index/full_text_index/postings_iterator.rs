use common::types::PointOffsetType;

use super::posting_list::PostingList;

pub fn intersect_postings_iterator<'a>(
    mut postings: Vec<&'a PostingList>,
    filter: impl Fn(PointOffsetType) -> bool + 'a,
) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
    let smallest_posting_idx = postings
        .iter()
        .enumerate()
        .min_by_key(|(_idx, posting)| posting.len())
        .map(|(idx, _posting)| idx)
        .unwrap();
    let smallest_posting = postings.remove(smallest_posting_idx);

    let and_iter = smallest_posting
        .iter(filter)
        .filter(move |doc_id| postings.iter().all(|posting| posting.contains(doc_id)));

    Box::new(and_iter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postings_iterator() {
        let mut p1 = PostingList::default();
        p1.insert(1);
        p1.insert(2);
        p1.insert(3);
        p1.insert(4);
        p1.insert(5);
        let mut p2 = PostingList::default();
        p2.insert(2);
        p2.insert(4);
        p2.insert(5);
        p2.insert(5);
        let mut p3 = PostingList::default();
        p3.insert(1);
        p3.insert(2);
        p3.insert(5);
        p3.insert(6);
        p3.insert(7);

        let postings = vec![&p1, &p2, &p3];
        let merged = intersect_postings_iterator(postings, |_| true);

        let res = merged.collect::<Vec<_>>();

        assert_eq!(res, vec![2, 5]);
    }
}
