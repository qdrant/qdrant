use common::types::PointOffsetType;
use posting_list::{IdsPostingListView, PostingListView};

use super::posting_list::PostingList;

pub fn intersect_postings_iterator<'a>(
    mut postings: Vec<&'a PostingList>,
) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
    let smallest_posting_idx = postings
        .iter()
        .enumerate()
        .min_by_key(|(_idx, posting)| posting.len())
        .map(|(idx, _posting)| idx)
        .unwrap();
    let smallest_posting = postings.remove(smallest_posting_idx);

    let and_iter = smallest_posting
        .iter()
        .filter(move |doc_id| postings.iter().all(|posting| posting.contains(*doc_id)));

    Box::new(and_iter)
}

pub fn intersect_compressed_postings_iterator<'a>(
    mut postings: Vec<IdsPostingListView<'a>>,
    filter: impl Fn(PointOffsetType) -> bool + 'a,
) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
    let smallest_posting_idx = postings
        .iter()
        .enumerate()
        .min_by_key(|(_idx, posting)| posting.len())
        .map(|(idx, _posting)| idx)
        .unwrap();
    let smallest_posting = postings.remove(smallest_posting_idx);
    let smallest_posting_iterator = smallest_posting.into_iter();

    let mut posting_iterators = postings
        .into_iter()
        .map(PostingListView::into_iter)
        .collect::<Vec<_>>();

    let and_iter = smallest_posting_iterator
        .map(|elem| elem.id)
        .filter(move |id| {
            filter(*id)
                && posting_iterators.iter_mut().all(|posting_iterator| {
                    // Custom "contains" check, which leverages the fact that smallest posting is sorted,
                    // so the next id that must be in all postings is strictly greater than the previous one.
                    //
                    // This means that the other iterators can remember the last id they returned to avoid extra work
                    posting_iterator
                        .advance_until_greater_or_equal(*id)
                        .is_some_and(|elem| elem.id == *id)
                })
        });

    Box::new(and_iter)
}

#[cfg(test)]
mod tests {

    use posting_list::IdsPostingList;

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
        let merged = intersect_postings_iterator(postings);

        let res = merged.collect::<Vec<_>>();

        assert_eq!(res, vec![2, 5]);

        let p1_compressed: IdsPostingList = p1.iter().map(|id| (id, ())).collect();
        let p2_compressed: IdsPostingList = p2.iter().map(|id| (id, ())).collect();
        let p3_compressed: IdsPostingList = p3.iter().map(|id| (id, ())).collect();
        let compressed_posting_reades = vec![
            p1_compressed.view(),
            p2_compressed.view(),
            p3_compressed.view(),
        ];
        let merged = intersect_compressed_postings_iterator(compressed_posting_reades, |_| true);

        let res = merged.collect::<Vec<_>>();

        assert_eq!(res, vec![2, 5]);
    }
}
