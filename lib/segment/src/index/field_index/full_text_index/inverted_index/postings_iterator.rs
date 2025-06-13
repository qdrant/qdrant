use common::types::PointOffsetType;
use posting_list::{PostingIterator, PostingListView, PostingValue};

use super::posting_list::PostingList;
use crate::index::field_index::full_text_index::inverted_index::positions::{
    PartialDocument, Positions, TokenPosition,
};
use crate::index::field_index::full_text_index::inverted_index::{Document, TokenId};

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

pub fn intersect_compressed_postings_iterator<'a, V: PostingValue + 'a>(
    mut postings: Vec<PostingListView<'a, V>>,
    is_active: impl Fn(PointOffsetType) -> bool + 'a,
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
            is_active(*id)
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

/// Returns an iterator over the points that match the given phrase query.
pub fn intersect_compressed_postings_phrase_iterator<'a>(
    phrase: Document,
    token_to_posting: impl Fn(&TokenId) -> Option<PostingListView<'a, Positions>>,
    is_active: impl Fn(PointOffsetType) -> bool + 'a,
) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
    if phrase.is_empty() {
        // Empty request -> no matches
        return Box::new(std::iter::empty());
    }

    let postings_opt: Option<Vec<_>> = phrase
        .to_token_set()
        .tokens()
        .iter()
        .map(|token_id| token_to_posting(token_id).map(|posting| (*token_id, posting)))
        .collect();

    let Some(mut postings) = postings_opt else {
        // There are unseen tokens -> no matches
        return Box::new(std::iter::empty());
    };

    let smallest_posting_idx = postings
        .iter()
        .enumerate()
        .min_by_key(|(_idx, (_token_id, posting))| posting.len())
        .map(|(idx, _posting)| idx)
        .unwrap();
    let (smallest_posting_token, smallest_posting) = postings.remove(smallest_posting_idx);
    let smallest_posting_iterator = smallest_posting.into_iter();

    let mut posting_iterators = postings
        .into_iter()
        .map(|(token_id, posting)| (token_id, posting.into_iter()))
        .collect::<Vec<_>>();

    let has_phrase_iter = smallest_posting_iterator
        .filter(move |elem| {
            if !is_active(elem.id) {
                return false;
            }

            let initial_tokens_positions = elem.value.to_token_positions(smallest_posting_token);

            phrase_in_all_postings(
                elem.id,
                &phrase,
                initial_tokens_positions,
                &mut posting_iterators,
            )
        })
        .map(|elem| elem.id);

    Box::new(has_phrase_iter)
}

/// Reconstructs a partial document from the posting lists (which contain positions)
///
/// Returns true if the document contains the entire phrase, in the same order.
///
/// # Arguments
///
/// - `initial_tokens_positions` - must be prepopulated if iterating over a posting not included in the `posting_iterators`.
fn phrase_in_all_postings<'a>(
    id: PointOffsetType,
    phrase: &Document,
    initial_tokens_positions: Vec<TokenPosition>,
    posting_iterators: &mut Vec<(TokenId, PostingIterator<'a, Positions>)>,
) -> bool {
    let mut tokens_positions = initial_tokens_positions;
    for (token_id, posting_iterator) in posting_iterators.iter_mut() {
        // Custom "contains" check, which leverages the fact that smallest posting is sorted,
        // so the next id that must be in all postings is strictly greater than the previous one.
        //
        // This means that the other iterators can remember the last id they returned to avoid extra work
        let Some(other) = posting_iterator.advance_until_greater_or_equal(id) else {
            return false;
        };

        if id != other.id {
            return false;
        }

        debug_assert!(!other.value.is_empty());
        tokens_positions.extend(other.value.to_token_positions(*token_id))
    }
    PartialDocument::new(tokens_positions).has_phrase(phrase)
}

pub fn check_compressed_postings_phrase<'a>(
    phrase: &Document,
    point_id: PointOffsetType,
    token_to_posting: impl Fn(&TokenId) -> Option<PostingListView<'a, Positions>>,
) -> bool {
    let Some(mut posting_iterators): Option<Vec<_>> = phrase
        .tokens()
        .iter()
        .map(|token_id| token_to_posting(token_id).map(|posting| (*token_id, posting.into_iter())))
        .collect()
    else {
        // not all tokens are present in the index
        return false;
    };

    phrase_in_all_postings(point_id, phrase, Vec::new(), &mut posting_iterators)
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
