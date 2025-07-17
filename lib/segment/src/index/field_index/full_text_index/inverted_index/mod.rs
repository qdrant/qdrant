pub(super) mod immutable_inverted_index;
pub mod immutable_postings_enum;
pub(super) mod mmap_inverted_index;
pub(super) mod mutable_inverted_index;
pub(super) mod mutable_inverted_index_builder;
mod positions;
mod posting_list;
mod postings_iterator;

use std::collections::HashMap;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub type TokenId = u32;

/// Contains the set of tokens that are in a document.
///
/// Internally, it keeps them unique and sorted, so that we can binary-search over them
#[derive(Default, Debug, Clone)]
pub struct TokenSet(Vec<TokenId>);

impl TokenSet {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn tokens(&self) -> &[TokenId] {
        &self.0
    }

    pub fn inner(self) -> Vec<TokenId> {
        self.0
    }

    pub fn contains(&self, token: &TokenId) -> bool {
        self.0.binary_search(token).is_ok()
    }

    /// Checks if the current set contains all given tokens.
    ///
    /// Returns false if the subset is empty
    pub fn has_subset(&self, subset: &TokenSet) -> bool {
        if subset.is_empty() {
            return false;
        }
        subset.0.iter().all(|token| self.contains(token))
    }
}

impl From<AHashSet<TokenId>> for TokenSet {
    fn from(tokens: AHashSet<TokenId>) -> Self {
        let sorted_unique = tokens.into_iter().sorted_unstable().collect();

        Self(sorted_unique)
    }
}

impl FromIterator<TokenId> for TokenSet {
    fn from_iter<T: IntoIterator<Item = TokenId>>(iter: T) -> Self {
        let tokens = iter
            .into_iter()
            .sorted_unstable()
            .dedup()
            .collect::<Vec<_>>();

        Self(tokens)
    }
}

/// Contains the token ids that make up a document, in the same order that appear in the document.
///
/// In contrast to `TokenSet`, it can contain the same token in multiple places.
#[derive(Debug, Clone)]
pub struct Document(Vec<TokenId>);

impl Document {
    pub fn new(tokens: Vec<TokenId>) -> Self {
        Self(tokens)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn tokens(&self) -> &[TokenId] {
        &self.0
    }

    pub fn to_token_set(&self) -> TokenSet {
        self.0.iter().copied().collect()
    }

    /// Checks if the current document contains the given phrase.
    ///
    /// Returns false if the phrase is empty
    pub fn has_phrase(&self, phrase: &Document) -> bool {
        let doc = self.0.as_slice();
        let phrase = phrase.0.as_slice();

        if doc.is_empty() || phrase.is_empty() {
            return false;
        }

        // simple check for tokens in the same order as phrase
        doc.windows(phrase.len()).any(|window| window == phrase)
    }
}

impl IntoIterator for Document {
    type Item = TokenId;
    type IntoIter = std::vec::IntoIter<TokenId>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<TokenId> for Document {
    fn from_iter<T: IntoIterator<Item = TokenId>>(iter: T) -> Self {
        let tokens = iter.into_iter().collect::<Vec<_>>();

        Self(tokens)
    }
}

#[derive(Debug, Clone)]
pub enum ParsedQuery {
    /// All these tokens must be present in the document, regardless of order.
    ///
    /// In other words this should be a subset of the document's token set.
    Tokens(TokenSet),

    /// All these tokens must be present in the document, in the same order as this query.
    Phrase(Document),
}

pub trait InvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId>;

    /// Translate the string tokens into token ids.
    /// If it is an unseen token, it is added to the vocabulary and a new token id is generated.
    ///
    /// The order of the tokens is preserved.
    fn register_tokens<'a>(
        &mut self,
        str_tokens: impl IntoIterator<Item = impl AsRef<str>> + 'a,
    ) -> Vec<TokenId> {
        str_tokens
            .into_iter()
            .map(|token| self.register_token(token))
            .collect()
    }

    /// Translate the string token into token id.
    /// If it is an unseen token, it is added to the vocabulary and a new token id is generated.
    fn register_token<S: AsRef<str>>(&mut self, token_str: S) -> TokenId {
        let vocab = self.get_vocab_mut();
        match vocab.get(token_str.as_ref()) {
            Some(&idx) => idx,
            None => {
                let next_token_id = vocab.len() as TokenId;
                vocab.insert(token_str.as_ref().to_string(), next_token_id);
                next_token_id
            }
        }
    }

    fn index_tokens(
        &mut self,
        idx: PointOffsetType,
        tokens: TokenSet,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn index_document(
        &mut self,
        idx: PointOffsetType,
        document: Document,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn remove(&mut self, idx: PointOffsetType) -> bool;

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>;

    fn get_posting_len(&self, token_id: TokenId, hw_counter: &HardwareCounterCell)
    -> Option<usize>;

    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        match query {
            ParsedQuery::Tokens(tokens) => {
                self.estimate_has_subset_cardinality(tokens, condition, hw_counter)
            }
            ParsedQuery::Phrase(phrase) => {
                self.estimate_has_phrase_cardinality(phrase, condition, hw_counter)
            }
        }
    }

    fn estimate_has_subset_cardinality(
        &self,
        tokens: &TokenSet,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let points_count = self.points_count();

        let posting_lengths: Option<Vec<usize>> = tokens
            .tokens()
            .iter()
            .map(|&vocab_idx| self.get_posting_len(vocab_idx, hw_counter))
            .collect();
        if posting_lengths.is_none() || points_count == 0 {
            // There are unseen tokens -> no matches
            return CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));
        }
        let postings = posting_lengths.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));
        }
        // Smallest posting is the largest possible cardinality
        let smallest_posting = postings.iter().min().copied().unwrap();

        if postings.len() == 1 {
            return CardinalityEstimation::exact(smallest_posting)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));
        }

        let expected_frac: f64 = postings
            .iter()
            .map(|posting| *posting as f64 / points_count as f64)
            .product();
        let exp = (expected_frac * points_count as f64) as usize;
        CardinalityEstimation {
            primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
            min: 0, // ToDo: make better estimation
            exp,
            max: smallest_posting,
        }
    }

    fn estimate_has_phrase_cardinality(
        &self,
        phrase: &Document,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        if phrase.is_empty() {
            return CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));
        }

        // Start with same cardinality estimation as has_subset
        let tokenset = phrase.to_token_set();
        let subset_estimation =
            self.estimate_has_subset_cardinality(&tokenset, condition, hw_counter);

        // But we can restrict it by considering the phrase length
        let phrase_sq = phrase.len() * phrase.len();

        CardinalityEstimation {
            primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
            min: subset_estimation.min / phrase_sq,
            exp: subset_estimation.exp / phrase_sq,
            max: subset_estimation.max / phrase_sq,
        }
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_;

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> impl Iterator<Item = PayloadBlockCondition> + '_ {
        let map_filter_condition = move |(token, postings_len): (&str, usize)| {
            if postings_len >= threshold {
                Some(PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), Match::new_text(token)),
                    cardinality: postings_len,
                })
            } else {
                None
            }
        };

        // It might be very hard to predict possible combinations of conditions,
        // so we only build it for individual tokens
        self.vocab_with_postings_len_iter()
            .filter_map(map_filter_condition)
    }

    fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool;

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;

    fn values_count(&self, point_id: PointOffsetType) -> usize;

    fn points_count(&self) -> usize;

    fn get_token_id(&self, token: &str, hw_counter: &HardwareCounterCell) -> Option<TokenId>;
}

#[cfg(test)]
mod tests {

    use common::counter::hardware_counter::HardwareCounterCell;
    use rand::Rng;
    use rand::seq::SliceRandom;
    use rstest::rstest;

    use super::{Document, InvertedIndex, ParsedQuery, TokenId, TokenSet};
    use crate::index::field_index::full_text_index::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
    use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::MmapInvertedIndex;
    use crate::index::field_index::full_text_index::inverted_index::mutable_inverted_index::MutableInvertedIndex;

    fn generate_word() -> String {
        let mut rng = rand::rng();

        // Each word is 1 to 3 characters long
        let len = rng.random_range(1..=3);
        rng.sample_iter(rand::distr::Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    fn generate_query() -> Vec<String> {
        let mut rng = rand::rng();
        let len = rng.random_range(1..=2);
        (0..len).map(|_| generate_word()).collect()
    }

    /// Tries to parse a query. If there is an unknown id to a token, returns `None`
    fn to_parsed_query(
        query: Vec<String>,
        token_to_id: impl Fn(String) -> Option<TokenId>,
    ) -> Option<ParsedQuery> {
        let tokens = query
            .into_iter()
            .map(token_to_id)
            .collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::Tokens(tokens))
    }

    fn mutable_inverted_index(
        indexed_count: u32,
        deleted_count: u32,
        with_positions: bool,
    ) -> MutableInvertedIndex {
        let mut index = MutableInvertedIndex::new(with_positions);

        let hw_counter = HardwareCounterCell::new();

        for idx in 0..indexed_count {
            // Generate 10 to 30-word documents
            let doc_len = rand::rng().random_range(10..=30);
            let tokens: Vec<String> = (0..doc_len).map(|_| generate_word()).collect();
            let token_ids = index.register_tokens(&tokens);
            if with_positions {
                index
                    .index_document(idx, Document(token_ids.clone()), &hw_counter)
                    .unwrap();
            }
            let token_set = TokenSet::from_iter(token_ids);
            index.index_tokens(idx, token_set, &hw_counter).unwrap();
        }

        // Remove some points
        let mut points_to_delete = (0..indexed_count).collect::<Vec<_>>();
        points_to_delete.shuffle(&mut rand::rng());
        for idx in &points_to_delete[..deleted_count as usize] {
            index.remove(*idx);
        }

        index
    }

    #[rstest]
    fn test_mutable_to_immutable(#[values(false, true)] phrase_matching: bool) {
        let mutable = mutable_inverted_index(2000, 400, phrase_matching);

        // todo: test with phrase-enabled
        let immutable = ImmutableInvertedIndex::from(mutable.clone());

        assert!(immutable.vocab.len() < mutable.vocab.len());
        assert!(immutable.postings.len() < mutable.postings.len());
        assert!(!immutable.vocab.is_empty());

        // Check that new vocabulary token ids leads to the same posting lists
        assert!({
            immutable.vocab.iter().all(|(key, new_token)| {
                let mut new_posting_iter = immutable.postings.iter_ids(*new_token).unwrap();

                let orig_token = mutable.vocab.get(key).unwrap();

                let orig_posting = mutable.postings.get(*orig_token as usize).cloned().unwrap();

                let all_equal = orig_posting
                    .iter()
                    .zip(&mut new_posting_iter)
                    .all(|(orig, new)| orig == new);

                let same_length = new_posting_iter.next().is_none();

                all_equal && same_length
            })
        });
    }

    #[rstest]
    #[case(2000, 400)]
    #[case(2000, 2000)]
    #[case(1111, 1110)]
    #[case(1111, 0)]
    #[case(10, 2)]
    #[case(0, 0)]
    #[test]
    fn test_immutable_to_mmap_to_immutable(
        #[case] indexed_count: u32,
        #[case] deleted_count: u32,
        #[values(false, true)] phrase_matching: bool,
    ) {
        use std::collections::HashSet;

        let mutable = mutable_inverted_index(indexed_count, deleted_count, phrase_matching);
        let immutable = ImmutableInvertedIndex::from(mutable);

        let mmap_dir = tempfile::tempdir().unwrap();

        let hw_counter = HardwareCounterCell::new();

        MmapInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let mmap = MmapInvertedIndex::open(mmap_dir.path().into(), false, phrase_matching).unwrap();

        let imm_mmap = ImmutableInvertedIndex::from(&mmap);

        // Check same vocabulary
        for (token, token_id) in immutable.vocab.iter() {
            assert_eq!(mmap.get_token_id(token, &hw_counter), Some(*token_id));
            assert_eq!(imm_mmap.get_token_id(token, &hw_counter), Some(*token_id));
        }

        // Check same postings
        for token_id in 0..immutable.postings.len() as TokenId {
            let mutable_ids = immutable
                .postings
                .iter_ids(token_id)
                .unwrap()
                .collect::<HashSet<_>>();

            // Check mutable vs mmap
            let mmap_ids = mmap
                .storage
                .as_ref()
                .unwrap()
                .postings
                .iter_ids(token_id)
                .unwrap()
                .collect();
            assert_eq!(mutable_ids, mmap_ids);

            // Check mutable vs immutable mmap
            let imm_mmap_ids = imm_mmap
                .postings
                .iter_ids(token_id)
                .unwrap()
                .collect::<HashSet<_>>();
            assert_eq!(mutable_ids, imm_mmap_ids);
        }

        for (point_id, count) in immutable.point_to_tokens_count.iter().enumerate() {
            // Check same deleted points
            assert_eq!(
                mmap.storage
                    .as_ref()
                    .unwrap()
                    .deleted_points
                    .get(point_id)
                    .unwrap(),
                *count == 0,
                "point_id: {point_id}",
            );

            // Check same count
            assert_eq!(
                *mmap
                    .storage
                    .as_ref()
                    .unwrap()
                    .point_to_tokens_count
                    .get(point_id)
                    .unwrap(),
                *count
            );
            assert_eq!(imm_mmap.point_to_tokens_count[point_id], *count);
        }

        // Check same points count
        assert_eq!(immutable.points_count, mmap.active_points_count);
        assert_eq!(immutable.points_count, imm_mmap.points_count);
    }

    #[rstest]
    fn test_mmap_index_congruence(#[values(false, true)] phrase_matching: bool) {
        let indexed_count = 10000;
        let deleted_count = 500;

        let hw_counter = HardwareCounterCell::new();
        let mmap_dir = tempfile::tempdir().unwrap();

        let mut mut_index = mutable_inverted_index(indexed_count, deleted_count, phrase_matching);

        let immutable = ImmutableInvertedIndex::from(mut_index.clone());
        MmapInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let mut mmap_index =
            MmapInvertedIndex::open(mmap_dir.path().into(), false, phrase_matching).unwrap();

        let mut imm_mmap_index = ImmutableInvertedIndex::from(&mmap_index);

        let queries: Vec<_> = (0..100).map(|_| generate_query()).collect();

        let mut_parsed_queries: Vec<_> = queries
            .iter()
            .cloned()
            .map(|query| to_parsed_query(query, |token| mut_index.vocab.get(&token).copied()))
            .collect();
        let mmap_parsed_queries: Vec<_> = queries
            .iter()
            .cloned()
            .map(|query| {
                to_parsed_query(query, |token| mmap_index.get_token_id(&token, &hw_counter))
            })
            .collect();
        let imm_mmap_parsed_queries: Vec<_> = queries
            .into_iter()
            .map(|query| {
                to_parsed_query(query, |token| {
                    imm_mmap_index.get_token_id(&token, &hw_counter)
                })
            })
            .collect();

        check_query_congruence(
            &mut_parsed_queries,
            &mmap_parsed_queries,
            &imm_mmap_parsed_queries,
            &mut_index,
            &mmap_index,
            &imm_mmap_index,
            &hw_counter,
        );

        // Delete random documents from both indexes
        let points_to_delete: Vec<_> = (0..deleted_count)
            .map(|_| rand::rng().random_range(0..indexed_count))
            .collect();
        for point_id in &points_to_delete {
            mut_index.remove(*point_id);
            mmap_index.remove(*point_id);
            imm_mmap_index.remove(*point_id);
        }

        // Check congruence after deletion
        check_query_congruence(
            &mut_parsed_queries,
            &mmap_parsed_queries,
            &imm_mmap_parsed_queries,
            &mut_index,
            &mmap_index,
            &imm_mmap_index,
            &hw_counter,
        );
    }

    fn check_query_congruence(
        mut_parsed_queries: &[Option<ParsedQuery>],
        mmap_parsed_queries: &[Option<ParsedQuery>],
        imm_mmap_parsed_queries: &[Option<ParsedQuery>],
        mut_index: &MutableInvertedIndex,
        mmap_index: &MmapInvertedIndex,
        imm_mmap_index: &ImmutableInvertedIndex,
        hw_counter: &HardwareCounterCell,
    ) {
        for queries in mut_parsed_queries.iter().cloned().zip(
            mmap_parsed_queries
                .iter()
                .cloned()
                .zip(imm_mmap_parsed_queries.iter().cloned()),
        ) {
            let (Some(mut_query), (Some(imm_query), Some(imm_mmap_query))) = queries else {
                // Immutable index can have a smaller vocabulary, since it only contains tokens that have
                // non-empty posting lists.
                // Since we removed some documents from the mutable index, it can happen that the immutable
                // index returns None when parsing the query, even if the mutable index returns Some.
                //
                // In this case both queries would filter to an empty set of documents.
                continue;
            };
            let mut_filtered = mut_index.filter(mut_query, hw_counter).collect::<Vec<_>>();
            let imm_filtered = mmap_index.filter(imm_query, hw_counter).collect::<Vec<_>>();
            let imm_mmap_filtered = imm_mmap_index
                .filter(imm_mmap_query, hw_counter)
                .collect::<Vec<_>>();

            assert_eq!(mut_filtered, imm_filtered);
            assert_eq!(imm_filtered, imm_mmap_filtered);
        }
    }
}
