pub(super) mod immutable_inverted_index;
pub mod immutable_postings_enum;
pub(super) mod mutable_inverted_index;
pub(super) mod mutable_inverted_index_builder;
pub(super) mod on_disk_inverted_index;
mod positions;
mod posting_list;
mod postings_iterator;

use std::cmp::min;
use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;
use itertools::Itertools;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::index::query_estimator::expected_should_estimation;
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub type TokenId = u32;

/// Sentinel string inserted between tokens of consecutive array elements.
/// When registered as a normal vocab token it occupies a position in the
/// document, preventing phrase queries from matching across element boundaries.
/// No tokenizer will ever produce this string, so it can never appear in a
/// user query.
pub const ARRAY_BOUNDARY_SENTINEL: &str = "\x00";

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

    /// Heap memory usage in bytes.
    pub fn heap_bytes(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<TokenId>()
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

    /// Checks if the current set contains any of the given tokens.
    /// Returns false if the subset is empty
    pub fn has_any(&self, subset: &TokenSet) -> bool {
        if subset.is_empty() {
            return false;
        }
        subset.0.iter().any(|token| self.contains(token))
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

    /// Heap memory usage in bytes.
    pub fn heap_bytes(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<TokenId>()
    }

    pub fn to_token_set(&self) -> TokenSet {
        self.0.iter().copied().collect()
    }

    /// Checks if the current document contains the given phrase.
    ///
    /// Returns false if the phrase is empty.
    /// Boundary sentinels naturally prevent matches across array elements
    /// because the query never contains them.
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
    AllTokens(TokenSet),

    /// At least one of these tokens must be present in the document.
    AnyTokens(TokenSet),

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
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>>;

    fn get_posting_len(
        &self,
        token_id: TokenId,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<usize>>;

    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        match query {
            ParsedQuery::AllTokens(tokens) => {
                self.estimate_has_subset_cardinality(tokens, condition, hw_counter)
            }
            ParsedQuery::Phrase(phrase) => {
                self.estimate_has_phrase_cardinality(phrase, condition, hw_counter)
            }
            ParsedQuery::AnyTokens(tokens) => {
                self.estimate_has_any_cardinality(tokens, condition, hw_counter)
            }
        }
    }

    fn estimate_has_subset_cardinality(
        &self,
        tokens: &TokenSet,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let points_count = self.points_count();

        let posting_lengths: Option<Vec<usize>> = tokens
            .tokens()
            .iter()
            .map(|&vocab_idx| self.get_posting_len(vocab_idx, hw_counter))
            .collect::<OperationResult<Option<Vec<usize>>>>()?;
        if posting_lengths.is_none() || points_count == 0 {
            // There are unseen tokens -> no matches
            return Ok(CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }
        let postings = posting_lengths.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Ok(CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }
        // Smallest posting is the largest possible cardinality
        let smallest_posting = postings.iter().min().copied().unwrap();

        if postings.len() == 1 {
            return Ok(CardinalityEstimation::exact(smallest_posting)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }

        let expected_frac: f64 = postings
            .iter()
            .map(|posting| *posting as f64 / points_count as f64)
            .product();
        let exp = (expected_frac * points_count as f64) as usize;
        Ok(CardinalityEstimation {
            primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
            min: 0, // ToDo: make better estimation
            exp,
            max: smallest_posting,
        })
    }

    fn estimate_has_any_cardinality(
        &self,
        tokens: &TokenSet,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let points_count = self.points_count();

        let posting_lengths: Vec<usize> = tokens
            .tokens()
            .iter()
            .filter_map(|&vocab_idx| self.get_posting_len(vocab_idx, hw_counter).transpose())
            .collect::<OperationResult<Vec<usize>>>()?;

        if posting_lengths.is_empty() {
            // Empty request -> no matches
            return Ok(CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }

        // At least one posting is the largest possible cardinality
        let largest_posting = posting_lengths.iter().max().copied().unwrap();

        if posting_lengths.len() == 1 {
            return Ok(CardinalityEstimation::exact(largest_posting)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }

        let sum: usize = posting_lengths.iter().sum();

        let exp = expected_should_estimation(posting_lengths.into_iter(), points_count);

        Ok(CardinalityEstimation {
            primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
            min: largest_posting,
            exp,
            max: min(sum, points_count),
        })
    }

    fn estimate_has_phrase_cardinality(
        &self,
        phrase: &Document,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        if phrase.is_empty() {
            return Ok(CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }

        // Start with same cardinality estimation as has_subset
        let tokenset = phrase.to_token_set();
        let subset_estimation =
            self.estimate_has_subset_cardinality(&tokenset, condition, hw_counter)?;

        // But we can restrict it by considering the phrase length
        let phrase_sq = phrase.len() * phrase.len();

        Ok(CardinalityEstimation {
            primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
            min: subset_estimation.min / phrase_sq,
            exp: subset_estimation.exp / phrase_sq,
            max: subset_estimation.max / phrase_sq,
        })
    }

    fn for_each_vocab_with_postings_len(
        &self,
        f: impl FnMut(&str, usize) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // It might be very hard to predict possible combinations of conditions,
        // so we only build it for individual tokens
        self.for_each_vocab_with_postings_len(|token, postings_len| {
            if postings_len >= threshold {
                f(PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), Match::new_text(token)),
                    cardinality: postings_len,
                })?;
            }
            Ok(())
        })
    }

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
    ) -> OperationResult<bool>;

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;

    fn values_count(&self, point_id: PointOffsetType) -> usize;

    fn points_count(&self) -> usize;

    /// Number of tokens indexed for `point_id`, repetitions included. This is
    /// `|d|` in BM25, and it is stored, never derived: the token set is
    /// deduplicated and carries no lengths.
    ///
    /// `None` when this index does not record lengths, or when `point_id` is
    /// past the point space it covers. `Some(0)` for a point it holds no tokens
    /// for, which covers a deleted document and one whose tokens were all
    /// filtered away alike: every backend encodes those the same way, and none
    /// of them can tell the two apart.
    ///
    /// Every backend answers identically for the same data. That is not free on
    /// disk, where the sidecar is written unmasked and keeps a deleted point's
    /// original length, so the deletion mask is consulted first.
    fn doc_len(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<u32>>;

    /// Total tokens over the points this index still holds, the numerator of
    /// `avgdl`. `None` when this index does not record lengths.
    ///
    /// This is the capability probe, not [`Self::doc_len`], which also answers
    /// `None` for a point id outside the index.
    ///
    /// "Still holds" is not "live" under append-only deletion, where a dropped
    /// point never reaches `remove` and the in-RAM backends keep counting it.
    /// The on-disk backend re-reads the id tracker's mask at `open` and does
    /// not.
    ///
    /// Deliberately not divided by [`Self::points_count`] here. The average
    /// is a corpus statistic, and a per-segment average would drift from the
    /// one a search actually needs, which is summed over every segment.
    fn total_tokens(&self, hw_counter: &HardwareCounterCell) -> OperationResult<Option<u64>>;

    /// Resolve token -> token_id and call the closure for each token_id.
    fn for_each_token_id<'a, U: UserData>(
        &self,
        tokens: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()>;
}

#[cfg(test)]
mod tests {

    use common::bitvec::BitVec;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use rand::RngExt;
    use rand::seq::SliceRandom;
    use rstest::rstest;

    use super::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
    use crate::index::field_index::full_text_index::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
    use crate::index::field_index::full_text_index::inverted_index::mutable_inverted_index::MutableInvertedIndex;
    use crate::index::field_index::full_text_index::inverted_index::on_disk_inverted_index::{
        OnDiskInvertedIndex, POINT_TO_DOC_LEN_FILE,
    };

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
    fn to_parsed_query(token_ids: &[Option<TokenId>]) -> Option<ParsedQuery> {
        let tokens = token_ids.iter().copied().collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::AllTokens(tokens))
    }

    fn to_parsed_query_any(token_ids: &[Option<TokenId>]) -> Option<ParsedQuery> {
        let tokens = token_ids.iter().copied().collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::AnyTokens(tokens))
    }

    fn parse_all<I: InvertedIndex>(
        queries: &[Vec<String>],
        index: &I,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<Option<ParsedQuery>> {
        queries
            .iter()
            .flat_map(|query| {
                let mut ids = vec![None; query.len()];
                index
                    .for_each_token_id(
                        query.iter().map(String::as_str).enumerate(),
                        hw_counter,
                        |i, id| ids[i] = id,
                    )
                    .unwrap();
                [to_parsed_query(&ids), to_parsed_query_any(&ids)]
            })
            .collect()
    }

    fn mutable_inverted_index(
        indexed_count: u32,
        deleted_count: u32,
        with_positions: bool,
    ) -> MutableInvertedIndex {
        let mut index = MutableInvertedIndex::new(with_positions, true);

        let hw_counter = HardwareCounterCell::new();

        for idx in 0..indexed_count {
            // Generate 10 to 30-word documents
            let doc_len = rand::rng().random_range(10..=30);
            let tokens: Vec<String> = (0..doc_len).map(|_| generate_word()).collect();
            // Through the same entry point the write paths use, so the fixture
            // records lengths too.
            index
                .index_str_tokens(idx, &tokens, Some(doc_len as u32), &hw_counter)
                .unwrap();
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

        // Deleted points included: `remove` zeroes them on both sides.
        assert_eq!(
            immutable.point_to_doc_len, mutable.point_to_doc_len,
            "document lengths lost converting to the immutable index",
        );
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

        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let empty_deleted = BitVec::new();
        let mmap: OnDiskInvertedIndex = OnDiskInvertedIndex::open(
            &MmapFs,
            mmap_dir.path().into(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .unwrap();

        let imm_mmap = ImmutableInvertedIndex::try_from(&mmap).unwrap();

        // Check same vocabulary
        let assert_same_id = |expected: TokenId, actual: Option<TokenId>| {
            assert_eq!(actual, Some(expected));
        };
        let vocab_iter = || immutable.vocab.iter().map(|(t, id)| (*id, t.as_str()));
        mmap.for_each_token_id(vocab_iter(), &hw_counter, assert_same_id)
            .unwrap();
        imm_mmap
            .for_each_token_id(vocab_iter(), &hw_counter, assert_same_id)
            .unwrap();

        // Check same postings
        for token_id in 0..immutable.postings.len() as TokenId {
            let mutable_ids = immutable
                .postings
                .iter_ids(token_id)
                .unwrap()
                .collect::<HashSet<_>>();

            // Check mutable vs mmap
            let mmap_ids = mmap.storage.postings.iter_ids(token_id).unwrap().collect();
            assert_eq!(mutable_ids, mmap_ids);

            // Check mutable vs immutable mmap
            let imm_mmap_ids = imm_mmap
                .postings
                .iter_ids(token_id)
                .unwrap()
                .collect::<HashSet<_>>();
            assert_eq!(mutable_ids, imm_mmap_ids);
        }

        let mmap_counts = mmap
            .storage
            .point_to_tokens_count
            .read_whole()
            .unwrap()
            .into_owned();
        let mmap_doc_lens = mmap
            .storage
            .point_to_doc_len
            .as_ref()
            .expect("sidecar written for a scoring index")
            .read_whole()
            .unwrap()
            .into_owned();
        let immutable_doc_lens = immutable
            .point_to_doc_len
            .as_ref()
            .expect("lengths recorded");
        let imm_mmap_doc_lens = imm_mmap
            .point_to_doc_len
            .as_ref()
            .expect("lengths read back");
        for (point_id, count) in immutable.point_to_tokens_count.iter().enumerate() {
            // Check same deleted points
            assert_eq!(
                mmap.storage.deleted_points.is_active(point_id as u32),
                *count != 0,
                "point_id: {point_id}",
            );

            // Check same count
            assert_eq!(mmap_counts[point_id], *count);
            assert_eq!(imm_mmap.point_to_tokens_count[point_id], *count);

            // Check same document length, masked identically
            assert_eq!(mmap_doc_lens[point_id], immutable_doc_lens[point_id]);
            assert_eq!(
                imm_mmap_doc_lens[point_id], immutable_doc_lens[point_id],
                "point_id: {point_id}",
            );
        }

        // A deleted point contributes nothing to the live total.
        for (point_id, count) in immutable.point_to_tokens_count.iter().enumerate() {
            if *count == 0 {
                assert_eq!(
                    imm_mmap_doc_lens[point_id], 0,
                    "deleted point {point_id} still carries a length",
                );
            }
        }

        // Check same points count
        assert_eq!(immutable.points_count, mmap.points_count());
        assert_eq!(immutable.points_count, imm_mmap.points_count);
    }

    /// A missing sidecar makes the index report no lengths, never makes it
    /// report itself absent. `files()` is keyed off what is on disk, which is
    /// what decides whether a snapshot carries the sidecar.
    #[rstest]
    fn missing_doc_len_sidecar_reports_no_lengths(#[values(false, true)] phrase_matching: bool) {
        let mutable = mutable_inverted_index(200, 20, phrase_matching);
        let immutable = ImmutableInvertedIndex::from(mutable);

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let empty_deleted = BitVec::new();
        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);

        let open = || {
            OnDiskInvertedIndex::<MmapFile>::open(
                &MmapFs,
                mmap_dir.path().to_path_buf(),
                Populate::No,
                phrase_matching,
                &empty_deleted,
            )
        };

        {
            let opened = open().unwrap().expect("a freshly built index opens");
            assert!(opened.records_doc_len());
            assert!(
                opened.files().contains(&sidecar),
                "a sidecar on disk belongs to the snapshot file set",
            );
        }

        fs_err::remove_file(&sidecar).unwrap();

        let without = open()
            .unwrap()
            .expect("a missing sidecar must not make the index absent");
        assert!(!without.records_doc_len());
        assert!(
            !without.files().contains(&sidecar),
            "a file that is not on disk must not reach the snapshot file set",
        );
    }

    /// Masking on load exists for a point deleted through the id-tracker after
    /// the index was built. The congruence tests cannot reach it: they delete on
    /// the mutable index, so those lengths are zero before `create` runs.
    #[rstest]
    fn doc_len_is_masked_for_runtime_deletions(#[values(false, true)] phrase_matching: bool) {
        let mutable = mutable_inverted_index(64, 0, phrase_matching);
        let immutable = ImmutableInvertedIndex::from(mutable);
        let lens_at_build = immutable
            .point_to_doc_len
            .clone()
            .expect("the fixture records lengths");

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();

        // A point that is live and non-empty on disk, deleted only at runtime.
        let victim = lens_at_build
            .iter()
            .position(|&len| len > 0)
            .expect("some document has tokens");
        let mut deleted = BitVec::repeat(false, lens_at_build.len());
        deleted.set(victim, true);

        let mmap = OnDiskInvertedIndex::<MmapFile>::open(
            &MmapFs,
            mmap_dir.path().to_path_buf(),
            Populate::No,
            phrase_matching,
            &deleted,
        )
        .unwrap()
        .unwrap();

        let on_disk = mmap
            .storage
            .point_to_doc_len
            .as_ref()
            .unwrap()
            .read_whole()
            .unwrap()
            .into_owned();
        assert_eq!(
            on_disk[victim], lens_at_build[victim],
            "the file itself is written once and keeps the original length",
        );

        let imm_mmap = ImmutableInvertedIndex::try_from(&mmap).unwrap();
        assert_eq!(
            imm_mmap.point_to_doc_len.as_ref().unwrap()[victim],
            0,
            "a runtime deletion must be masked out on load",
        );

        // Summing has to mask too. The agreement test cannot catch this: its
        // deletions happen before `create`, so every inactive slot is already
        // zero on disk and dropping the mask there changes nothing.
        let live_total: u64 = lens_at_build
            .iter()
            .enumerate()
            .filter(|(point_id, _)| *point_id != victim)
            .map(|(_, doc_len)| u64::from(*doc_len))
            .sum();
        let hw_counter = HardwareCounterCell::new();
        assert_eq!(
            mmap.total_tokens(&hw_counter).unwrap(),
            Some(live_total),
            "the total must not count a point the id tracker deleted",
        );
        assert_eq!(
            imm_mmap.total_tokens(&hw_counter).unwrap(),
            Some(live_total)
        );
        assert_eq!(
            mmap.doc_len(victim as PointOffsetType, &hw_counter)
                .unwrap(),
            Some(0),
            "a runtime deletion must read as no tokens, not as the stale length",
        );
    }

    /// Rebuilding the same directory without lengths must not leave the
    /// previous build's sidecar behind: `open` would read it as this build's,
    /// at offsets that now belong to different documents.
    #[rstest]
    fn rebuilding_without_lengths_removes_the_sidecar(
        #[values(false, true)] phrase_matching: bool,
    ) {
        let hw_counter = HardwareCounterCell::new();
        let mmap_dir = tempfile::tempdir().unwrap();
        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);
        let empty_deleted = BitVec::new();

        let with_lengths =
            ImmutableInvertedIndex::from(mutable_inverted_index(64, 0, phrase_matching));
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &with_lengths).unwrap();
        assert!(sidecar.exists());

        // The same points, indexed by a build that records nothing.
        let mut without_lengths = MutableInvertedIndex::new(phrase_matching, false);
        for idx in 0..64 {
            let tokens: Vec<String> = (0..=idx % 8).map(|_| generate_word()).collect();
            without_lengths
                .index_str_tokens(idx, &tokens, None, &hw_counter)
                .unwrap();
        }
        let without_lengths = ImmutableInvertedIndex::from(without_lengths);
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &without_lengths).unwrap();

        assert!(
            !sidecar.exists(),
            "a stale sidecar outlived the build that wrote it"
        );
        let opened = OnDiskInvertedIndex::<MmapFile>::open(
            &MmapFs,
            mmap_dir.path().to_path_buf(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .unwrap();
        assert!(!opened.records_doc_len());
        assert!(!opened.files().contains(&sidecar));
    }

    /// The other end of the same check: a sidecar covering more points than the
    /// index has is as untrustworthy as one covering fewer, and used to be
    /// accepted and then silently truncated when materialized.
    #[rstest]
    fn oversized_doc_len_sidecar_is_ignored(#[values(false, true)] phrase_matching: bool) {
        let mutable = mutable_inverted_index(64, 0, phrase_matching);
        let immutable = ImmutableInvertedIndex::from(mutable);

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();

        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);
        let full = fs_err::metadata(&sidecar).unwrap().len();
        fs_err::OpenOptions::new()
            .write(true)
            .open(&sidecar)
            .unwrap()
            .set_len(full + size_of::<u32>() as u64)
            .unwrap();

        let empty_deleted = BitVec::new();
        let opened = OnDiskInvertedIndex::<MmapFile>::open(
            &MmapFs,
            mmap_dir.path().to_path_buf(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .expect("the index still opens");
        assert!(
            !opened.records_doc_len(),
            "a sidecar longer than the index must not be trusted either",
        );
    }

    /// A truncated sidecar is treated as absent rather than padded, since the
    /// padding would read exactly like real zero-length documents.
    #[rstest]
    fn truncated_doc_len_sidecar_is_ignored(#[values(false, true)] phrase_matching: bool) {
        let mutable = mutable_inverted_index(200, 20, phrase_matching);
        let immutable = ImmutableInvertedIndex::from(mutable);

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();

        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);
        let full = fs_err::metadata(&sidecar).unwrap().len();
        fs_err::OpenOptions::new()
            .write(true)
            .open(&sidecar)
            .unwrap()
            .set_len(full - size_of::<u32>() as u64)
            .unwrap();

        let empty_deleted = BitVec::new();
        let opened = OnDiskInvertedIndex::<MmapFile>::open(
            &MmapFs,
            mmap_dir.path().to_path_buf(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .expect("the index still opens");
        assert!(
            !opened.records_doc_len(),
            "a truncated sidecar must not be padded into looking complete",
        );
    }

    /// Every backend answers `doc_len` with the same number for every point,
    /// including the ones it holds no tokens for, and every total is the sum of
    /// those answers. This is the whole contract a scorer gets from a segment,
    /// so it is pinned across all four shapes rather than on the one that
    /// happens to be cheapest.
    #[rstest]
    fn doc_len_and_total_tokens_agree_across_backends(
        #[values(false, true)] phrase_matching: bool,
    ) {
        let hw_counter = HardwareCounterCell::new();
        let mutable = mutable_inverted_index(200, 20, phrase_matching);
        let immutable = ImmutableInvertedIndex::from(mutable.clone());

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let empty_deleted = BitVec::new();
        let mmap: OnDiskInvertedIndex = OnDiskInvertedIndex::open(
            &MmapFs,
            mmap_dir.path().into(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .unwrap();
        let imm_mmap = ImmutableInvertedIndex::try_from(&mmap).unwrap();

        let mut live_total = 0;
        for point_id in 0..immutable.point_to_tokens_count.len() as PointOffsetType {
            let expected = mutable.doc_len(point_id, &hw_counter).unwrap();
            assert_eq!(immutable.doc_len(point_id, &hw_counter).unwrap(), expected);
            assert_eq!(imm_mmap.doc_len(point_id, &hw_counter).unwrap(), expected);
            assert_eq!(
                mmap.doc_len(point_id, &hw_counter).unwrap(),
                expected,
                "point {point_id}, deleted or empty included",
            );
            live_total += u64::from(expected.expect("every point of the index has a length"));
        }

        assert!(live_total > 0, "the fixture indexed nothing");
        // Only the on-disk backend reads anything to answer, and it bills it.
        assert!(
            hw_counter.payload_index_io_read_counter().get() > 0,
            "on-disk reads must be measured",
        );

        for (backend, total) in [
            ("mutable", mutable.total_tokens(&hw_counter).unwrap()),
            ("immutable", immutable.total_tokens(&hw_counter).unwrap()),
            ("mmap", mmap.total_tokens(&hw_counter).unwrap()),
            (
                "immutable from mmap",
                imm_mmap.total_tokens(&hw_counter).unwrap(),
            ),
        ] {
            assert_eq!(
                total,
                Some(live_total),
                "{backend} disagrees with the sum of its own lengths",
            );
        }
    }

    /// Without recording, every accessor reports absence rather than zero. Zero
    /// is a real length, so the two must not collapse into each other.
    #[rstest]
    fn accessors_report_absence_without_recording(#[values(false, true)] phrase_matching: bool) {
        let hw_counter = HardwareCounterCell::new();
        let mut mutable = MutableInvertedIndex::new(phrase_matching, false);
        for idx in 0..16 {
            let tokens: Vec<String> = (0..=idx).map(|_| generate_word()).collect();
            mutable
                .index_str_tokens(idx, &tokens, None, &hw_counter)
                .unwrap();
        }
        let immutable = ImmutableInvertedIndex::from(mutable.clone());

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let empty_deleted = BitVec::new();
        let mmap: OnDiskInvertedIndex = OnDiskInvertedIndex::open(
            &MmapFs,
            mmap_dir.path().into(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .unwrap();

        assert!(!mmap.records_doc_len(), "nothing to write, nothing to read");
        for point_id in 0..16 {
            assert_eq!(mutable.doc_len(point_id, &hw_counter).unwrap(), None);
            assert_eq!(immutable.doc_len(point_id, &hw_counter).unwrap(), None);
            assert_eq!(mmap.doc_len(point_id, &hw_counter).unwrap(), None);
        }
        assert_eq!(mutable.total_tokens(&hw_counter).unwrap(), None);
        assert_eq!(immutable.total_tokens(&hw_counter).unwrap(), None);
        assert_eq!(mmap.total_tokens(&hw_counter).unwrap(), None);
    }

    #[rstest]
    fn test_mmap_index_congruence(#[values(false, true)] phrase_matching: bool) {
        let indexed_count = 10000;
        let deleted_count = 500;

        let hw_counter = HardwareCounterCell::new();
        let mmap_dir = tempfile::tempdir().unwrap();

        let mut mut_index = mutable_inverted_index(indexed_count, deleted_count, phrase_matching);

        let immutable = ImmutableInvertedIndex::from(mut_index.clone());
        OnDiskInvertedIndex::create(mmap_dir.path().into(), &immutable).unwrap();
        let empty_deleted = BitVec::new();
        let mut mmap_index = OnDiskInvertedIndex::open(
            &MmapFs,
            mmap_dir.path().into(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .unwrap();

        let mut imm_mmap_index = ImmutableInvertedIndex::try_from(&mmap_index).unwrap();

        let queries: Vec<_> = (0..100).map(|_| generate_query()).collect();

        let mut_parsed_queries = parse_all(&queries, &mut_index, &hw_counter);
        let mmap_parsed_queries = parse_all(&queries, &mmap_index, &hw_counter);
        let imm_mmap_parsed_queries = parse_all(&queries, &imm_mmap_index, &hw_counter);

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
        mmap_index: &OnDiskInvertedIndex,
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
            let mut_filtered = mut_index
                .filter(mut_query, hw_counter)
                .unwrap()
                .collect::<Vec<_>>();
            let imm_filtered = mmap_index
                .filter(imm_query, hw_counter)
                .unwrap()
                .collect::<Vec<_>>();
            let imm_mmap_filtered = imm_mmap_index
                .filter(imm_mmap_query, hw_counter)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(mut_filtered, imm_filtered);
            assert_eq!(imm_filtered, imm_mmap_filtered);
        }
    }
}
