pub mod bm25;
mod doc_len;
pub(super) mod immutable_inverted_index;
pub mod immutable_postings_enum;
pub(super) mod mutable_inverted_index;
pub(super) mod mutable_inverted_index_builder;
pub(super) mod on_disk_inverted_index;
mod positions;
mod posting_list;
mod postings_iterator;
pub(super) mod term_frequencies;

use std::cmp::min;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use bm25::{Bm25Accept, Bm25Query};
use common::types::{PointOffsetType, ScoredPointOffset};
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

    fn index_document(&mut self, idx: PointOffsetType, document: Document) -> OperationResult<()>;

    fn remove(&mut self, idx: PointOffsetType) -> bool;

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>>;

    fn get_posting_len(&self, token_id: TokenId) -> OperationResult<Option<usize>>;

    /// The `limit` best documents for `query` by BM25, highest first, among
    /// those `accept` allows. Term frequencies come from positions, so an
    /// index built without them cannot score and reports an error.
    fn score_bm25(
        &self,
        query: &Bm25Query,
        accept: &Bm25Accept<'_>,
        limit: usize,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>>;

    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        match query {
            ParsedQuery::AllTokens(tokens) => {
                self.estimate_has_subset_cardinality(tokens, condition)
            }
            ParsedQuery::Phrase(phrase) => self.estimate_has_phrase_cardinality(phrase, condition),
            ParsedQuery::AnyTokens(tokens) => self.estimate_has_any_cardinality(tokens, condition),
        }
    }

    fn estimate_has_subset_cardinality(
        &self,
        tokens: &TokenSet,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        let points_count = self.points_count();

        let posting_lengths: Option<Vec<usize>> = tokens
            .tokens()
            .iter()
            .map(|&vocab_idx| self.get_posting_len(vocab_idx))
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
    ) -> OperationResult<CardinalityEstimation> {
        let points_count = self.points_count();

        let posting_lengths: Vec<usize> = tokens
            .tokens()
            .iter()
            .filter_map(|&vocab_idx| self.get_posting_len(vocab_idx).transpose())
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
    ) -> OperationResult<CardinalityEstimation> {
        if phrase.is_empty() {
            return Ok(CardinalityEstimation::exact(0)
                .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))));
        }

        // Start with same cardinality estimation as has_subset
        let tokenset = phrase.to_token_set();
        let subset_estimation = self.estimate_has_subset_cardinality(&tokenset, condition)?;

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

    /// Documents the BM25 statistics count, `N`: those holding a token, as
    /// `posting_len` and [`Self::total_tokens`] count them. An immutable index
    /// counts its build, deleted documents included, so a deletion never
    /// changes its statistics, unlike [`Self::points_count`]. A mutable index
    /// counts the documents it holds.
    fn document_count(&self) -> usize {
        self.points_count()
    }

    /// Number of tokens indexed for each of `point_ids`, repetitions included:
    /// `f(index, doc_len)` once per entry, with `index` into `point_ids`, in no
    /// particular order. This is `|d|` in BM25, and it is stored, never
    /// derived: the token set is deduplicated and carries no lengths.
    ///
    /// `None` when this index does not record lengths, or when a point is past
    /// the point space it covers. `Some(0)` for a point it holds no tokens for,
    /// which covers a deleted document and one whose tokens were all filtered
    /// away alike: every backend encodes those the same way, and none of them
    /// can tell the two apart.
    ///
    /// Every backend answers identically for the same data. That is not free on
    /// disk, where the sidecar is written unmasked and keeps a deleted point's
    /// original length, so the deletion mask is consulted first.
    ///
    /// There is no per-point variant, on purpose: the on-disk index may sit on
    /// a slow or remote disk, where a length read per point is a round trip per
    /// point. The reads it has to make are issued together.
    fn doc_len_batch(
        &self,
        point_ids: &[PointOffsetType],
        f: impl FnMut(usize, Option<u32>),
    ) -> OperationResult<()>;

    /// Total tokens over the documents [`Self::document_count`] counts, the
    /// numerator of `avgdl`. `None` when this index does not record lengths.
    ///
    /// This is the capability probe, not [`Self::doc_len_batch`], which also
    /// answers `None` for a point id outside the index.
    ///
    /// Deliberately not divided by [`Self::points_count`] here. The average
    /// is a corpus statistic, and a per-segment average would drift from the
    /// one a search actually needs, which is summed over every segment.
    fn total_tokens(&self) -> Option<u64>;

    /// Resolve token -> token_id and call the closure for each token_id.
    fn for_each_token_id<'a, U: UserData>(
        &self,
        tokens: impl Iterator<Item = (U, &'a str)>,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()>;
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::ambient::AmbientContext;
    use common::ambient::hw::HwMetric;
    use common::bitvec::BitVec;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use rand::RngExt;
    use rand::seq::SliceRandom;
    use rstest::rstest;

    use super::doc_len::{decode_doc_len, round_doc_len};
    use super::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
    use crate::index::field_index::full_text_index::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
    use crate::index::field_index::full_text_index::inverted_index::mutable_inverted_index::MutableInvertedIndex;
    use crate::index::field_index::full_text_index::inverted_index::on_disk_inverted_index::types::PostingsHeader;
    use crate::index::field_index::full_text_index::inverted_index::on_disk_inverted_index::doc_len_sidecar::read_exact;
    use crate::index::field_index::full_text_index::inverted_index::on_disk_inverted_index::{
        COMPACT_VOCAB_FILE, OnDiskInvertedIndex, POINT_TO_DOC_LEN_FILE, POSTINGS_FILE, VOCAB_FILE,
        Vocab, VocabFormat,
    };
    use crate::index::field_index::full_text_index::inverted_index::on_disk_inverted_index::posting_directory::POSTING_DIRECTORY_FILE;

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

    fn parse_all<I: InvertedIndex>(queries: &[Vec<String>], index: &I) -> Vec<Option<ParsedQuery>> {
        queries
            .iter()
            .flat_map(|query| {
                let mut ids = vec![None; query.len()];
                index
                    .for_each_token_id(query.iter().map(String::as_str).enumerate(), |i, id| {
                        ids[i] = id
                    })
                    .unwrap();
                [to_parsed_query(&ids), to_parsed_query_any(&ids)]
            })
            .collect()
    }

    /// Every answer of [`InvertedIndex::doc_len_batch`], in `point_ids` order.
    fn doc_lens(index: &impl InvertedIndex, point_ids: &[PointOffsetType]) -> Vec<Option<u32>> {
        let mut out = vec![Some(u32::MAX); point_ids.len()];
        index
            .doc_len_batch(point_ids, |at, doc_len| out[at] = doc_len)
            .unwrap();
        out
    }

    fn mutable_inverted_index(
        indexed_count: u32,
        deleted_count: u32,
        with_positions: bool,
    ) -> MutableInvertedIndex {
        let mut index = MutableInvertedIndex::new(with_positions, true);

        for idx in 0..indexed_count {
            // Generate 10 to 30-word documents
            let doc_len = rand::rng().random_range(10..=30);
            let tokens: Vec<String> = (0..doc_len).map(|_| generate_word()).collect();
            // Through the same entry point the write paths use, so the fixture
            // records lengths too.
            index
                .index_str_tokens(idx, &tokens, Some(doc_len as u32))
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
            immutable.point_to_doc_len(),
            mutable.point_to_doc_len,
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
        #[values(VocabFormat::Hashmap, VocabFormat::Compact)] vocab_format: VocabFormat,
    ) {
        use std::collections::HashSet;

        let mutable = mutable_inverted_index(indexed_count, deleted_count, phrase_matching);
        let mmap_dir = tempfile::tempdir().unwrap();

        let _scope = ambient::test_guard();

        let immutable = OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable,
            vocab_format,
        )
        .unwrap();
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

        assert_eq!(mmap.storage.vocab.format(), vocab_format);
        let imm_mmap = ImmutableInvertedIndex::try_from(&mmap).unwrap();
        assert_eq!(imm_mmap.vocab, immutable.vocab);

        // Check same vocabulary
        let assert_same_id = |expected: TokenId, actual: Option<TokenId>| {
            assert_eq!(actual, Some(expected));
        };
        let vocab_iter = || immutable.vocab.iter().map(|(t, id)| (*id, t.as_str()));
        mmap.for_each_token_id(vocab_iter(), assert_same_id)
            .unwrap();
        imm_mmap
            .for_each_token_id(vocab_iter(), assert_same_id)
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
            .doc_len_codes()
            .expect("sidecar written for a scoring index")
            .to_vec();
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
            assert_eq!(mmap_counts[point_id], *count as usize);
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

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable).unwrap();
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

        let mmap_dir = tempfile::tempdir().unwrap();
        let immutable = OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable).unwrap();
        let lens_at_build = immutable
            .point_to_doc_len
            .clone()
            .expect("the fixture records lengths");

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

        let on_disk = mmap.doc_len_codes().unwrap();
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

        // Both totals are the build-time one, like `posting_len`: the
        // statistics count a deleted document until the next build.
        let build_total: u64 = lens_at_build
            .iter()
            .map(|&code| u64::from(decode_doc_len(code)))
            .sum();
        let _scope = ambient::test_guard();
        assert_eq!(
            mmap.total_tokens(),
            Some(build_total),
            "the on-disk total does not subtract runtime deletions",
        );
        assert_eq!(imm_mmap.total_tokens(), Some(build_total));
        assert_eq!(imm_mmap.document_count(), mmap.document_count());
        assert_eq!(
            doc_lens(&mmap, &[victim as PointOffsetType]),
            [Some(0)],
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
        let mmap_dir = tempfile::tempdir().unwrap();
        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);
        let empty_deleted = BitVec::new();

        let with_lengths = mutable_inverted_index(64, 0, phrase_matching);
        OnDiskInvertedIndex::create(mmap_dir.path().into(), with_lengths).unwrap();
        assert!(sidecar.exists());

        // The same points, indexed by a build that records nothing.
        let mut without_lengths = MutableInvertedIndex::new(phrase_matching, false);
        for idx in 0..64 {
            let tokens: Vec<String> = (0..=idx % 8).map(|_| generate_word()).collect();
            without_lengths
                .index_str_tokens(idx, &tokens, None)
                .unwrap();
        }
        OnDiskInvertedIndex::create(mmap_dir.path().into(), without_lengths).unwrap();

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

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable).unwrap();

        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);
        let full = fs_err::metadata(&sidecar).unwrap().len();
        fs_err::OpenOptions::new()
            .write(true)
            .open(&sidecar)
            .unwrap()
            .set_len(full + size_of::<u16>() as u64)
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

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable).unwrap();

        let sidecar = mmap_dir.path().join(POINT_TO_DOC_LEN_FILE);
        let full = fs_err::metadata(&sidecar).unwrap().len();
        fs_err::OpenOptions::new()
            .write(true)
            .open(&sidecar)
            .unwrap()
            .set_len(full - size_of::<u16>() as u64)
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

    /// A header written before it carried the total reads it as zero. Over
    /// live documents that is not a real total, so it is reported as absent.
    #[rstest]
    fn zero_header_total_reports_no_total(#[values(false, true)] phrase_matching: bool) {
        let mutable = mutable_inverted_index(200, 20, phrase_matching);

        let mmap_dir = tempfile::tempdir().unwrap();
        OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable).unwrap();

        let postings = mmap_dir.path().join(POSTINGS_FILE);
        let mut bytes = fs_err::read(&postings).unwrap();
        let offset = std::mem::offset_of!(PostingsHeader, total_tokens);
        bytes[offset..offset + size_of::<u64>()].fill(0);
        fs_err::write(&postings, bytes).unwrap();

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
        assert!(opened.points_count() > 0);
        assert_eq!(opened.total_tokens(), None);
    }

    /// Every backend answers `doc_len_batch` with the same number for every point,
    /// including the ones it holds no tokens for, and every total is the sum of
    /// those answers. This is the whole contract a scorer gets from a segment,
    /// so it is pinned across all four shapes rather than on the one that
    /// happens to be cheapest.
    #[rstest]
    fn doc_len_and_total_tokens_agree_across_backends(
        #[values(false, true)] phrase_matching: bool,
    ) {
        let _scope = ambient::test_guard();
        let mutable = mutable_inverted_index(200, 20, phrase_matching);

        let mmap_dir = tempfile::tempdir().unwrap();
        let immutable =
            OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable.clone()).unwrap();
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

        // Every point, deleted or empty included, then out of order, repeated
        // and past the index.
        let point_count = immutable.point_to_tokens_count.len() as PointOffsetType;
        let in_order: Vec<PointOffsetType> = (0..point_count).collect();
        let mut shuffled: Vec<PointOffsetType> = (0..point_count + 5).rev().collect();
        shuffled.extend([0, point_count / 2, 0]);
        for point_ids in [&in_order, &shuffled] {
            let expected = doc_lens(&mutable, point_ids);
            assert_eq!(doc_lens(&immutable, point_ids), expected);
            assert_eq!(doc_lens(&imm_mmap, point_ids), expected);
            assert_eq!(doc_lens(&mmap, point_ids), expected);
        }
        let live_total: u64 = doc_lens(&mutable, &in_order)
            .into_iter()
            .map(|doc_len| u64::from(doc_len.expect("every point of the index has a length")))
            .sum();

        // The on-disk backend holds its lengths in RAM too, so answering
        // reads nothing and bills nothing.
        let mmap_acc = AmbientContext::new();
        mmap_acc.measure(|| doc_lens(&mmap, &shuffled));
        assert_eq!(mmap_acc.hw_data()[HwMetric::PayloadIndexIoRead], 0);

        assert!(live_total > 0, "the fixture indexed nothing");

        for (backend, total) in [
            ("mutable", mutable.total_tokens()),
            ("immutable", immutable.total_tokens()),
            ("mmap", mmap.total_tokens()),
            ("immutable from mmap", imm_mmap.total_tokens()),
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
        let mut mutable = MutableInvertedIndex::new(phrase_matching, false);
        for idx in 0..16 {
            let tokens: Vec<String> = (0..=idx).map(|_| generate_word()).collect();
            mutable.index_str_tokens(idx, &tokens, None).unwrap();
        }
        let mmap_dir = tempfile::tempdir().unwrap();
        let immutable =
            OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable.clone()).unwrap();
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
        let point_ids: Vec<PointOffsetType> = (0..16).collect();
        assert_eq!(doc_lens(&mutable, &point_ids), [None; 16]);
        assert_eq!(doc_lens(&immutable, &point_ids), [None; 16]);
        assert_eq!(doc_lens(&mmap, &point_ids), [None; 16]);
        assert_eq!(mutable.total_tokens(), None);
        assert_eq!(immutable.total_tokens(), None);
        assert_eq!(mmap.total_tokens(), None);
    }

    /// Lengths past the exact range of the stored code are rounded the same
    /// way on every backend, and every total sums the rounded lengths, so a
    /// removal subtracts exactly what was added.
    #[rstest]
    fn long_documents_round_the_same_on_every_backend(
        #[values(false, true)] phrase_matching: bool,
    ) {
        let _scope = ambient::test_guard();
        let lens = [5, 8_191, 8_193, 100_003, 3_000_017, u32::MAX];
        let mut mutable = MutableInvertedIndex::new(phrase_matching, true);
        for (idx, &len) in (0..).zip(&lens) {
            let tokens: Vec<String> = (0..3).map(|_| generate_word()).collect();
            mutable.index_str_tokens(idx, &tokens, Some(len)).unwrap();
        }
        // Indexed then removed: must leave nothing behind in the total.
        let removed = lens.len() as PointOffsetType;
        mutable
            .index_str_tokens(removed, &[generate_word()], Some(77_777))
            .unwrap();
        mutable.remove(removed);

        let mmap_dir = tempfile::tempdir().unwrap();
        let immutable =
            OnDiskInvertedIndex::create(mmap_dir.path().into(), mutable.clone()).unwrap();
        let empty_deleted = BitVec::new();
        let open = || -> OnDiskInvertedIndex {
            OnDiskInvertedIndex::open(
                &MmapFs,
                mmap_dir.path().into(),
                Populate::No,
                phrase_matching,
                &empty_deleted,
            )
            .unwrap()
            .unwrap()
        };
        let mmap = open();
        let imm_mmap = ImmutableInvertedIndex::try_from(&mmap).unwrap();
        // The heap placement's path, which reads the files directly.
        let (loaded, _) = ImmutableInvertedIndex::open(
            &MmapFs,
            mmap_dir.path(),
            Populate::No,
            phrase_matching,
            &empty_deleted,
        )
        .unwrap()
        .unwrap();

        // Whatever the backends round to, the file keeps every length exactly,
        // the removed point's zeroed.
        let sidecar = fs_err::read(mmap_dir.path().join(POINT_TO_DOC_LEN_FILE)).unwrap();
        let mut exact = lens.to_vec();
        exact.push(0);
        assert_eq!(read_exact(&sidecar, exact.len()).unwrap(), exact);

        let point_ids: Vec<PointOffsetType> = (0..lens.len() as PointOffsetType).collect();
        let expected: Vec<Option<u32>> = lens.iter().map(|&len| Some(round_doc_len(len))).collect();
        assert_eq!(expected[..2], [Some(5), Some(8_191)], "exact range");
        assert_ne!(expected[2], Some(8_193), "rounded past the exact range");
        let expected_total: u64 = expected.iter().map(|len| u64::from(len.unwrap())).sum();

        for (backend, lengths, total) in [
            (
                "mutable",
                doc_lens(&mutable, &point_ids),
                mutable.total_tokens(),
            ),
            (
                "immutable",
                doc_lens(&immutable, &point_ids),
                immutable.total_tokens(),
            ),
            ("mmap", doc_lens(&mmap, &point_ids), mmap.total_tokens()),
            (
                "immutable from mmap",
                doc_lens(&imm_mmap, &point_ids),
                imm_mmap.total_tokens(),
            ),
            (
                "loaded from mmap",
                doc_lens(&loaded, &point_ids),
                loaded.total_tokens(),
            ),
        ] {
            assert_eq!(lengths, expected, "{backend}");
            assert_eq!(total, Some(expected_total), "{backend}");
        }
    }

    #[rstest]
    fn test_mmap_index_congruence(
        #[values(false, true)] phrase_matching: bool,
        #[values(VocabFormat::Hashmap, VocabFormat::Compact)] vocab_format: VocabFormat,
    ) {
        let indexed_count = 10000;
        let deleted_count = 500;

        let _scope = ambient::test_guard();
        let mmap_dir = tempfile::tempdir().unwrap();

        let mut mut_index = mutable_inverted_index(indexed_count, deleted_count, phrase_matching);

        OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mut_index.clone(),
            vocab_format,
        )
        .unwrap();
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

        let mut_parsed_queries = parse_all(&queries, &mut_index);
        let mmap_parsed_queries = parse_all(&queries, &mmap_index);
        let imm_mmap_parsed_queries = parse_all(&queries, &imm_mmap_index);

        check_query_congruence(
            &mut_parsed_queries,
            &mmap_parsed_queries,
            &imm_mmap_parsed_queries,
            &mut_index,
            &mmap_index,
            &imm_mmap_index,
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
        );
    }

    fn open_on_disk(dir: &std::path::Path, phrase_matching: bool) -> OnDiskInvertedIndex {
        OnDiskInvertedIndex::open(
            &MmapFs,
            dir.to_path_buf(),
            Populate::No,
            phrase_matching,
            &BitVec::new(),
        )
        .unwrap()
        .unwrap()
    }

    /// Only one vocabulary format is ever on disk: a rebuild in the other
    /// format removes the file of the first, and the file list follows.
    #[rstest]
    fn rebuilding_in_another_vocab_format_removes_the_other_file(
        #[values(false, true)] phrase_matching: bool,
    ) {
        let mmap_dir = tempfile::tempdir().unwrap();
        let hashmap = mmap_dir.path().join(VOCAB_FILE);
        let _scope = ambient::test_guard();
        let compact = mmap_dir.path().join(COMPACT_VOCAB_FILE);
        let mutable = mutable_inverted_index(64, 8, phrase_matching);

        for (format, written, removed) in [
            (VocabFormat::Hashmap, &hashmap, &compact),
            (VocabFormat::Compact, &compact, &hashmap),
            (VocabFormat::Hashmap, &hashmap, &compact),
        ] {
            let immutable = OnDiskInvertedIndex::create_with_vocab_format(
                mmap_dir.path().into(),
                mutable.clone(),
                format,
            )
            .unwrap();
            assert!(written.exists(), "{format:?}");
            assert!(!removed.exists(), "{format:?}");
            // The posting directory goes with the compact vocabulary.
            let directory = mmap_dir.path().join(POSTING_DIRECTORY_FILE);
            let resident = format == VocabFormat::Compact;
            assert_eq!(directory.exists(), resident, "{format:?}");

            let opened = open_on_disk(mmap_dir.path(), phrase_matching);
            assert_eq!(opened.storage.vocab.format(), format);
            assert_eq!(opened.storage.postings.has_directory(), resident);
            let files = opened.files();
            assert!(files.contains(written), "{format:?}");
            assert!(!files.contains(removed), "{format:?}");
            assert_eq!(files.contains(&directory), resident, "{format:?}");
            for (token, &id) in &immutable.vocab {
                opened
                    .for_each_token_id(std::iter::once(((), token.as_str())), |(), found| {
                        assert_eq!(found, Some(id), "{token:?}");
                    })
                    .unwrap();
            }
        }
    }

    /// Loaded whole into RAM, the index reports the vocabulary file it was
    /// read from, whichever the format: the list is what the segment manifest
    /// carries.
    #[rstest]
    fn loads_into_ram_from_either_vocab_format(
        #[values(VocabFormat::Hashmap, VocabFormat::Compact)] vocab_format: VocabFormat,
    ) {
        let mmap_dir = tempfile::tempdir().unwrap();
        let mutable = mutable_inverted_index(200, 20, true);
        let immutable = OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable,
            vocab_format,
        )
        .unwrap();

        let (loaded, files) = ImmutableInvertedIndex::open(
            &MmapFs,
            mmap_dir.path(),
            Populate::No,
            true,
            &BitVec::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(loaded.vocab, immutable.vocab);

        let (written, other) = match vocab_format {
            VocabFormat::Hashmap => (VOCAB_FILE, COMPACT_VOCAB_FILE),
            VocabFormat::Compact => (COMPACT_VOCAB_FILE, VOCAB_FILE),
        };
        assert!(files.contains(&mmap_dir.path().join(written)));
        assert!(!files.contains(&mmap_dir.path().join(other)));
        for file in &files {
            assert!(file.exists(), "{} listed but not on disk", file.display());
        }
    }

    /// Both formats on disk is a file set no build leaves behind, and nothing
    /// says which one the postings were written with.
    #[test]
    fn both_vocab_formats_on_disk_fail_the_open() {
        let mmap_dir = tempfile::tempdir().unwrap();
        let mutable = mutable_inverted_index(64, 0, false);
        OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable.clone(),
            VocabFormat::Compact,
        )
        .unwrap();
        let compact = fs_err::read(mmap_dir.path().join(COMPACT_VOCAB_FILE)).unwrap();
        OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable,
            VocabFormat::Hashmap,
        )
        .unwrap();
        fs_err::write(mmap_dir.path().join(COMPACT_VOCAB_FILE), compact).unwrap();

        let opened = OnDiskInvertedIndex::<MmapFile>::open(
            &MmapFs,
            mmap_dir.path().to_path_buf(),
            Populate::No,
            false,
            &BitVec::new(),
        );
        assert!(opened.is_err());
        let loaded = ImmutableInvertedIndex::open(
            &MmapFs,
            mmap_dir.path(),
            Populate::No,
            false,
            &BitVec::new(),
        );
        assert!(loaded.is_err());
    }

    /// The compact vocabulary is held in RAM and answers without IO; the hash
    /// map is read in place and accounts a read per token.
    #[rstest]
    fn compact_vocab_is_resident(
        #[values(VocabFormat::Hashmap, VocabFormat::Compact)] vocab_format: VocabFormat,
    ) {
        let mmap_dir = tempfile::tempdir().unwrap();
        let mutable = mutable_inverted_index(200, 0, false);
        let immutable = OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable,
            vocab_format,
        )
        .unwrap();
        let opened = open_on_disk(mmap_dir.path(), false);

        let compact = matches!(opened.storage.vocab, Vocab::Compact(_));
        assert_eq!(compact, vocab_format == VocabFormat::Compact);
        let vocab_bytes = opened.storage.vocab.ram_usage_bytes();
        assert_eq!(
            vocab_bytes > 0,
            compact,
            "{vocab_bytes} bytes of vocabulary"
        );
        assert!(opened.ram_usage_bytes() >= vocab_bytes);

        let acc = AmbientContext::new();
        acc.measure(|| {
            let tokens = immutable.vocab.keys().map(|token| ((), token.as_str()));
            opened
                .for_each_token_id(tokens, |(), id| assert!(id.is_some()))
                .unwrap();
        });
        let io = acc.hw_data()[HwMetric::PayloadIndexIoRead];
        assert_eq!(io == 0, compact, "{io} bytes accounted");

        // Posting lengths come from the directory, without IO either.
        let acc = AmbientContext::new();
        acc.measure(|| {
            for token_id in 0..immutable.vocab.len() as TokenId {
                assert!(opened.get_posting_len(token_id).unwrap().is_some());
            }
        });
        let io = acc.hw_data()[HwMetric::PayloadIndexIoRead];
        assert_eq!(io == 0, compact, "{io} bytes accounted for posting lengths");
        assert_eq!(opened.storage.postings.ram_usage_bytes() > 0, compact);
    }

    /// A posting directory of another build is ignored, instead of pointing
    /// posting reads at another build's offsets: the index reads its headers in
    /// place and answers as without one.
    #[test]
    fn a_stale_posting_directory_is_ignored() {
        let mmap_dir = tempfile::tempdir().unwrap();
        let _scope = ambient::test_guard();
        let directory = mmap_dir.path().join(POSTING_DIRECTORY_FILE);
        OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable_inverted_index(64, 0, false),
            VocabFormat::Compact,
        )
        .unwrap();
        let stale = fs_err::read(&directory).unwrap();
        let immutable = OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable_inverted_index(80, 0, false),
            VocabFormat::Compact,
        )
        .unwrap();
        fs_err::write(&directory, stale).unwrap();

        let opened = open_on_disk(mmap_dir.path(), false);
        assert!(!opened.storage.postings.has_directory());
        assert!(!opened.files().contains(&directory));
        for token_id in 0..immutable.postings.len() as TokenId {
            let expected: Vec<_> = immutable.postings.iter_ids(token_id).unwrap().collect();
            let actual: Vec<_> = opened
                .storage
                .postings
                .iter_ids(token_id)
                .unwrap()
                .collect();
            assert_eq!(actual, expected, "token {token_id}");
        }
    }

    /// `preopen` fetches the compact vocabulary ahead of `open`, which then
    /// reads it from the prefetch rather than from storage; a hash map
    /// vocabulary still preopens and opens.
    #[rstest]
    fn preopen_fetches_the_vocab(
        #[values(VocabFormat::Hashmap, VocabFormat::Compact)] vocab_format: VocabFormat,
    ) {
        use common::universal_io::{
            CachedFs, CachedReadFs as _, ReadOnly, UniversalRead, UniversalReadFs as _,
        };

        type Storage = ReadOnly<MmapFile>;
        type RoFs = <Storage as UniversalRead>::Fs;
        let _scope = ambient::test_guard();

        let mmap_dir = tempfile::tempdir().unwrap();
        let mutable = mutable_inverted_index(200, 20, true);
        let immutable = OnDiskInvertedIndex::create_with_vocab_format(
            mmap_dir.path().into(),
            mutable,
            vocab_format,
        )
        .unwrap();

        let fs = RoFs::from_context(Default::default()).unwrap();
        let mut cached_fs = CachedFs::new(fs, mmap_dir.path()).unwrap();
        cached_fs.cache_file_info().unwrap();
        assert!(
            OnDiskInvertedIndex::<Storage>::preopen(
                &cached_fs,
                mmap_dir.path(),
                Populate::PreferBackground,
            )
            .unwrap()
        );
        futures::executor::block_on(cached_fs.wait_all());

        if vocab_format == VocabFormat::Compact {
            // The read of `open` must now come from the prefetch pool.
            fs_err::remove_file(mmap_dir.path().join(COMPACT_VOCAB_FILE)).unwrap();
            fs_err::remove_file(mmap_dir.path().join(POSTING_DIRECTORY_FILE)).unwrap();
        }

        let opened = OnDiskInvertedIndex::<Storage>::open(
            &cached_fs,
            mmap_dir.path().to_path_buf(),
            Populate::PreferBackground,
            true,
            &BitVec::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(opened.storage.vocab.format(), vocab_format);
        let tokens = immutable
            .vocab
            .iter()
            .map(|(token, &id)| (id, token.as_str()));
        opened
            .for_each_token_id(tokens, |expected, found| assert_eq!(found, Some(expected)))
            .unwrap();
    }

    fn check_query_congruence(
        mut_parsed_queries: &[Option<ParsedQuery>],
        mmap_parsed_queries: &[Option<ParsedQuery>],
        imm_mmap_parsed_queries: &[Option<ParsedQuery>],
        mut_index: &MutableInvertedIndex,
        mmap_index: &OnDiskInvertedIndex,
        imm_mmap_index: &ImmutableInvertedIndex,
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
            let mut_filtered = mut_index.filter(mut_query).unwrap().collect::<Vec<_>>();
            let imm_filtered = mmap_index.filter(imm_query).unwrap().collect::<Vec<_>>();
            let imm_mmap_filtered = imm_mmap_index
                .filter(imm_mmap_query)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(mut_filtered, imm_filtered);
            assert_eq!(imm_filtered, imm_mmap_filtered);
        }
    }
}
