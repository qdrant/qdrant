//! The immutable and on-disk indexes check the query tokens of a point in the order that
//! decides the point with the fewest posting list lookups: the rarest token first when all
//! must match, the most common first when any may. The results don't depend on the order,
//! so these tests count the lookups.

use common::ambient;
use common::types::PointOffsetType;
use rstest::rstest;
use serde_json::Value;

use super::test_congruence::{IndexType, create_builder, resolve_tokens};
use crate::index::field_index::full_text_index::FullTextIndex;
use crate::index::field_index::full_text_index::full_text_index_read::FullTextIndexRead;
use crate::index::field_index::full_text_index::inverted_index::posting_lookups::posting_lookups;
use crate::index::field_index::full_text_index::inverted_index::{ParsedQuery, TokenSet};

const POINT_COUNT: PointOffsetType = 10;

/// `apple` and `yak` are in every point, `zebra` and `bee` only in point 0. Of the pairs
/// (`apple`, `zebra`) and (`yak`, `bee`) one has the common token first in token id order and
/// the other the rare one, whether ids follow first occurrence or the alphabet, so a check in
/// token id order takes two lookups for one of them.
fn build_index(index_type: IndexType) -> (FullTextIndex, tempfile::TempDir) {
    let (mut builder, temp_dir, _db) = create_builder(index_type, false);
    for point_id in 0..POINT_COUNT {
        let text = match point_id {
            0 => "apple zebra bee yak",
            _ => "apple yak filler",
        };
        builder
            .add_point(point_id, &[&Value::String(text.to_string())])
            .unwrap();
    }
    (builder.finalize().unwrap(), temp_dir)
}

fn tokens(index: &FullTextIndex, words: &[&str]) -> TokenSet {
    resolve_tokens(index, words)
        .into_iter()
        .collect::<Option<TokenSet>>()
        .unwrap()
}

/// Lookups and results of checking `query` on points 1.., one by one and in a batch.
fn check_points(index: &FullTextIndex, query: &ParsedQuery) -> [(usize, Vec<bool>); 2] {
    let points = 1..POINT_COUNT;

    let mut single = Vec::new();
    let single_lookups = posting_lookups(|| {
        for point_id in points.clone() {
            single.push(index.check_match(query, point_id).unwrap());
        }
    });

    let mut batch = vec![false; points.len()];
    let batch_lookups = posting_lookups(|| {
        index
            .check_match_batch(query, points.clone().map(|id| (id, id)), |id, matched| {
                batch[id as usize - 1] = matched;
            })
            .unwrap();
    });

    [(single_lookups, single), (batch_lookups, batch)]
}

#[rstest]
fn test_token_order(#[values(IndexType::OnDisk, IndexType::Immutable)] index_type: IndexType) {
    let _scope = ambient::test_guard();
    let (index, _temp_dir) = build_index(index_type);
    let checked = (POINT_COUNT - 1) as usize;

    for [common, rare] in [["apple", "zebra"], ["yak", "bee"]] {
        let all = ParsedQuery::AllTokens(tokens(&index, &[common, rare]));
        let any = ParsedQuery::AnyTokens(tokens(&index, &[common, rare]));

        // The counter sees every lookup: point 0 has both tokens, so all of them are checked.
        let both = posting_lookups(|| assert!(index.check_match(&all, 0).unwrap()));
        assert_eq!(both, 2, "{index_type:?}: point 0, all of {common} {rare}");

        // All tokens: the rare one rejects the other points in one lookup.
        for (lookups, matched) in check_points(&index, &all) {
            assert_eq!(matched, vec![false; checked]);
            assert_eq!(lookups, checked, "{index_type:?}: all of {common} {rare}");
        }

        // Any token: the common one accepts the other points in one lookup.
        for (lookups, matched) in check_points(&index, &any) {
            assert_eq!(matched, vec![true; checked]);
            assert_eq!(lookups, checked, "{index_type:?}: any of {common} {rare}");
        }
    }
}
