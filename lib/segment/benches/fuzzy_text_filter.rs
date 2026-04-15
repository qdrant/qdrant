/// Benchmark for the fuzzy-filter pipeline.
///
/// The full pipeline for a single `MatchFuzzy` filter call:
///
///   1. Tokenization          – split the query string into tokens
///   2. FST expansion         – for each token, search the FST with a Levenshtein
///                              automaton to find candidate terms  (or linear scan
///                              over BTreeSet for the Mutable variant)
///   3. Token-ID lookup       – map each candidate term → integer token-id
///   4. Posting-list traversal – merge/intersect the posting lists and collect
///                               matching point-ids
///
/// Each stage is benched individually so that the hotspot is obvious.
use std::hint::black_box;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::prelude::*;
use segment::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
use segment::index::field_index::full_text_index::fuzzy_index::FuzzyIndex;
use segment::index::field_index::full_text_index::text_index::FullTextIndex;
use segment::index::field_index::full_text_index::tokenizers::Tokenizer;
use segment::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};
use segment::types::{Fuzzy, FuzzyParams, MatchFuzzy};
use tempfile::TempDir;

// ── Corpus / vocabulary sizes ──────────────────────────────────────────────
const NUM_DOCS: usize = 50_000;
const WORDS_PER_DOC: usize = 12;
const VOCAB_SIZE: usize = 20_000;
const SEED: u64 = 0xDEAD_BEEF;

// ── Benchmark query words ──────────────────────────────────────────────────
// These are words that are guaranteed to exist in the vocabulary (we pull them
// from the generated vocab list at index-build time).  We use indices rather
// than literals so nothing is hard-coded.
const QUERY_WORD_IDX: usize = 42; // single-token query  → vocab[42]
const QUERY_WORDS_IDX: [usize; 3] = [42, 1337, 9999]; // 3-token query

// ── Helpers ────────────────────────────────────────────────────────────────

/// Generate a deterministic vocabulary of `n` random lowercase words.
fn generate_vocab(n: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut vocab: Vec<String> = (0..n)
        .map(|_| {
            let len: usize = rng.random_range(5..=10);
            (0..len)
                .map(|_| (b'a' + rng.random_range(0u8..26)) as char)
                .collect()
        })
        .collect();
    // FST requires sorted, deduplicated input – ensure vocab is unique and sorted.
    vocab.sort_unstable();
    vocab.dedup();
    vocab
}

/// Build a `FullTextIndex` (Mutable variant, gridstore-backed) from a random corpus.
///
/// Returns (index, temp-dir-handle, vocab-snapshot).
/// The caller must keep `TempDir` alive for as long as the index is used.
fn build_mutable_index(config: TextIndexParams) -> (FullTextIndex, TempDir, Vec<String>) {
    let vocab = generate_vocab(VOCAB_SIZE);
    let dir = tempfile::Builder::new()
        .prefix("fuzzy_bench")
        .tempdir()
        .unwrap();

    let mut builder = FullTextIndex::builder_gridstore(dir.path().to_path_buf(), config);
    builder.init().unwrap();

    let hw = HardwareCounterCell::disposable();
    let mut rng = StdRng::seed_from_u64(SEED + 1);

    for doc_id in 0..NUM_DOCS {
        // Build a document as a space-separated sequence of random vocab words.
        let text: String = (0..WORDS_PER_DOC)
            .map(|_| vocab[rng.random_range(0..vocab.len())].as_str())
            .collect::<Vec<_>>()
            .join(" ");
        builder
            .add_many(doc_id as PointOffsetType, vec![text], &hw)
            .unwrap();
    }

    let index = builder.finalize().unwrap();
    (index, dir, vocab)
}

/// Build a `FullTextIndex` (Mmap variant, populated into RAM) from a random corpus.
fn build_mmap_index(config: TextIndexParams) -> (FullTextIndex, TempDir, Vec<String>) {
    let vocab = generate_vocab(VOCAB_SIZE);
    let dir = tempfile::Builder::new()
        .prefix("fuzzy_bench_mmap")
        .tempdir()
        .unwrap();

    let hw = HardwareCounterCell::disposable();
    let mut rng = StdRng::seed_from_u64(SEED + 1);

    let mut builder = FullTextIndex::builder_mmap(dir.path().to_path_buf(), config, false);
    builder.init().unwrap();

    for doc_id in 0..NUM_DOCS {
        let text: String = (0..WORDS_PER_DOC)
            .map(|_| vocab[rng.random_range(0..vocab.len())].as_str())
            .collect::<Vec<_>>()
            .join(" ");
        builder
            .add_many(doc_id as PointOffsetType, vec![text], &hw)
            .unwrap();
    }

    let index = builder.finalize().unwrap();
    (index, dir, vocab)
}

/// Convenience: return a `TextIndexParams` with fuzzy matching enabled.
fn fuzzy_config() -> TextIndexParams {
    TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Word,
        min_token_len: None,
        max_token_len: None,
        lowercase: Some(true),
        phrase_matching: None,
        fuzzy_matching: Some(true),
        stopwords: None,
        on_disk: None,
        stemmer: None,
        ascii_folding: None,
        enable_hnsw: None,
    }
}

// ── Stage-1: Tokenisation ─────────────────────────────────────────────────

fn bench_tokenization(c: &mut Criterion) {
    let config = fuzzy_config();
    let tokenizer = Tokenizer::new_from_text_index_params(&config);
    let vocab = generate_vocab(VOCAB_SIZE);

    let single_word = vocab[QUERY_WORD_IDX].clone();
    let multi_word = QUERY_WORDS_IDX
        .iter()
        .map(|&i| vocab[i].as_str())
        .collect::<Vec<_>>()
        .join(" ");

    let mut group = c.benchmark_group("fuzzy_filter/1_tokenize");

    group.bench_function("single_word", |b| {
        b.iter(|| {
            let mut count = 0usize;
            tokenizer.tokenize_query(&single_word, |t| {
                black_box(t);
                count += 1;
            });
            black_box(count);
        })
    });

    group.bench_function("multi_word_3", |b| {
        b.iter(|| {
            let mut count = 0usize;
            tokenizer.tokenize_query(&multi_word, |t| {
                black_box(t);
                count += 1;
            });
            black_box(count);
        })
    });

    group.finish();
}

// ── Stage-2: FST expansion ─────────────────────────────────────────────────

fn bench_fst_expansion(c: &mut Criterion) {
    let (mutable_idx, _dir_m, vocab) = build_mutable_index(fuzzy_config());
    let (immutable_idx, _dir_i, _) = build_mmap_index(fuzzy_config());

    let query_word = vocab[QUERY_WORD_IDX].clone();

    // Obtain the fuzzy-index implementations.
    let fuzzy_mutable: &dyn FuzzyIndex = match &mutable_idx {
        FullTextIndex::Mutable(m) => m.get_fuzzy_index().expect("fuzzy must be enabled"),
        _ => unreachable!(),
    };
    let fuzzy_immutable: &dyn FuzzyIndex = match &immutable_idx {
        FullTextIndex::Immutable(m) => m.get_fuzzy_index().expect("fuzzy must be enabled"),
        _ => unreachable!(),
    };

    let params_e1_x30 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 30,
    };
    let params_e2_x30 = FuzzyParams {
        max_edits: 2,
        prefix_length: 0,
        max_expansions: 30,
    };
    let params_e1_x10 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 10,
    };
    let params_e1_x100 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 100,
    };

    let mut group = c.benchmark_group("fuzzy_filter/2_fst_expansion");

    // Mutable index (BTreeSet linear scan)
    for (label, params) in [
        ("mutable/e1_x30", &params_e1_x30),
        ("mutable/e2_x30", &params_e2_x30),
        ("mutable/e1_x10", &params_e1_x10),
        ("mutable/e1_x100", &params_e1_x100),
    ] {
        group.bench_with_input(BenchmarkId::new(label, &query_word), params, |b, p| {
            b.iter(|| black_box(fuzzy_mutable.search_levenshtein(&query_word, p)))
        });
    }

    // Immutable index (FST + Levenshtein automaton)
    for (label, params) in [
        ("immutable/e1_x30", &params_e1_x30),
        ("immutable/e2_x30", &params_e2_x30),
        ("immutable/e1_x10", &params_e1_x10),
        ("immutable/e1_x100", &params_e1_x100),
    ] {
        group.bench_with_input(BenchmarkId::new(label, &query_word), params, |b, p| {
            b.iter(|| black_box(fuzzy_immutable.search_levenshtein(&query_word, p)))
        });
    }

    group.finish();
}

// ── Stage-3: parse_fuzzy_query (tokenize + expand + token-id lookup) ───────

fn make_match_fuzzy_text(text: &str, params: FuzzyParams) -> MatchFuzzy {
    MatchFuzzy {
        fuzzy: vec![Fuzzy::Text {
            text: text.to_string(),
            params: Some(params),
        }],
    }
}

fn bench_parse_fuzzy_query(c: &mut Criterion) {
    let (mutable_idx, _dir_m, vocab) = build_mutable_index(fuzzy_config());
    let (immutable_idx, _dir_i, _) = build_mmap_index(fuzzy_config());
    let hw = HardwareCounterCell::disposable();

    let single_word = vocab[QUERY_WORD_IDX].clone();
    let multi_word = QUERY_WORDS_IDX
        .iter()
        .map(|&i| vocab[i].as_str())
        .collect::<Vec<_>>()
        .join(" ");

    let params_e1 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 30,
    };
    let params_e2 = FuzzyParams {
        max_edits: 2,
        prefix_length: 0,
        max_expansions: 30,
    };

    let mut group = c.benchmark_group("fuzzy_filter/3_parse_query");

    for (label, word) in [("single", &single_word), ("multi_3", &multi_word)] {
        // Baseline: exact token lookup without any fuzzy expansion.
        {
            let bench_id = format!("baseline/mutable/{label}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| black_box(mutable_idx.parse_text_query(word, &hw)))
            });
            let bench_id = format!("baseline/immutable/{label}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| black_box(immutable_idx.parse_text_query(word, &hw)))
            });
        }
        for (plabel, params) in [("e1", &params_e1), ("e2", &params_e2)] {
            let mf = make_match_fuzzy_text(word, *params);
            let bench_id = format!("mutable/{label}/{plabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| black_box(mutable_idx.parse_fuzzy_query(&mf, &hw)))
            });
            let bench_id = format!("immutable/{label}/{plabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| black_box(immutable_idx.parse_fuzzy_query(&mf, &hw)))
            });
        }
    }

    group.finish();
}

// ── Stage-4: Posting-list traversal (collect results) ─────────────────────

fn bench_filter_query(c: &mut Criterion) {
    let (mutable_idx, _dir_m, vocab) = build_mutable_index(fuzzy_config());
    let (immutable_idx, _dir_i, _) = build_mmap_index(fuzzy_config());
    let hw = HardwareCounterCell::disposable();

    let single_word = vocab[QUERY_WORD_IDX].clone();
    let multi_word = QUERY_WORDS_IDX
        .iter()
        .map(|&i| vocab[i].as_str())
        .collect::<Vec<_>>()
        .join(" ");

    let params_e1 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 30,
    };

    let mut group = c.benchmark_group("fuzzy_filter/4_posting_traversal");

    // Pre-compute queries so we only time the traversal, not the parse.
    for (qlabel, word) in [("single", &single_word), ("multi_3", &multi_word)] {
        // Baseline: exact token lookup, no fuzzy expansion.
        if let Some(query_baseline_m) = mutable_idx.parse_text_query(word, &hw) {
            let bench_id = format!("baseline/mutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    let q = query_baseline_m.clone();
                    let count = mutable_idx.filter_query(q, &hw).count();
                    black_box(count)
                })
            });
        }

        if let Some(query_baseline_i) = immutable_idx.parse_text_query(word, &hw) {
            let bench_id = format!("baseline/immutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    let q = query_baseline_i.clone();
                    let count = immutable_idx.filter_query(q, &hw).count();
                    black_box(count)
                })
            });
        }

        let mf = make_match_fuzzy_text(word, params_e1);

        if let Some(query_m) = mutable_idx.parse_fuzzy_query(&mf, &hw) {
            let bench_id = format!("mutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    let q = query_m.clone();
                    let count = mutable_idx.filter_query(q, &hw).count();
                    black_box(count)
                })
            });
        }

        if let Some(query_i) = immutable_idx.parse_fuzzy_query(&mf, &hw) {
            let bench_id = format!("immutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    let q = query_i.clone();
                    let count = immutable_idx.filter_query(q, &hw).count();
                    black_box(count)
                })
            });
        }
    }

    group.finish();
}

// ── Stage-5: End-to-end (parse + filter) ──────────────────────────────────

fn bench_end_to_end(c: &mut Criterion) {
    let (mutable_idx, _dir_m, vocab) = build_mutable_index(fuzzy_config());
    let (immutable_idx, _dir_i, _) = build_mmap_index(fuzzy_config());
    let hw = HardwareCounterCell::disposable();

    let single_word = vocab[QUERY_WORD_IDX].clone();
    let multi_word = QUERY_WORDS_IDX
        .iter()
        .map(|&i| vocab[i].as_str())
        .collect::<Vec<_>>()
        .join(" ");

    let params_e1 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 30,
    };
    let params_e2 = FuzzyParams {
        max_edits: 2,
        prefix_length: 0,
        max_expansions: 30,
    };

    let mut group = c.benchmark_group("fuzzy_filter/5_end_to_end");

    for (qlabel, word) in [("single", &single_word), ("multi_3", &multi_word)] {
        // Baseline: exact match, no fuzzy expansion.
        {
            let bench_id = format!("baseline/mutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    if let Some(q) = mutable_idx.parse_text_query(word, &hw) {
                        black_box(mutable_idx.filter_query(q, &hw).count())
                    } else {
                        0
                    }
                })
            });
            let bench_id = format!("baseline/immutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    if let Some(q) = immutable_idx.parse_text_query(word, &hw) {
                        black_box(immutable_idx.filter_query(q, &hw).count())
                    } else {
                        0
                    }
                })
            });
        }
        for (plabel, params) in [("e1_x30", params_e1), ("e2_x30", params_e2)] {
            let mf = make_match_fuzzy_text(word, params);
            let bench_id = format!("mutable/{qlabel}/{plabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    if let Some(q) = mutable_idx.parse_fuzzy_query(&mf, &hw) {
                        black_box(mutable_idx.filter_query(q, &hw).count())
                    } else {
                        0
                    }
                })
            });
            let bench_id = format!("immutable/{qlabel}/{plabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    if let Some(q) = immutable_idx.parse_fuzzy_query(&mf, &hw) {
                        black_box(immutable_idx.filter_query(q, &hw).count())
                    } else {
                        0
                    }
                })
            });
        }
    }

    group.finish();
}

// ── Stage-6: Phrase + fuzzy posting traversal ─────────────────────────────
//
// This stage specifically exercises `intersect_compressed_postings_fuzzy_phrase_iterator`
// (peekable-iterator path) by enabling both phrase_matching AND fuzzy_matching.
// Each query is a multi-word phrase expanded with edit-distance 1 per token.

fn phrase_fuzzy_config() -> TextIndexParams {
    TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Word,
        min_token_len: None,
        max_token_len: None,
        lowercase: Some(true),
        phrase_matching: Some(true),
        fuzzy_matching: Some(true),
        stopwords: None,
        on_disk: None,
        stemmer: None,
        ascii_folding: None,
        enable_hnsw: None,
    }
}

fn bench_phrase_fuzzy_traversal(c: &mut Criterion) {
    let (mutable_idx, _dir_m, vocab) = build_mutable_index(phrase_fuzzy_config());
    let (immutable_idx, _dir_i, _) = build_mmap_index(phrase_fuzzy_config());
    let hw = HardwareCounterCell::disposable();

    // 2-word and 3-word phrase queries; words are guaranteed to exist in the index.
    let phrase2 = [QUERY_WORDS_IDX[0], QUERY_WORDS_IDX[1]]
        .iter()
        .map(|&i| vocab[i].as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let phrase3 = QUERY_WORDS_IDX
        .iter()
        .map(|&i| vocab[i].as_str())
        .collect::<Vec<_>>()
        .join(" ");

    let params_e1 = FuzzyParams {
        max_edits: 1,
        prefix_length: 0,
        max_expansions: 30,
    };

    let mut group = c.benchmark_group("fuzzy_filter/6_phrase_fuzzy_traversal");

    for (qlabel, phrase) in [("phrase2", &phrase2), ("phrase3", &phrase3)] {
        let mf = make_match_fuzzy_text(phrase, params_e1);

        if let Some(query_m) = mutable_idx.parse_fuzzy_query(&mf, &hw) {
            let bench_id = format!("mutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    let q = query_m.clone();
                    black_box(mutable_idx.filter_query(q, &hw).count())
                })
            });
        }

        if let Some(query_i) = immutable_idx.parse_fuzzy_query(&mf, &hw) {
            let bench_id = format!("immutable/{qlabel}");
            group.bench_function(&bench_id, |b| {
                b.iter(|| {
                    let q = query_i.clone();
                    black_box(immutable_idx.filter_query(q, &hw).count())
                })
            });
        }
    }

    group.finish();
}

// ── Criterion wiring ───────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_tokenization,
    bench_fst_expansion,
    bench_parse_fuzzy_query,
    bench_filter_query,
    bench_end_to_end,
    bench_phrase_fuzzy_traversal,
);
criterion_main!(benches);
