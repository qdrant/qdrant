//! Regex filtering over a keyword index, with and without the trigram index.
//!
//! Keys are synthetic URLs by default. Set `REGEX_BENCH_KEYS` to a file with
//! one key per line (e.g. Wikipedia titles) to bench on real data, and
//! `REGEX_BENCH_PATTERNS` to a file with one pattern per line.

use std::path::Path;

use common::ambient;
use common::bitvec::BitVec;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::SmallRng;
use rand::seq::IndexedRandom;
use rand::{RngExt, SeedableRng};
use segment::index::field_index::map_index::MapIndex;
use segment::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndexRead};
use segment::json_path::JsonPath;
use segment::types::{FieldCondition, Match, Memory};
use serde_json::Value;
use tempfile::Builder;

const NUM_KEYS: usize = 200_000;

const PATTERNS: &[&str] = &[
    // Literals anywhere: leading wildcard, served by trigrams.
    "qdrant",
    "(?i)REPORT",
    r"\.pdf$",
    "/blog/.*-42",
    // Anchored literal prefix.
    r"^https://docs\.",
    // Required literal around unconstrained parts.
    r"-\d{4}\.",
    // No literal of 3 bytes: every key is checked either way.
    "^[a-z]+://[a-z]+$",
];

fn synthetic_keys() -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(42);
    let hosts = [
        "qdrant.tech",
        "docs.qdrant.tech",
        "example.com",
        "files.example.org",
    ];
    let sections = ["blog", "docs", "articles", "reports", "media"];
    let words = [
        "vector", "search", "index", "Report", "filter", "payload", "trigram",
    ];
    let extensions = ["html", "pdf", "PDF", "tar.gz", "json"];
    (0..NUM_KEYS)
        .map(|i| {
            format!(
                "https://{}/{}/{}-{}.{}",
                hosts.choose(&mut rng).unwrap(),
                sections.choose(&mut rng).unwrap(),
                words.choose(&mut rng).unwrap(),
                rng.random_range(0..10_000) + i % 7,
                extensions.choose(&mut rng).unwrap(),
            )
        })
        .collect()
}

fn read_lines(path: &str) -> Vec<String> {
    fs_err::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect()
}

fn build(path: &Path, keys: &[String], appendable: bool, regex: bool) -> MapIndex<str> {
    let _scope = ambient::test_guard();
    let deleted = BitVec::repeat(false, keys.len());
    let add = |builder: &mut dyn FnMut(PointOffsetType, &[&Value])| {
        for (idx, key) in keys.iter().enumerate() {
            let value = Value::from(key.as_str());
            builder(idx as PointOffsetType, &[&value]);
        }
    };
    if appendable {
        let mut builder = MapIndex::<str>::builder_mutable(path.to_path_buf(), false);
        if regex {
            builder.enable_regex_index();
        }
        builder.init().unwrap();
        add(&mut |idx, values| builder.add_point(idx, values).unwrap());
        builder.finalize().unwrap()
    } else {
        let mut builder = MapIndex::<str>::builder_immutable(path, Memory::Cold, &deleted, false);
        if regex {
            builder.enable_regex_index();
        }
        builder.init().unwrap();
        add(&mut |idx, values| builder.add_point(idx, values).unwrap());
        builder.finalize().unwrap()
    }
}

fn regex_filtering(c: &mut Criterion) {
    let keys = match std::env::var("REGEX_BENCH_KEYS") {
        Ok(path) => read_lines(&path),
        Err(_) => synthetic_keys(),
    };
    let patterns: Vec<String> = match std::env::var("REGEX_BENCH_PATTERNS") {
        Ok(path) => read_lines(&path),
        Err(_) => PATTERNS.iter().map(|p| p.to_string()).collect(),
    };

    let mut group = c.benchmark_group("regex-filtering");
    group.sample_size(10);

    for (variant, appendable) in [("appendable", true), ("on-disk", false)] {
        for (index, regex) in [("trigram", true), ("scan", false)] {
            let dir = Builder::new().prefix("regex_bench").tempdir().unwrap();
            let map_index = build(dir.path(), &keys, appendable, regex);

            for pattern in &patterns {
                let condition =
                    FieldCondition::new_match(JsonPath::new("key"), Match::new_regex(pattern));
                let _scope = ambient::test_guard();
                let matches = map_index.filter(&condition).unwrap().unwrap().count();
                group.bench_function(
                    format!("{variant}/{index}/{pattern} ({matches} matches)"),
                    |b| {
                        b.iter(|| {
                            let _scope = ambient::test_guard();
                            map_index.filter(&condition).unwrap().unwrap().count()
                        })
                    },
                );
            }
        }
    }

    group.finish();
}

criterion_group!(benches, regex_filtering);
criterion_main!(benches);
