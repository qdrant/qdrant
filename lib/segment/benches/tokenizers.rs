//! Tokenizer throughput over a corpus of source files.
//!
//! The corpus is every file under `TOKENIZER_BENCH_CORPUS` (default: this crate's `src`) with one
//! of the extensions below, one document per file.

use std::path::{Path, PathBuf};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use segment::data_types::index::{TextIndexParams, TokenizerType};
use segment::index::field_index::full_text_index::tokenizers::Tokenizer;

const EXTENSIONS: &[&str] = &["rs", "py", "js", "ts", "go", "java", "c", "h", "cpp", "md"];

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs_err::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| EXTENSIONS.contains(&ext))
        {
            files.push(path);
        }
    }
}

fn load_corpus() -> Vec<String> {
    let dir = std::env::var("TOKENIZER_BENCH_CORPUS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("src"));
    let mut files = Vec::new();
    collect_files(&dir, &mut files);
    files.sort();
    files
        .iter()
        .filter_map(|path| fs_err::read_to_string(path).ok())
        .collect()
}

fn tokenizers(c: &mut Criterion) {
    let corpus = load_corpus();
    let bytes: usize = corpus.iter().map(String::len).sum();
    assert!(bytes > 0, "empty corpus");

    let mut group = c.benchmark_group("tokenize-doc");
    group.throughput(Throughput::Bytes(bytes as u64));
    group.sample_size(20);

    let types = [
        ("word", TokenizerType::Word),
        ("whitespace", TokenizerType::Whitespace),
        ("prefix", TokenizerType::Prefix),
        ("multilingual", TokenizerType::Multilingual),
        ("code", TokenizerType::Code),
    ];
    for (name, tokenizer_type) in types {
        let tokenizer = Tokenizer::new_from_text_index_params(&TextIndexParams {
            tokenizer: tokenizer_type,
            ..Default::default()
        });

        let mut tokens = 0;
        for document in &corpus {
            tokenizer.tokenize_doc(document, |_| tokens += 1);
        }
        eprintln!(
            "{name}: {bytes} bytes, {} documents, {tokens} tokens",
            corpus.len()
        );

        group.bench_function(name, |b| {
            b.iter(|| {
                let mut tokens = Vec::new();
                for document in &corpus {
                    tokens.clear();
                    tokenizer.tokenize_doc(document, |token| tokens.push(token));
                    std::hint::black_box(&tokens);
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, tokenizers);
criterion_main!(benches);
