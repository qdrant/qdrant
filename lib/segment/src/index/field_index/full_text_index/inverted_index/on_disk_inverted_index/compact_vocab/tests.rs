use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::super::varint::write_varint;
use super::format::{encode, read, save};
use super::header::Header;
use super::*;

fn sorted_vocab(tokens: &[&str]) -> HashMap<String, TokenId> {
    let mut tokens = tokens.to_vec();
    tokens.sort_unstable();
    tokens.dedup();
    (0..)
        .zip(tokens)
        .map(|(id, token)| (token.to_string(), id))
        .collect()
}

fn encode_vocab(vocab: &HashMap<String, TokenId>) -> Vec<u8> {
    let mut tokens: Vec<&str> = vocab.keys().map(String::as_str).collect();
    tokens.sort_unstable();
    encode(tokens.iter().copied(), tokens.len()).unwrap()
}

fn round_trip(vocab: &HashMap<String, TokenId>) -> Vec<u8> {
    let bytes = encode_vocab(vocab);
    let loaded = read(&bytes).unwrap();
    assert_eq!(loaded.keys_count(), vocab.len());
    for (token, &id) in vocab {
        assert_eq!(loaded.get(token), Some(id), "{token:?}");
    }
    let mut entries = Vec::new();
    loaded
        .for_each_entry(|token, id| {
            entries.push((token.to_string(), id));
            Ok::<_, ()>(())
        })
        .unwrap();
    let mut expected: Vec<_> = vocab.iter().map(|(k, &v)| (k.clone(), v)).collect();
    expected.sort_unstable();
    assert_eq!(entries, expected);
    bytes
}

#[test]
fn finds_every_token_and_nothing_else() {
    let vocab = sorted_vocab(&[
        "a",
        "ab",
        "abc",
        "abd",
        "b",
        "ba",
        "zz",
        "zzz",
        "é",
        "éa",
        "日本",
        "日本語",
    ]);
    let bytes = round_trip(&vocab);
    let loaded = read(&bytes).unwrap();
    for missing in ["", "0", "aa", "abcd", "bb", "c", "zzzz", "\u{10FFFF}", "日"] {
        assert_eq!(loaded.get(missing), None, "{missing:?}");
    }
}

#[test]
fn handles_block_boundaries() {
    let mut rng = StdRng::seed_from_u64(42);
    for count in [0, 1, 15, 16, 17, 32, 33, 1000] {
        let tokens: Vec<String> = (0..count * 2)
            .map(|_| {
                let len = rng.random_range(1..=8);
                (&mut rng)
                    .sample_iter(rand::distr::Alphanumeric)
                    .take(len)
                    .map(char::from)
                    .collect()
            })
            .collect();
        let tokens: Vec<&str> = tokens.iter().take(count).map(String::as_str).collect();
        round_trip(&sorted_vocab(&tokens));
    }
}

/// Tokens sharing their first 8 bytes tie on the anchor, across many
/// blocks; lookups then fall back to the first tokens.
#[test]
fn finds_tokens_whose_anchors_tie() {
    let tokens: Vec<String> = (0..500)
        .map(|i| format!("internationalization{i:04}"))
        .chain(["intern", "internal", "internationalizatio", "zebra"].map(String::from))
        .collect();
    let tokens: Vec<&str> = tokens.iter().map(String::as_str).collect();
    let vocab = sorted_vocab(&tokens);
    let loaded = read(&round_trip(&vocab)).unwrap();
    for missing in [
        "internationalization",
        "internationalization0",
        "internationalization00005",
        "internationalization0499x",
        "internationalization0500",
        "interna",
        "internationalizatioo",
    ] {
        assert_eq!(loaded.get(missing), None, "{missing:?}");
    }
}

/// Every lookup agrees with a binary search over the sorted tokens, on
/// tokens with long shared prefixes and multibyte characters.
#[test]
fn agrees_with_a_sorted_list() {
    let mut rng = StdRng::seed_from_u64(3);
    let alphabet = ['a', 'b', 'c', 'é', '日'];
    let random_token = |rng: &mut StdRng| -> String {
        let len = rng.random_range(1..=14);
        (0..len)
            .map(|_| alphabet[rng.random_range(0..alphabet.len())])
            .collect()
    };
    let tokens: Vec<String> = (0..3000).map(|_| random_token(&mut rng)).collect();
    let tokens: Vec<&str> = tokens.iter().map(String::as_str).collect();
    let vocab = sorted_vocab(&tokens);
    let mut sorted: Vec<&String> = vocab.keys().collect();
    sorted.sort_unstable();
    let loaded = read(&round_trip(&vocab)).unwrap();
    for _ in 0..20_000 {
        let probe = random_token(&mut rng);
        let expected = sorted
            .binary_search(&&probe)
            .ok()
            .map(|rank| rank as TokenId);
        assert_eq!(loaded.get(&probe), expected, "{probe:?}");
    }
}

/// The lookup reads a shared prefix as the whole common prefix, so a file
/// that understates one is rejected rather than missing tokens later.
#[test]
fn rejects_an_understated_shared_prefix() {
    let mut body = Vec::new();
    for (shared, suffix) in [(0, &b"abc"[..]), (1, b"bd")] {
        write_varint(&mut body, shared);
        write_varint(&mut body, suffix.len() as u32);
        body.extend_from_slice(suffix);
    }
    assert!(CompactVocab::from_body(body, 2, 16).is_err());
}

#[test]
fn save_requires_ids_in_token_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vocab");
    let vocab: HashMap<String, TokenId> = [("a".to_string(), 1), ("b".to_string(), 0)].into();
    assert!(save(&common::universal_io::MmapFs, &path, &vocab).is_err());
    let vocab = sorted_vocab(&["a", "b"]);
    save(&common::universal_io::MmapFs, &path, &vocab).unwrap();
    assert_eq!(
        read(&fs_err::read(&path).unwrap()).unwrap().get("b"),
        Some(1)
    );
}

/// Front-coding and zstd together keep the file well below the raw tokens.
#[test]
fn compresses_below_the_raw_tokens() {
    let tokens: Vec<String> = (0..10_000).map(|i| format!("token{i:05}")).collect();
    let raw: usize = tokens.iter().map(String::len).sum();
    let tokens: Vec<&str> = tokens.iter().map(String::as_str).collect();
    let vocab = sorted_vocab(&tokens);
    let bytes = encode_vocab(&vocab);
    assert!(bytes.len() < raw / 4, "{} bytes for {raw} raw", bytes.len());
    let loaded = read(&bytes).unwrap();
    assert!(
        loaded.ram_usage_bytes() < raw / 2,
        "{} bytes in RAM",
        loaded.ram_usage_bytes()
    );
}

#[test]
fn rejects_what_it_did_not_write() {
    let vocab = sorted_vocab(&["alpha", "beta", "gamma", "delta"]);
    let bytes = round_trip(&vocab);

    for len in [0, 8, size_of::<Header>(), bytes.len() - 1] {
        assert!(read(&bytes[..len]).is_err(), "{len} bytes");
    }
    let mut padded = bytes.clone();
    padded.push(0);
    assert!(read(&padded).is_err());
    let mut versioned = bytes.clone();
    versioned[std::mem::offset_of!(Header, version)] = 2;
    assert!(read(&versioned).is_err());
    let mut recompressed = bytes.clone();
    recompressed[std::mem::offset_of!(Header, compression)] = 2;
    assert!(read(&recompressed).is_err());
    let mut recounted = bytes.clone();
    recounted[std::mem::offset_of!(Header, count)] = 3;
    assert!(read(&recounted).is_err());
    let mut reserved = bytes.clone();
    reserved[std::mem::offset_of!(Header, reserved)] = 0xFF;
    assert!(read(&reserved).is_ok());
    let mut corrupted = bytes;
    let last = corrupted.len() - 5;
    corrupted[last] ^= 1;
    assert!(read(&corrupted).is_err());
}
