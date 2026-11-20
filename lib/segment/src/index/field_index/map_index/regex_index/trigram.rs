//! Trigrams of keyword keys: the unit the regex index is built from.
//!
//! A key contributes the trigrams of its *folded* form wrapped in
//! [`BOUNDARY`] markers, e.g. `"Ab"` → `\xFFab\xFF` → `{\xFFab, ab\xFF}`.
//! Folding makes one index serve both case-sensitive and case-insensitive
//! patterns (the regex itself decides case on verification); the markers make
//! anchored patterns selective even with a short literal (`^ab`, `ab$`).

/// Marks the start and the end of a key. `0xFF` never occurs in UTF-8, so it
/// cannot collide with key content.
pub const BOUNDARY: u8 = 0xFF;

/// Three consecutive bytes, packed big-endian into the low 24 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Trigram(u32);

impl Trigram {
    pub fn new(bytes: [u8; 3]) -> Self {
        let [a, b, c] = bytes;
        Self(u32::from_be_bytes([0, a, b, c]))
    }

    /// Restore a trigram from [`Self::to_u32`].
    pub fn from_u32(value: u32) -> Self {
        debug_assert!(value >> 24 == 0, "trigram must fit into 24 bits");
        Self(value)
    }

    pub fn to_u32(self) -> u32 {
        self.0
    }
}

/// Fold `text` for trigram extraction: replace every char by its lowercase
/// mapping.
///
/// Folding is char by char, without the context-dependent rules of
/// [`str::to_lowercase`] (final sigma), so that the folding of a piece of a
/// string is the corresponding piece of the folded string. Query literals
/// are folded the same way, which is what makes the query sound.
pub fn fold(text: &str) -> String {
    text.chars().flat_map(char::to_lowercase).collect()
}

/// Trigrams of every 3-byte window of `bytes`, in order, with repetitions.
pub(super) fn windows(bytes: &[u8]) -> impl Iterator<Item = Trigram> + '_ {
    bytes
        .windows(3)
        .map(|window| Trigram::new([window[0], window[1], window[2]]))
}

/// Distinct trigrams of `key`, sorted.
pub fn key_trigrams(key: &str) -> Vec<Trigram> {
    let folded = fold(key);
    let mut padded = Vec::with_capacity(folded.len() + 2);
    padded.push(BOUNDARY);
    padded.extend_from_slice(folded.as_bytes());
    padded.push(BOUNDARY);

    let mut trigrams: Vec<Trigram> = windows(&padded).collect();
    trigrams.sort_unstable();
    trigrams.dedup();
    trigrams
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tri(bytes: &[u8]) -> Trigram {
        Trigram::new(bytes.try_into().unwrap())
    }

    #[test]
    fn key_trigrams_are_folded_and_wrapped() {
        assert_eq!(
            key_trigrams("Abc"),
            vec![tri(b"abc"), tri(b"bc\xFF"), tri(b"\xFFab")],
        );
        assert_eq!(key_trigrams("a"), vec![tri(b"\xFFa\xFF")]);
        assert!(key_trigrams("").is_empty());
        // Repeated windows collapse.
        assert_eq!(
            key_trigrams("aaaa"),
            vec![tri(b"aaa"), tri(b"aa\xFF"), tri(b"\xFFaa")],
        );
    }

    #[test]
    fn fold_is_context_free() {
        // `str::to_lowercase` would turn the final sigma into `ς`.
        assert_eq!(fold("ΟΔΟΣ"), "οδοσ");
        assert_eq!(fold("ΟΔ") + &fold("ΟΣ"), fold("ΟΔΟΣ"));
        // Kelvin sign folds to ASCII `k`, dotted capital I to two chars.
        assert_eq!(fold("\u{212A}"), "k");
        assert_eq!(fold("İ"), "i\u{307}");
    }

    #[test]
    fn trigram_u32_round_trip() {
        let trigram = tri(b"\xFFab");
        assert_eq!(Trigram::from_u32(trigram.to_u32()), trigram);
        assert_eq!(trigram.to_u32(), 0x00FF_6162);
    }
}
