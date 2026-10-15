//! Document lengths as stored: a 16-bit minifloat, 4-bit exponent over a
//! 12-bit fraction, with an implicit leading one above the exact range.
//!
//! ```text
//! e == 0:  value = f                              0 ..= 4095
//! e >= 1:  value = (0x1000 | f) << (e - 1)        4096 ..= 134,201,344
//! ```
//!
//! Exact to 8,191 tokens, rounded down by less than 2^-12 above, saturating at
//! 2^27. BM25 reads `|d|` only as `|d| / avgdl`, so the relative error is the
//! one that matters. Codes order like their values, and 0 stays 0.
//!
//! Every backend holds `round_doc_len(len)`, in its slots, its total and what
//! `doc_len_batch` answers, so the same document scores the same wherever it
//! lives and a total never drifts from the lengths it is the sum of.

const FRACTION_BITS: u32 = 12;
const FRACTION_MASK: u32 = (1 << FRACTION_BITS) - 1;
const MAX_EXPONENT: u32 = 15;

pub fn encode_doc_len(len: u32) -> u16 {
    if len < 1 << FRACTION_BITS {
        return len as u16;
    }
    let exponent = u32::BITS - len.leading_zeros() - FRACTION_BITS;
    if exponent > MAX_EXPONENT {
        return u16::MAX;
    }
    (exponent << FRACTION_BITS | (len >> (exponent - 1)) & FRACTION_MASK) as u16
}

pub fn decode_doc_len(code: u16) -> u32 {
    let code = u32::from(code);
    let (exponent, fraction) = (code >> FRACTION_BITS, code & FRACTION_MASK);
    if exponent == 0 {
        fraction
    } else {
        (fraction | 1 << FRACTION_BITS) << (exponent - 1)
    }
}

/// The length an index holds for a document of `len` tokens.
pub fn round_doc_len(len: u32) -> u32 {
    decode_doc_len(encode_doc_len(len))
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXACT_UP_TO: u32 = 8_191;
    const MAX_LEN: u32 = 134_201_344;

    #[test]
    fn every_code_round_trips_in_order() {
        let mut previous = None;
        for code in 0..=u16::MAX {
            let len = decode_doc_len(code);
            assert_eq!(encode_doc_len(len), code, "code {code} decodes to {len}");
            if let Some(previous) = previous {
                assert!(len > previous, "code {code} does not increase");
            }
            previous = Some(len);
        }
        assert_eq!(decode_doc_len(u16::MAX), MAX_LEN);
    }

    #[test]
    fn exact_up_to_8191() {
        for len in 0..=EXACT_UP_TO {
            assert_eq!(round_doc_len(len), len);
        }
        // 8,192 is a power of two, so it still has a code of its own.
        assert_eq!(round_doc_len(EXACT_UP_TO + 1), EXACT_UP_TO + 1);
        assert_eq!(round_doc_len(EXACT_UP_TO + 2), EXACT_UP_TO + 1);
    }

    #[test]
    fn rounds_down_within_the_error_bound() {
        let bound = 1.0 / f64::from(1 << FRACTION_BITS);
        let mut len = EXACT_UP_TO + 1;
        while len <= MAX_LEN {
            let rounded = round_doc_len(len);
            assert!(rounded <= len, "{len} rounds up to {rounded}");
            let error = f64::from(len - rounded) / f64::from(len);
            assert!(error < bound, "{len} rounds to {rounded}, error {error}");
            len += len / 997 + 1;
        }
    }

    #[test]
    fn saturates_past_the_largest_code() {
        for len in [MAX_LEN, MAX_LEN + 1, 1 << 27, u32::MAX] {
            assert_eq!(encode_doc_len(len), u16::MAX, "{len}");
        }
    }
}
