const FLOAT_NAN: u8 = 0x00;
const FLOAT_NEG: u8 = 0x01;
const FLOAT_ZERO: u8 = 0x02;
const FLOAT_POS: u8 = 0x03;

const F64_KEY_LEN: usize = 13;
const I64_KEY_LEN: usize = 12;

/// Encode a f64 into `buf`
///
/// The encoded format for a f64 is :
///
/// **for positives:** the f64 bits ( in IEEE 754 format ) are re-interpreted as an int64 and
/// encoded using big-endian order.
///
/// **for negative** f64 : invert all the bits and encode it using bit-endian order.
///
/// A single-byte prefix tag is appended to the front of the encoding slice to ensures that
/// NaNs are always sorted first.
///
/// This approach was inspired by https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/float.go
///
///
/// #f64 encoding format
///
///```text
/// 0                    1                9
/// ┌───────────────────┬─────────────────┐
/// │     Float Type    │  NEG:  !key_val │
/// │  NAN/NEG/ZERO/POS |  POS:   key_val │
/// │    (big-endian)   │   (big-endian)  │
/// └───────────────────┴─────────────────┘
/// ```
///
pub fn encode_f64_ascending(val: f64, buf: &mut Vec<u8>) {
    if val.is_nan() {
        buf.push(FLOAT_NAN);
        buf.extend(&[0_u8; std::mem::size_of::<f64>()]);
        return;
    }

    if val == 0f64 {
        buf.push(FLOAT_ZERO);
        buf.extend(&[0_u8; std::mem::size_of::<f64>()]);
        return;
    }

    let f_as_u64 = val.to_bits();

    if f_as_u64 & (1 << 63) != 0 {
        let f = !f_as_u64;
        buf.push(FLOAT_NEG);
        buf.extend(f.to_be_bytes());
    } else {
        buf.push(FLOAT_POS);
        buf.extend(f_as_u64.to_be_bytes());
    }
}

/// Decode a f64 from a slice.
pub fn decode_f64_ascending(buf: &[u8]) -> f64 {
    match buf[0] {
        FLOAT_NAN => f64::NAN,
        FLOAT_NEG => {
            let u = u64::from_be_bytes(buf[1..9].try_into().expect("cannot decode f64"));
            let f = !u;
            f64::from_bits(f)
        }
        FLOAT_ZERO => 0f64,
        FLOAT_POS => {
            let u = u64::from_be_bytes(buf[1..9].try_into().expect("cannot decode f64"));
            f64::from_bits(u)
        }
        _ => panic!("invalid f64 prefix"),
    }
}

/// Encode a i64 into `buf` so that is sorts ascending.
pub fn encode_i64_ascending(val: i64, buf: &mut Vec<u8>) {
    let i = val ^ i64::MIN;
    buf.extend(i.to_be_bytes());
}

/// Decode a i64 from a slice
fn decode_i64_ascending(buf: &[u8]) -> i64 {
    let i = i64::from_be_bytes(buf[0..8].try_into().expect("cannot decode i64"));
    i ^ i64::MIN
}

/// Encodes a f64 key so that it sort in ascending order.
///
/// The key is compound by the numeric value of the key plus a u32 representing
/// the payload offset within the payload store.
///
/// # float key encoding format
///
///```text
///
/// 0                    1                9             13
/// ┌───────────────────┬─────────────────┬──────────────┐
/// │     Float Type    │  NEG:  !key_val │              │
/// │  NAN/NEG/ZERO/POS |  POS:   key_val │ point_offset │
/// │    (big-endian)   │   (big-endian)  │              │
/// └───────────────────┴─────────────────┴──────────────┘
/// ```
///
pub fn encode_f64_key_ascending(key_val: f64, point_offset: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(F64_KEY_LEN);
    encode_f64_ascending(key_val, &mut buf);
    buf.extend(point_offset.to_be_bytes());
    buf
}

pub fn decode_f64_key_ascending(buf: &[u8]) -> (u32, f64) {
    (
        u32::from_be_bytes(
            (&buf[F64_KEY_LEN - std::mem::size_of::<u32>()..])
                .try_into()
                .unwrap(),
        ),
        decode_f64_ascending(buf),
    )
}

/// Encodes a i64 key so that it sort in ascending order.
///
/// The key is compound by the numeric value of the key plus a u32 representing
/// the payload offset within the payload store.
///
/// # int key encoding format
///
///```text
///
/// 0                    8             12
/// ┌────────────────────┬──────────────┐
/// │ key_val ^ i64::MIN │ point_offset │
/// │    (big-endian)    │ (big-endian) │
/// └────────────────────┴──────────────┘
///```
pub fn encode_i64_key_ascending(key_val: i64, point_offset: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(I64_KEY_LEN);
    encode_i64_ascending(key_val, &mut buf);
    buf.extend(point_offset.to_be_bytes());
    buf
}

pub fn decode_i64_key_ascending(buf: &[u8]) -> (u32, i64) {
    (
        u32::from_be_bytes(
            (&buf[I64_KEY_LEN - std::mem::size_of::<u32>()..])
                .try_into()
                .unwrap(),
        ),
        decode_i64_ascending(buf),
    )
}

#[cfg(test)]
mod tests {
    use crate::index::key_encoding::{
        decode_f64_ascending, decode_i64_ascending, encode_f64_ascending, encode_i64_ascending,
    };
    use std::cmp::Ordering;

    #[test]
    fn test_encode_f64() {
        test_f64_encoding_roundtrip(0.42342);
        test_f64_encoding_roundtrip(0f64);
        test_f64_encoding_roundtrip(f64::NAN);
        test_f64_encoding_roundtrip(-0.423423983);
    }

    #[test]
    fn test_encode_i64() {
        test_i64_encoding_roundtrip(i64::MIN);
        test_i64_encoding_roundtrip(i64::MAX);
        test_i64_encoding_roundtrip(0);
        test_i64_encoding_roundtrip(41262);
        test_i64_encoding_roundtrip(-98793);
    }

    #[test]
    fn test_f64_lex_order() {
        let mut nan_buf = Vec::new();
        let mut zero_buf = Vec::new();
        let mut pos_buf = Vec::new();
        let mut neg_buf = Vec::new();

        encode_f64_ascending(f64::NAN, &mut nan_buf);
        encode_f64_ascending(0f64, &mut zero_buf);
        encode_f64_ascending(0.2435224412, &mut pos_buf);
        encode_f64_ascending(-0.82976347, &mut neg_buf);

        assert_eq!(nan_buf.cmp(&neg_buf), Ordering::Less);
        assert_eq!(neg_buf.cmp(&zero_buf), Ordering::Less);
        assert_eq!(zero_buf.cmp(&pos_buf), Ordering::Less);
    }

    #[test]
    fn test_i64_lex_order() {
        let mut zero_buf = Vec::new();
        let mut pos_buf = Vec::new();
        let mut neg_buf = Vec::new();

        encode_i64_ascending(0, &mut zero_buf);
        encode_i64_ascending(123, &mut pos_buf);
        encode_i64_ascending(-4324, &mut neg_buf);

        assert_eq!(neg_buf.cmp(&zero_buf), Ordering::Less);
        assert_eq!(zero_buf.cmp(&pos_buf), Ordering::Less);
    }

    fn test_f64_encoding_roundtrip(val: f64) {
        let mut buf = Vec::new();
        encode_f64_ascending(val, &mut buf);
        let dec_val = decode_f64_ascending(buf.as_slice());
        if val.is_nan() {
            assert!(dec_val.is_nan());
            return;
        }
        assert_eq!(val, dec_val);
    }

    fn test_i64_encoding_roundtrip(val: i64) {
        let mut buf = Vec::new();
        encode_i64_ascending(val, &mut buf);
        let res = decode_i64_ascending(buf.as_slice());
        assert_eq!(val, res);
    }
}
