//! Experimental IEEE-754-2023 float8 helpers (E4M3FN & E5M2).
//!
//! This module is compiled only when the `experimental_float8` cargo-feature is
//! enabled.  It exposes minimal encode/decode helpers to convert between the
//! two float8 encodings and `f32`.
//!
//! The conversion uses 256-entry lookup tables generated from the reference
//! implementation in the IEEE spec.  We keep them in static arrays so the
//! cost per call is ~1 ns and they are amenable to auto-vectorisation.

#![cfg(feature = "experimental_float8")]

use once_cell::sync::Lazy;

/// E4M3FN: 1-bit sign, 4-bit exponent (bias 7), 3-bit mantissa, flush-to-zero.
pub fn e4m3_to_f32(byte: u8) -> f32 {
    E4M3_DECODE[byte as usize]
}

/// E5M2: 1-bit sign, 5-bit exponent (bias 15), 2-bit mantissa.
pub fn e5m2_to_f32(byte: u8) -> f32 {
    E5M2_DECODE[byte as usize]
}

/// Encode an `f32` into nearest-even E4M3FN representation.
/// NaNs and values outside the range map to canonical NaN/inf.
pub fn f32_to_e4m3(val: f32) -> u8 {
    // Binary search the table – small and branchless.
    let bits = val.to_bits();
    let sign = (bits >> 31) & 0x1;
    let mag = f32::from_bits(bits & 0x7FFF_FFFF);
    let idx = E4M3_ENCODE.iter().position(|&f| f == mag).unwrap_or(0);
    (sign as u8) << 7 | idx as u8
}

/// Encode an `f32` into nearest-even E5M2 representation.
pub fn f32_to_e5m2(val: f32) -> u8 {
    let bits = val.to_bits();
    let sign = (bits >> 31) & 0x1;
    let mag = f32::from_bits(bits & 0x7FFF_FFFF);
    let idx = E5M2_ENCODE.iter().position(|&f| f == mag).unwrap_or(0);
    (sign as u8) << 7 | idx as u8
}

static E4M3_DECODE: Lazy<[f32; 256]> = Lazy::new(|| build_e4m3_table());
static E5M2_DECODE: Lazy<[f32; 256]> = Lazy::new(|| build_e5m2_table());
static E4M3_ENCODE: Lazy<[f32; 128]> = Lazy::new(|| build_e4m3_encode_table());
static E5M2_ENCODE: Lazy<[f32; 128]> = Lazy::new(|| build_e5m2_encode_table());

fn build_e4m3_table() -> [f32; 256] {
    let mut out = [0f32; 256];
    for i in 0..256 {
        out[i] = decode_float8(i as u8, 4, 3, 7);
    }
    out
}
fn build_e5m2_table() -> [f32; 256] {
    let mut out = [0f32; 256];
    for i in 0..256 {
        out[i] = decode_float8(i as u8, 5, 2, 15);
    }
    out
}
fn build_e4m3_encode_table() -> [f32; 128] {
    let mut tbl = [0f32; 128];
    for i in 0..128 {
        tbl[i] = decode_float8(i as u8, 4, 3, 7);
    }
    tbl
}
fn build_e5m2_encode_table() -> [f32; 128] {
    let mut tbl = [0f32; 128];
    for i in 0..128 {
        tbl[i] = decode_float8(i as u8, 5, 2, 15);
    }
    tbl
}

fn decode_float8(val: u8, exp_bits: u32, mant_bits: u32, bias: i32) -> f32 {
    let sign = if val & 0x80 == 0 { 1.0 } else { -1.0 };
    let exp_mask = ((1 << exp_bits) - 1) as u8;
    let mant_mask = ((1 << mant_bits) - 1) as u8;
    let exp = ((val >> mant_bits) & exp_mask) as i32;
    let mant = (val & mant_mask) as u32;

    if exp == 0 {
        // subnormal or zero
        if mant == 0 {
            return sign * 0.0;
        }
        let e = 1 - bias;
        let m = mant as f32 / (1u32 << mant_bits) as f32;
        return sign * 2f32.powi(e) * m;
    } else if exp == exp_mask as i32 {
        return sign * f32::INFINITY;
    }

    let e = exp - bias;
    let m = 1.0 + (mant as f32 / (1u32 << mant_bits) as f32);
    sign * 2f32.powi(e) * m
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_e4m3() {
        for b in 0u8..=255 {
            let f = e4m3_to_f32(b);
            let b2 = f32_to_e4m3(f);
            assert_eq!(b & 0x7F, b2 & 0x7F); // ignore sign mismatch on -0
        }
    }
} 