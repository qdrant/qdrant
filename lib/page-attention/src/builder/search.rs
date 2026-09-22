#[inline(always)]
pub fn bf16_to_f32(bits: u16) -> f32 {
    f32::from_bits((bits as u32) << 16)
}

#[inline(always)]
pub fn f32_to_bf16(x: f32) -> u16 {
    let bits = x.to_bits();
    if (bits & 0x7fff_ffff) > 0x7f80_0000 {
        // NaN: keep it a NaN
        return ((bits >> 16) as u16) | 0x0040;
    }
    let rounding = 0x7fff + ((bits >> 16) & 1);
    ((bits.wrapping_add(rounding)) >> 16) as u16
}

pub fn dot_bf16_bytes(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert!(a.len().is_multiple_of(2));
    const LANES: usize = 8;
    let n = a.len() / 2;
    let chunks = n / LANES;
    let mut acc = [0.0f32; LANES];
    for c in 0..chunks {
        let off = c * LANES * 2;
        let (av, bv) = (&a[off..off + LANES * 2], &b[off..off + LANES * 2]);
        for l in 0..LANES {
            let x = bf16_to_f32(u16::from_le_bytes([av[2 * l], av[2 * l + 1]]));
            let y = bf16_to_f32(u16::from_le_bytes([bv[2 * l], bv[2 * l + 1]]));
            acc[l] += x * y;
        }
    }
    let mut sum = ((acc[0] + acc[1]) + (acc[2] + acc[3])) + ((acc[4] + acc[5]) + (acc[6] + acc[7]));
    for i in chunks * LANES..n {
        let x = bf16_to_f32(u16::from_le_bytes([a[2 * i], a[2 * i + 1]]));
        let y = bf16_to_f32(u16::from_le_bytes([b[2 * i], b[2 * i + 1]]));
        sum += x * y;
    }
    sum
}
