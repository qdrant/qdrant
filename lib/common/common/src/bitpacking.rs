type Buf = u64;

pub struct BitPacker<'a> {
    output: &'a mut Vec<u8>,
    buf: Buf,
    buf_bits: u8,
}

impl<'a> BitPacker<'a> {
    #[inline]
    pub fn new(output: &'a mut Vec<u8>) -> Self {
        output.clear();
        Self {
            output,
            buf: 0,
            buf_bits: 0,
        }
    }
    #[inline]
    pub fn push(&mut self, value: u32, bits: u8) {
        if self.buf_bits + bits >= Buf::BITS as u8 {
            self.buf |= Buf::from(value) << self.buf_bits;
            self.output.extend_from_slice(&self.buf.to_le_bytes());
            self.buf = Buf::from(value) >> (Buf::BITS as u8 - self.buf_bits);
            self.buf_bits = self.buf_bits + bits - Buf::BITS as u8;
        } else {
            self.buf |= Buf::from(value) << self.buf_bits;
            self.buf_bits += bits;
        }
    }

    #[inline]
    pub fn finish(self) {
        self.output.extend_from_slice(
            &self.buf.to_le_bytes()[..(self.buf_bits as usize).div_ceil(size_of::<Buf>())],
        );
    }
}

pub struct BitUnpacker<'a> {
    input: &'a [u8],
    buf: Buf,
    buf_bits: u8,
    bits: u8,
    mask: Buf,
}

impl<'a> BitUnpacker<'a> {
    #[inline]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            buf: 0,
            buf_bits: 0,
            bits: 0,
            mask: 0,
        }
    }

    #[inline]
    pub fn set_bits(&mut self, bits: u8) {
        self.bits = bits;
        self.mask = (1u64 << bits) - 1;
    }

    #[inline]
    pub fn read(&mut self) -> u32 {
        if self.buf_bits < self.bits {
            let mut new_bits;
            if self.input.len() >= size_of::<Buf>() {
                new_bits = Buf::from_le_bytes(self.input[0..size_of::<Buf>()].try_into().unwrap());
                self.input = &self.input[size_of::<Buf>()..];
            } else {
                new_bits = 0;
                for (i, byte) in self.input.iter().copied().enumerate() {
                    new_bits |= Buf::from(byte) << (i * size_of::<Buf>());
                }
            }
            let val = ((self.buf | (new_bits << self.buf_bits)) & self.mask) as u32;
            self.buf = new_bits >> (self.bits - self.buf_bits);
            self.buf_bits += Buf::BITS as u8 - self.bits;
            val
        } else {
            self.buf_bits -= self.bits;
            let val = (self.buf & self.mask) as u32;
            self.buf >>= self.bits;
            val
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::*;

    #[test]
    fn test_pack_unpack() {
        let mut rng = StdRng::seed_from_u64(42);

        let mut bits_per_value = Vec::new();
        let mut values = Vec::new();
        let mut packed = Vec::new();
        let mut unpacked = Vec::new();
        for len in 0..40 {
            for _ in 0..100 {
                values.clear();
                bits_per_value.clear();
                let mut total_bits = 0;
                for _ in 0..len {
                    let bits = rng.gen_range(0u8..=32u8);
                    values.push(rng.gen_range(0..(1u64 << bits)) as u32);
                    bits_per_value.push(bits);
                    total_bits += u64::from(bits);
                }

                let mut packer = BitPacker::new(&mut packed);
                for (&x, &bits) in zip(&values, &bits_per_value) {
                    packer.push(x, bits);
                }
                packer.finish();

                assert_eq!(packed.len(), total_bits.next_multiple_of(8) as usize / 8);

                unpacked.clear();
                let mut unpacker = BitUnpacker::new(&packed);
                for &bits in &bits_per_value {
                    unpacker.set_bits(bits);
                    unpacked.push(unpacker.read());
                }

                assert_eq!(values, unpacked);
            }
        }
    }
}
