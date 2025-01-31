use num_traits::{AsPrimitive, ConstOne, PrimInt, Unsigned};

use crate::num_traits::ConstBits;

/// The internal buffer type for [`BitWriter`] and [`BitReader`].
/// Instead of writing/reading a single byte at a time, they write/read
/// `size_of::<Buf>()` bytes at once, for a better performance.
/// This is an implementation detail and shouldn't affect the data layout.
/// Any unsigned numeric type larger than `u32` should work.
type Buf = u64;

/// Writes bits to the `u8` vector.
/// It's like [`std::io::Write`], but for bits rather than bytes.
pub struct BitWriter<'a> {
    output: &'a mut Vec<u8>,
    buf: Buf,
    buf_bits: u8,
}

impl<'a> BitWriter<'a> {
    /// Create a new writer that appends bits to the `output`.
    #[inline]
    pub fn new(output: &'a mut Vec<u8>) -> Self {
        Self {
            output,
            buf: 0,
            buf_bits: 0,
        }
    }

    /// Write a `value` of `bits` bits to the output.
    ///
    /// The `bits` must be less than or equal to 32, and the `value` must fit in
    /// the `bits` bits.
    #[inline]
    pub fn write<T: ConstBits + Into<Buf>>(&mut self, value: T, bits: u8) {
        let value = value.into();

        #[cfg(test)]
        debug_assert!(u32::from(bits) <= T::BITS && packed_bits(value) <= bits);

        self.buf |= value << self.buf_bits;
        self.buf_bits += bits;
        if self.buf_bits >= Buf::BITS as u8 {
            // ┌──value───┐┌───initial self.buf────┐
            // rrrrrvvvvvvvbbbbbbbbbbbbbbbbbbbbbbbbb
            // └[2]┘└─────────────[1]──────────────┘
            self.output.extend_from_slice(&self.buf.to_le_bytes()); // [1]
            self.buf_bits -= Buf::BITS as u8;
            if bits - self.buf_bits == Buf::BITS as u8 {
                self.buf = 0;
            } else {
                self.buf = value >> (bits - self.buf_bits); // [2]
            }
        }
    }

    /// Write the remaining bufferized bits to the output.
    #[inline]
    pub fn finish(self) {
        self.output.extend_from_slice(
            &self.buf.to_le_bytes()[..(self.buf_bits as usize).div_ceil(u8::BITS as usize)],
        );
    }
}

/// Reads bits from `u8` slice.
/// It's like [`std::io::Read`], but for bits rather than bytes.
pub struct BitReader<'a> {
    input: &'a [u8],
    buf: Buf,
    buf_bits: u8,
    mask: Buf,
    bits: u8,
}

impl<'a> BitReader<'a> {
    #[inline]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            buf: 0,
            buf_bits: 0,
            mask: 0,
            bits: 0,
        }
    }

    /// Configure the reader to read `bits` bits at a time. This affects
    /// subsequent calls to [`read()`].
    ///
    /// The `bits` must be less than or equal to 32.
    ///
    /// Note: it's a separate method and not a parameter of [`read()`] to
    /// optimize reading a group of values with the same bit size.
    ///
    /// [`read()`]: Self::read
    #[inline]
    pub fn set_bits(&mut self, bits: u8) {
        #[cfg(test)]
        debug_assert!(u32::from(bits) <= Buf::BITS);

        self.bits = bits;
        self.mask = make_bitmask(bits);
    }

    /// Returns the number of bits set with [`set_bits()`].
    ///
    /// [`set_bits()`]: Self::set_bits
    #[inline]
    pub fn bits(&self) -> u8 {
        self.bits
    }

    /// Read next `bits` bits from the input. The amount of bits must be set
    /// with [`set_bits()`] before calling this method.
    ///
    /// If read beyond the end of the input, the result would be an unspecified
    /// garbage.
    ///
    /// [`set_bits()`]: Self::set_bits
    #[inline]
    pub fn read<T>(&mut self) -> T
    where
        T: 'static + Copy,
        Buf: AsPrimitive<T>,
    {
        if self.buf_bits >= self.bits {
            self.buf_bits -= self.bits;
            let val = (self.buf & self.mask).as_();
            self.buf >>= self.bits;
            val
        } else {
            // Consider a naive approach:
            //
            //     let new_buf = read_buf_and_advance(&mut self.input);
            //     self.buf |= new_buf << self.buf_bits; // *overflow*
            //     self.buf_bits += size_of_val(&new_buf) * u8::BITS;
            //     ... then proceed as usual ...
            //
            // For performance reasons, we want `new_buf` and `self.buf` to be
            // both 64-bit. But when they are the same, the naive approach would
            // overflow in the commented line. So, the following code is a trick
            // to let us use the same type for both.
            //
            // ┌───────────new_buf────────────┐┌─self.buf─┐
            // rrrrrrrrrrrrrrrrrrrrrrrrrvvvvvvvbbbbbbbbbbbb
            // └──────────[3]──────────┘├─[2]─┘└───[1]────┤
            //                          └───────val───────┘
            let new_buf = read_buf_and_advance(&mut self.input);
            let val = ((/*[1]*/self.buf) | (/*[2]*/new_buf << self.buf_bits) & self.mask).as_();
            self.buf_bits += Buf::BITS as u8 - self.bits;
            if self.buf_bits == 0 {
                self.buf = 0;
            } else {
                self.buf = /*[3]*/ new_buf >> (Buf::BITS as u8 - self.buf_bits);
            }
            val
        }
    }
}

/// Read a single [`Buf`] from the `input` and advance (or not) the `input`.
#[inline]
fn read_buf_and_advance(input: &mut &[u8]) -> Buf {
    let mut buf = 0;
    if input.len() >= size_of::<Buf>() {
        // This line translates to a single unaligned pointer read.
        buf = Buf::from_le_bytes(input[0..size_of::<Buf>()].try_into().unwrap());
        // This line translates to a single pointer advance.
        *input = &input[size_of::<Buf>()..];
    } else {
        // We could remove this branch by explicitly using unsafe pointer
        // operations in the branch above, but we are playing it safe here.
        for (i, byte) in input.iter().copied().enumerate() {
            buf |= Buf::from(byte) << (i * u8::BITS as usize);
        }

        // The following line is commented out for performance reasons as this
        // should be the last read. If the caller will try to read input again
        // anyway, it will get the same values again (aka "unspecified garbage"
        // as stated in the documentation).
        // *input = &[]; // Not needed, see the comment above.
    }
    buf
}

/// Minimum amount of bits required to store a value in the range
/// `0..=max_value`.
pub fn packed_bits<T: ConstBits + PrimInt + Unsigned>(max_value: T) -> u8 {
    (T::BITS - max_value.leading_zeros()) as u8
}

pub fn make_bitmask<T: ConstBits + ConstOne + PrimInt + Unsigned>(bits: u8) -> T {
    if u32::from(bits) >= T::BITS {
        T::max_value()
    } else {
        (T::ONE << usize::from(bits)) - T::ONE
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::iter::zip;

    use num_traits::{ConstOne, ConstZero, PrimInt, Unsigned};
    use rand::distr::uniform::SampleUniform;
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::*;

    #[test]
    fn test_simple() {
        let mut packed = Vec::new();
        let mut w = BitWriter::new(&mut packed);

        w.write::<u32>(0b01010, 5);
        w.write::<u32>(0b10110, 5);
        w.write::<u32>(0b10100, 5);
        w.write::<u32>(0b010110010, 9);
        w.write::<u32>(0b101100001, 9);
        w.write::<u32>(0b001001101, 9);
        w.write::<u32>(0x12345678, 32);
        w.finish();
        assert_eq!(packed.len(), 10);

        let mut r = BitReader::new(&packed);
        r.set_bits(5);
        assert_eq!(r.read::<u32>(), 0b01010);
        assert_eq!(r.read::<u32>(), 0b10110);
        assert_eq!(r.read::<u32>(), 0b10100);
        r.set_bits(9);
        assert_eq!(r.read::<u32>(), 0b010110010);
        assert_eq!(r.read::<u32>(), 0b101100001);
        assert_eq!(r.read::<u32>(), 0b001001101);
        r.set_bits(32);
        assert_eq!(r.read::<u32>(), 0x12345678);
    }

    #[test]
    fn test_random() {
        test_random_impl::<u8>();
        test_random_impl::<u16>();
        test_random_impl::<u32>();
        test_random_impl::<u64>();
    }

    fn test_random_impl<T>()
    where
        Buf: AsPrimitive<T>,
        T: ConstBits
            + ConstOne
            + ConstZero
            + Copy
            + Debug
            + Into<Buf>
            + PrimInt
            + SampleUniform
            + Unsigned
            + 'static,
    {
        let mut rng = StdRng::seed_from_u64(42);

        let mut bits_per_value = Vec::new();
        let mut values = Vec::<T>::new();
        let mut packed = Vec::new();
        let mut unpacked = Vec::<T>::new();
        for len in 0..40 {
            for _ in 0..100 {
                values.clear();
                bits_per_value.clear();
                let mut total_bits = 0;
                for _ in 0..len {
                    let bits = rng.random_range(0u8..=T::BITS as u8);
                    values.push(rng.random_range(T::ZERO..=make_bitmask(bits)));
                    bits_per_value.push(bits);
                    total_bits += u64::from(bits);
                }

                packed.clear();
                let mut w = BitWriter::new(&mut packed);
                for (&x, &bits) in zip(&values, &bits_per_value) {
                    w.write(x, bits);
                }
                w.finish();

                assert_eq!(packed.len(), total_bits.next_multiple_of(8) as usize / 8);

                unpacked.clear();
                let mut r = BitReader::new(&packed);
                for &bits in &bits_per_value {
                    r.set_bits(bits);
                    unpacked.push(r.read());
                }

                assert_eq!(values, unpacked);
            }
        }
    }

    #[test]
    fn test_packed_bits_simple() {
        assert_eq!(packed_bits(0_u32), 0);

        assert_eq!(packed_bits(1_u32), 1);

        assert_eq!(packed_bits(2_u32), 2);
        assert_eq!(packed_bits(3_u32), 2);

        assert_eq!(packed_bits(4_u32), 3);
        assert_eq!(packed_bits(7_u32), 3);

        assert_eq!(packed_bits(0x_7FFF_FFFF_u32), 31);

        assert_eq!(packed_bits(0x_8000_0000_u32), 32);
        assert_eq!(packed_bits(0x_FFFF_FFFF_u32), 32);
    }

    #[test]
    fn test_packed_bits_extensive() {
        fn check<T: Unsigned + PrimInt + ConstBits + TryFrom<u128>>(v: u128, expected_bits: u8) {
            if let Ok(x) = v.try_into() {
                assert_eq!(packed_bits::<T>(x), expected_bits);
            }
        }

        for expected_bits in 0..=128_u8 {
            let (min, max);
            if expected_bits == 0 {
                (min, max) = (0, 0);
            } else {
                min = 1_u128 << (expected_bits - 1);
                max = (min - 1) * 2 + 1;
            }

            check::<u8>(min, expected_bits);
            check::<u16>(min, expected_bits);
            check::<u32>(min, expected_bits);
            check::<u64>(min, expected_bits);
            check::<u128>(min, expected_bits);
            check::<usize>(min, expected_bits);

            check::<u8>(max, expected_bits);
            check::<u16>(max, expected_bits);
            check::<u32>(max, expected_bits);
            check::<u64>(max, expected_bits);
            check::<u128>(max, expected_bits);
            check::<usize>(max, expected_bits);
        }
    }
}
