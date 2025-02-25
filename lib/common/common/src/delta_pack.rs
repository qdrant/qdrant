use crate::bitpacking::{BitReader, BitWriter, packed_bits};

/// To simplify value counting, each value should be at least one byte.
/// Otherwise, the count would be ambiguous, e.g., a 2-byte slice of 5-bit
/// values could contain either 2 or 3 values.
const MIN_BITS_PER_VALUE: u8 = u8::BITS as u8;

/// How many bits required to store a value in range
/// `MIN_BITS_PER_VALUE..=u64::BITS`.
const HEADER_BITS: u8 = 6;

/// Pack sequence of u64 into a delta encoded byte array, and then bitpack it
///
/// Max length of the input: 2**32
/// Assume that the input is sorted
/// Output contains 4 bytes of the length of the input, followed by the packed data.
pub fn delta_pack(data: &[u64]) -> Vec<u8> {
    let mut deltas = Vec::with_capacity(data.len());
    let mut prev = 0;
    for &value in data {
        deltas.push(value - prev);
        prev = value;
    }

    compress_sequence(&deltas)
}

/// Pack sequence of u64 into a delta encoded byte array, and then bitpack it
///
/// Max length of the input: 2**32
/// DO NOT Assume that the input is sorted
/// Output contains 4 bytes of the length of the input, followed by the packed data.
pub fn compress_sequence(data: &[u64]) -> Vec<u8> {
    let length = data.len() as u32;
    let mut output = Vec::with_capacity(4);

    if length == 0 {
        return output;
    }

    let max_value = *data.iter().max().unwrap();
    let bits_per_value = packed_bits(max_value).max(MIN_BITS_PER_VALUE);

    let mut writer = BitWriter::new(&mut output);

    writer.write(bits_per_value - MIN_BITS_PER_VALUE, HEADER_BITS);

    for &value in data {
        writer.write(value, bits_per_value);
    }

    writer.finish();

    output
}

pub fn decompress_sequence(data: &[u8]) -> Vec<u64> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();

    let mut reader = BitReader::new(data);
    let mut remaining_bits = data.len() * u8::BITS as usize;

    reader.set_bits(HEADER_BITS);
    let bits_per_value = reader.read::<u8>() + MIN_BITS_PER_VALUE;
    remaining_bits -= HEADER_BITS as usize;

    reader.set_bits(bits_per_value);

    // It might be possible, that some bits are left after reading the header,
    // but it is always less than 1 byte and less than bits_per_value
    while remaining_bits >= bits_per_value as usize {
        result.push(reader.read::<u64>());
        remaining_bits -= bits_per_value as usize;
    }

    result
}

pub fn delta_unpack(data: &[u8]) -> Vec<u64> {
    let mut sequence = decompress_sequence(data);

    let mut prev = 0;
    for value in sequence.iter_mut() {
        *value += prev;
        prev = *value;
    }

    sequence
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn pack_and_unpack_sorted_data() {
        let data = vec![1, 2, 3, 4, 5];
        let packed = delta_pack(&data);
        let unpacked = delta_unpack(&packed);
        assert_eq!(data, unpacked);
    }

    #[test]
    fn pack_and_unpack_unsorted_data() {
        let data = vec![5, 3, 1, 4, 2];
        let packed = compress_sequence(&data);
        let unpacked = decompress_sequence(&packed);
        assert_eq!(data, unpacked);
    }

    #[test]
    fn pack_and_unpack_empty_data() {
        let data: Vec<u64> = Vec::new();
        let packed = delta_pack(&data);
        let unpacked = delta_unpack(&packed);
        assert_eq!(data, unpacked);
    }

    #[test]
    fn pack_and_unpack_single_element() {
        let data = vec![42];
        let packed = delta_pack(&data);
        let unpacked = delta_unpack(&packed);
        assert_eq!(data, unpacked);
    }

    #[test]
    fn pack_and_unpack_large_numbers() {
        let data = vec![u64::MAX - 2, u64::MAX - 1, u64::MAX, u64::MAX];
        let packed = delta_pack(&data);
        let unpacked = delta_unpack(&packed);
        assert_eq!(data, unpacked);
    }

    #[test]
    fn pack_and_unpack_large_numbers_unsorted() {
        let data = vec![u64::MAX - 2, u64::MAX, u64::MAX, u64::MAX - 1];
        let packed = compress_sequence(&data);
        let unpacked = decompress_sequence(&packed);
        assert_eq!(data, unpacked);
    }

    #[test]
    fn pack_and_unpack_random_data() {
        let num_iterations = 100;
        let max_length = 100;

        let mut rng = rand::rng();

        for _ in 0..num_iterations {
            let length = rng.random_range(0..max_length);
            let mut data = (0..length)
                .map(|_| rng.random_range(0..u64::MAX))
                .collect::<Vec<_>>();
            data.sort();
            let packed = delta_pack(&data);
            let unpacked = delta_unpack(&packed);
            assert_eq!(data, unpacked);
        }
    }
}
