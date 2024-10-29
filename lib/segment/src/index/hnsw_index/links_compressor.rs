pub struct DecompressIterator<T: Iterator<Item = u8>> {
    data: T,
    delta: u64,
}

impl<T: Iterator<Item = u8>> DecompressIterator<T> {
    pub fn new(data: T) -> Self {
        DecompressIterator { data, delta: 0 }
    }
}

impl<T: Iterator<Item = u8>> Iterator for DecompressIterator<T> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let mut link: u64 = 0;
        let mut shift = 0;
        while let Some(byte) = self.data.next() {
            let byte = byte as u64;
            link |= (byte & 0x7F) << shift;
            shift += 7;
            if byte < 0x80 {
                let result = link + self.delta;
                self.delta = result;
                return Some(result);
            }
        }
        None
    }
}

pub fn estimate_compressed_size(data: &[u64]) -> usize {
    let mut delta = 0;
    data.iter()
        .map(|&orig| {
            if orig < delta {
                panic!("Links are not sorted");
            }

            let mut v = orig - delta;
            delta = orig;

            let mut bytes_count = 0;
            loop {
                bytes_count += 1;
                v >>= 7;
                if v == 0 {
                    break;
                }
            }
            bytes_count
        })
        .sum()
}

pub fn compress(data: &[u64], dst: &mut Vec<u8>) {
    let mut delta = 0;
    for &orig in data {
        if orig < delta {
            panic!("Links are not sorted");
        }

        let mut v = orig - delta;
        delta = orig;

        loop {
            let byte = (v & 0x7F) as u8;
            if v > 0x7F {
                dst.push(0x80 | byte);
            } else {
                dst.push(byte);
            }
            v >>= 7;
            if v == 0 {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression() {
        let data = vec![0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        //let data = vec![500, 1_000, 9_000, 1_000_000, 6_000_000, 7_000_000_000, 7_000_000_001, 7_000_000_001];
        let mut compressed = Vec::new();
        compress(&data, &mut compressed);
        let decompressed = DecompressIterator::new(compressed.iter().copied());
        let decompressed_data: Vec<_> = decompressed.collect();
        assert_eq!(data, decompressed_data);
    }

    #[test]
    fn test_estimate_compressed_size() {
        let data = vec![0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let compressed_size = estimate_compressed_size(&data);
        let mut compressed = Vec::new();
        compress(&data, &mut compressed);
        assert_eq!(compressed_size, compressed.len());
    }
}
