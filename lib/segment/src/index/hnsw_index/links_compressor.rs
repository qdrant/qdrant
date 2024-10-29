use crate::common::operation_error::{OperationError, OperationResult};

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
        #[allow(for_loops_over_fallibles)]
        while let Some(byte) = self.data.next() {
            let byte = byte as u64;
            link |= (link << 7) | (byte & 127);
            if byte < 128 {
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
        .map(|&v| {
            if v < delta {
                panic!("Links are not sorted");
            }

            let mut v = v - delta;
            delta = v;
            let mut bytes_count = 0;
            while v > 0 {
                bytes_count += 1;
                v >>= 7;
            }
            if bytes_count == 0 {
                1
            } else {
                bytes_count
            }
        })
        .sum()
}

pub fn compress(data: &[u64], dst: &mut Vec<u8>) {
    let mut delta = 0;
    for &v in data {
        if v < delta {
            panic!("Links are not sorted");
        }

        let mut v = v - delta;
        delta = v;

        loop {
            let byte = (v & 127) as u8;

            v >>= 7;
            if v > 0 {
                dst.push(128 | byte);
            } else {
                dst.push(byte);
            }

            if v == 0 {
                break;
            }
        }
    }
}
