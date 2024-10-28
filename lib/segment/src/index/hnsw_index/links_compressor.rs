use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};

pub struct DecompressIterator<T: Iterator<Item = u8>> {
    data: T,
    delta: u32,
}

impl<T: Iterator<Item = u8>> DecompressIterator<T> {
    pub fn new(data: T) -> Self {
        DecompressIterator { data, delta: 0 }
    }
}

impl<T: Iterator<Item = u8>> Iterator for DecompressIterator<T> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let mut link: PointOffsetType = 0;
        #[allow(for_loops_over_fallibles)]
        for byte in self.data.next() {
            let byte = byte as PointOffsetType;
            link |= (link << 7) | (byte & 127);
            if byte < 128 {
                let result = link + self.delta;
                self.delta = link;
                return Some(result);
            }
        }
        None
    }
}

pub fn estimate_compressed_size(data: &[u32]) -> usize {
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

pub fn compress(data: &[u32], dst: &mut [u8]) -> OperationResult<()> {
    if dst.len() != estimate_compressed_size(data) {
        return Err(OperationError::service_error(
            "Destination links buffer size does not match the estimated compressed size",
        ));
    }

    let mut pos = 0;
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
                dst[pos] = 128 | byte;
            } else {
                dst[pos] = byte;
            }
            pos += 1;

            if v == 0 {
                break;
            }
        }
    }

    Ok(())
}
