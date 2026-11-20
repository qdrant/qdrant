//! LEB128 varints for the compact on-disk structures of the index.

/// Append `value` as a LEB128 varint.
pub(super) fn write_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Split a LEB128 varint off `rest`, `None` if it is truncated or overflows.
pub(super) fn read_varint(rest: &mut &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for (index, &byte) in rest.iter().enumerate().take(5) {
        value |= u32::from(byte & 0x7F).checked_shl(7 * index as u32)?;
        if byte & 0x80 == 0 {
            *rest = &rest[index + 1..];
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trip() {
        for value in [0, 1, 127, 128, 300, 16_384, 1 << 21, u32::MAX] {
            let mut bytes = Vec::new();
            write_varint(&mut bytes, value);
            let mut rest = bytes.as_slice();
            assert_eq!(read_varint(&mut rest), Some(value));
            assert!(rest.is_empty());
            assert_eq!(read_varint(&mut &bytes[..bytes.len() - 1]), None);
        }
    }
}
