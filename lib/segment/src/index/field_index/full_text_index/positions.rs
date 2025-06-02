use posting_list::{PostingValue, UnsizedHandler, UnsizedValue};
use zerocopy::{FromBytes, IntoBytes};

/// Represents a list of positions of a token in a document.
#[derive(Default, Clone, Debug)]
pub(super) struct Positions(Vec<u32>);

impl Positions {
    pub fn push(&mut self, position: u32) {
        self.0.push(position);
    }
}

impl PostingValue for Positions {
    type Handler = UnsizedHandler<Self>;
}

impl UnsizedValue for Positions {
    fn write_len(&self) -> usize {
        self.0.as_bytes().len()
    }

    fn write_to(&self, dst: &mut [u8]) {
        self.0
            .as_slice()
            .write_to(dst)
            .expect("write_len should provide correct length");
    }

    fn from_bytes(data: &[u8]) -> Self {
        let positions =
            <[u32]>::ref_from_bytes(data).expect("write_len should provide correct length");
        Positions(positions.to_vec())
    }
}
