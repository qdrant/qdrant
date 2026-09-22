use crate::arr::Arr;
use crate::builder::kernel::{self, Codec, KeyQuery};
use crate::builder::tq4::TqHead;

#[repr(align(64))]
#[derive(Clone, Copy)]
struct Line(#[allow(dead_code)] [u8; 64]);

enum Codes {
    Owned(Vec<Line>),
    Mapped(Arr<u8>),
}

impl Codes {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            // SAFETY: `Line` is a plain byte array; the slice covers the same allocation.
            Codes::Owned(v) => unsafe {
                std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 64)
            },
            Codes::Mapped(a) => a,
        }
    }
    fn heap_bytes(&self) -> usize {
        match self {
            Codes::Owned(v) => v.len() * 64,
            Codes::Mapped(a) => a.heap_bytes(),
        }
    }
}

pub struct NodeRecords {
    pub codec: Codec,
    pub dim: usize,
    pub block: usize,
    pub n: u32,
    pub stride: usize,
    codes: Codes,
    pub norms: Arr<f32>,
}

impl NodeRecords {
    pub fn from_head(head: &TqHead) -> NodeRecords {
        let codec = head.codec;
        let (dim, block) = (head.dim, head.block);
        let stride = codec.code_bytes(dim);
        let slots = head.n_blocks as usize * block;
        let lines = (slots * stride).div_ceil(64);
        let mut recs = NodeRecords {
            codec,
            dim,
            block,
            n: head.n_tokens,
            stride,
            codes: Codes::Owned(vec![Line([0u8; 64]); lines]),
            norms: Arr::from_vec(
                head.key_norms
                    .iter()
                    .map(|&b| half::f16::from_bits(b).to_f32())
                    .collect(),
            ),
        };
        let per_byte = codec.per_byte();
        let bits = codec.bits();
        let row = codec.code_bytes(block); // bytes one coordinate row of a block occupies
        let out = recs.codes_mut();
        for t in 0..head.n_tokens as usize {
            let b = t / block;
            let tb = t % block;
            let src = head.block_codes_at(true, b);
            let dst = &mut out[t * stride..(t + 1) * stride];
            let (byte, shift) = (tb / per_byte, (tb % per_byte) * bits);
            for i in 0..dim {
                let code = (src[i * row + byte] >> shift) & ((1u8 << bits) - 1);
                // record byte g holds coordinates `per_byte * g ..` in ascending bit order
                dst[i / per_byte] |= code << ((i % per_byte) * bits);
            }
        }
        recs
    }

    pub fn from_parts(
        codec: Codec,
        dim: usize,
        block: usize,
        n: u32,
        codes: Arr<u8>,
        norms: Arr<f32>,
    ) -> Result<NodeRecords, String> {
        let stride = codec.code_bytes(dim);
        let slots = norms.len();
        if stride == 0 || slots == 0 || codes.len() < slots * stride {
            return Err(format!(
                "records: {} code bytes and {slots} norms for a stride of {stride}",
                codes.len()
            ));
        }
        if n as usize > slots {
            return Err(format!("records: {n} keys in {slots} slots"));
        }
        if !(codes.as_ptr() as usize).is_multiple_of(64) {
            return Err("records: the code array is not 64-byte aligned".to_string());
        }
        Ok(NodeRecords {
            codec,
            dim,
            block,
            n,
            stride,
            codes: Codes::Mapped(codes),
            norms,
        })
    }

    #[inline]
    pub fn codes(&self) -> &[u8] {
        self.codes.bytes()
    }

    fn codes_mut(&mut self) -> &mut [u8] {
        match &mut self.codes {
            Codes::Owned(v) => unsafe {
                std::slice::from_raw_parts_mut(v.as_mut_ptr() as *mut u8, v.len() * 64)
            },
            Codes::Mapped(_) => panic!("NodeRecords: the records are mapped (read-only)"),
        }
    }

    pub fn slots(&self) -> usize {
        self.norms.len()
    }

    pub fn heap_bytes(&self) -> u64 {
        (self.codes.heap_bytes() + self.norms.heap_bytes()) as u64
    }

    pub fn is_mapped(&self) -> bool {
        matches!(self.codes, Codes::Mapped(_))
    }

    pub fn record(&self, t: u32) -> &[u8] {
        let o = t as usize * self.stride;
        &self.codes()[o..o + self.stride]
    }

    #[inline]
    pub fn block_of(&self, t: u32) -> usize {
        t as usize / self.block
    }

    pub fn bytes(&self) -> u64 {
        (self.codes.bytes().len() + self.norms.len() * 4) as u64
    }

    pub fn bytes_per_key(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.bytes() as f64 / self.n as f64
        }
    }

    pub fn score(&self, mean_dots: &[f32], kq: &KeyQuery, ids: &[u32], out: &mut [f32]) {
        // Issue every record load of the batch before doing any arithmetic: the beam's neighbour
        // list is a random gather, so this turns `ids.len()` dependent cache misses into one
        // round of parallel ones (`night_e2` measured +24-35 % from exactly this on the block
        // kernel; here it is what makes a 40-neighbour expansion cost one memory latency).

        self.prefetch(ids);
        kernel::score_keys(
            self.codes(),
            self.stride,
            &self.norms,
            self.block,
            mean_dots,
            kq,
            ids,
            out,
        );
    }

    #[inline]
    pub fn prefetch_i32(&self, ids: &[i32]) {
        let p: &[u32] =
            unsafe { std::slice::from_raw_parts(ids.as_ptr() as *const u32, ids.len()) };
        self.prefetch(p);
    }

    #[inline]
    pub fn prefetch(&self, ids: &[u32]) {
        let base = self.codes().as_ptr();
        for &id in ids {
            // SAFETY: id < n_slots for every id the graph can produce.
            unsafe { kernel::prefetch(base.add(id as usize * self.stride), self.stride) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::index::SplitMix64;
    use crate::builder::tq4::{rotation_seed, Rotation};

    fn head(codec: Codec, n: usize, dim: usize, block: usize) -> (TqHead, Rotation) {
        let rot = Rotation::new(rotation_seed("rec"), dim);
        let mut rng = SplitMix64::new(4);
        let keys: Vec<f32> = (0..n * dim)
            .map(|i| (rng.next_f64() as f32 - 0.5) * 2.0 + (i / (dim * block)) as f32)
            .collect();
        let values: Vec<f32> = (0..n * dim).map(|_| rng.next_f64() as f32 - 0.5).collect();
        (TqHead::encode(&keys, &values, dim, block, &rot, codec), rot)
    }

    #[test]
    fn records_are_the_block_codes_transposed() {
        for codec in [Codec::Tq4] {
            for (n, dim, block) in [(70usize, 128usize, 32usize), (33, 64, 8), (1, 128, 32)] {
                let (h, _) = head(codec, n, dim, block);
                let r = NodeRecords::from_head(&h);
                assert_eq!(r.stride, codec.code_bytes(dim));
                assert_eq!(r.n, n as u32);
                let per_byte = codec.per_byte();
                let bits = codec.bits();
                for t in 0..n as u32 {
                    let rec = r.record(t);
                    for i in 0..dim {
                        let want = h.code(true, t, i);
                        let got =
                            (rec[i / per_byte] >> ((i % per_byte) * bits)) & ((1u8 << bits) - 1);
                        assert_eq!(got, want, "{codec:?} n {n} dim {dim} key {t} coord {i}");
                    }
                }
                // the padding slots of the last block are zero
                for t in n..h.n_blocks as usize * block {
                    assert!(r.codes()[t * r.stride..(t + 1) * r.stride]
                        .iter()
                        .all(|&b| b == 0));
                }
            }
        }
    }

    #[test]
    fn record_bytes_are_the_documented_budget() {
        let (h, _) = head(Codec::Tq4, 1024, 128, 32);
        let r = NodeRecords::from_head(&h);
        // 64 B of codes + 4 B of norm per key
        assert!(
            (r.bytes_per_key() - 68.0).abs() < 0.1,
            "{} B/key",
            r.bytes_per_key()
        );
    }

    #[test]
    fn records_are_cache_line_aligned() {
        let (h, _) = head(Codec::Tq4, 100, 128, 32);
        let r = NodeRecords::from_head(&h);
        assert_eq!(r.codes().as_ptr() as usize % 64, 0);
    }
}
