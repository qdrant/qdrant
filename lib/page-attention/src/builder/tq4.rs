use super::kernel::Codec;
use super::search::{bf16_to_f32, f32_to_bf16};
use crate::arr::Arr;
pub use crate::tq4::*;
use half::f16;
use rayon::prelude::*;
pub struct Lut {
    pub dim: usize,
    pub levels: usize,
    pub table: Vec<f32>,
    pub q: Vec<f32>,
}

impl Lut {
    pub fn new(q: &[f32], rot: &Rotation, codec: Codec) -> Lut {
        let dim = rot.dim;
        let levels = codec.levels();
        let mut rq = vec![0.0f32; dim];
        rot.apply(q, &mut rq);
        let inv = 1.0f32 / (dim as f32).sqrt();
        let mut table = vec![0.0f32; dim * levels];
        for (i, &qi) in rq.iter().enumerate() {
            for (v, &level) in codec.codebook().iter().enumerate() {
                table[i * levels + v] = qi * level * inv;
            }
        }
        Lut {
            dim,
            levels,
            table,
            q: q.to_vec(),
        }
    }

    pub fn dot_f16(&self, mean: &[u16]) -> f32 {
        let mut s = 0.0f32;
        for (i, &m) in mean.iter().enumerate() {
            s += self.q[i] * f16::from_bits(m).to_f32();
        }
        s
    }
}

// ---------------------------------------------------------------------------------------------
// the coded keys and values of one (layer, kv head)
// ---------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------
// the block centroid the key codes are a residual to
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum Centroid {
    F16 = 0,
    #[default]
    I8 = 1,
}

impl Centroid {
    pub fn parse(s: &str) -> Option<Centroid> {
        match s {
            "fp16" | "f16" | "mean" => Some(Centroid::F16),
            "int8" | "i8" => Some(Centroid::I8),
            _ => None,
        }
    }
    pub const fn name(self) -> &'static str {
        match self {
            Centroid::F16 => "fp16",
            Centroid::I8 => "int8",
        }
    }
    pub const fn from_u32(v: u32) -> Option<Centroid> {
        match v {
            0 => Some(Centroid::F16),
            1 => Some(Centroid::I8),
            _ => None,
        }
    }
    pub const fn bytes_per_block(self, dim: usize) -> usize {
        match self {
            Centroid::F16 => 2 * dim,
            Centroid::I8 => dim + 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CentroidSpec {
    pub kind: Centroid,
    pub iters: usize,
}

pub const DEFAULT_CENTROID_ITERS: usize = 3;

impl Default for CentroidSpec {
    fn default() -> CentroidSpec {
        CentroidSpec {
            kind: Centroid::default(),
            iters: DEFAULT_CENTROID_ITERS,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Cents<'a> {
    pub kind: Centroid,
    pub dim: usize,
    pub n_blocks: usize,
    pub bytes: &'a [u8],
}

impl<'a> Cents<'a> {
    #[inline]
    pub fn row(&self, b: usize) -> &'a [u8] {
        let w = self.kind.bytes_per_block(self.dim);
        &self.bytes[b * w..(b + 1) * w]
    }

    #[inline]
    pub fn into_f32(&self, b: usize, out: &mut [f32]) {
        decode_centroid(self.kind, self.dim, self.row(b), out);
    }

    pub fn bytes_len(&self) -> usize {
        self.n_blocks * self.kind.bytes_per_block(self.dim)
    }
}

#[inline]
pub fn decode_centroid(kind: Centroid, dim: usize, row: &[u8], out: &mut [f32]) {
    match kind {
        Centroid::F16 => {
            for i in 0..dim {
                out[i] = f16::from_bits(u16::from_le_bytes([row[2 * i], row[2 * i + 1]])).to_f32();
            }
        }
        Centroid::I8 => {
            let s = f16::from_bits(u16::from_le_bytes([row[dim], row[dim + 1]])).to_f32();
            for i in 0..dim {
                out[i] = (row[i] as i8) as f32 * s;
            }
        }
    }
}

pub fn quantise_centroid(kind: Centroid, c: &[f32], row: &mut [u8], out: &mut [f32]) {
    let dim = c.len();
    match kind {
        Centroid::F16 => {
            for i in 0..dim {
                let h = f16::from_f32(c[i]);
                row[2 * i..2 * i + 2].copy_from_slice(&h.to_bits().to_le_bytes());
                out[i] = h.to_f32();
            }
        }
        Centroid::I8 => {
            let m = c.iter().fold(0.0f32, |a, &v| a.max(v.abs()));
            let s16 = f16::from_f32(m / 127.0);
            let s = s16.to_f32();
            row[dim..dim + 2].copy_from_slice(&s16.to_bits().to_le_bytes());
            if s > 0.0 {
                let inv = 1.0f32 / s;
                for i in 0..dim {
                    let v = (c[i] * inv).round().clamp(-127.0, 127.0) as i8;
                    row[i] = v as u8;
                    out[i] = v as f32 * s;
                }
            } else {
                row[..dim].fill(0);
                out[..dim].fill(0.0);
            }
        }
    }
}

pub struct TqHead {
    pub codec: Codec,
    pub centroid: Centroid,
    pub n_tokens: u32,
    pub n_blocks: u32,
    pub dim: usize,
    pub block: usize,
    pub key_cent: Arr<u8>,
    pub key_norms: Arr<u16>,
    pub key_codes: Arr<u8>,
    pub value_norms: Arr<u16>,
    pub value_codes: Arr<u8>,
}

pub const CODEC_BLOCK: usize = 32;

pub fn block_len(page_tokens: usize) -> usize {
    if page_tokens > CODEC_BLOCK && page_tokens.is_multiple_of(CODEC_BLOCK) {
        CODEC_BLOCK
    } else {
        page_tokens
    }
}

pub fn blocks_per_page(page_tokens: usize) -> usize {
    page_tokens / block_len(page_tokens)
}

#[inline]
pub fn codes_per_row(block: usize, codec: Codec) -> usize {
    codec.code_bytes(block)
}

#[inline]
pub fn codes_per_block(dim: usize, block: usize, codec: Codec) -> usize {
    dim * codes_per_row(block, codec)
}

impl TqHead {
    pub fn encode(
        keys: &[f32],
        values: &[f32],
        dim: usize,
        block: usize,
        rot: &Rotation,
        codec: Codec,
    ) -> TqHead {
        TqHead::encode_spec(
            keys,
            values,
            dim,
            block,
            rot,
            codec,
            CentroidSpec::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_spec(
        keys: &[f32],
        values: &[f32],
        dim: usize,
        block: usize,
        rot: &Rotation,
        codec: Codec,
        cent: CentroidSpec,
    ) -> TqHead {
        let n = keys.len() / dim;
        TqHead::encode_from(n, dim, block, rot, codec, cent, |want_keys, lo, hi, out| {
            let src = if want_keys { keys } else { values };
            out.clear();
            out.extend_from_slice(&src[lo * dim..hi * dim]);
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_from<F>(
        n: usize,
        dim: usize,
        block: usize,
        rot: &Rotation,
        codec: Codec,
        cent: CentroidSpec,
        fill: F,
    ) -> TqHead
    where
        F: Fn(bool, usize, usize, &mut Vec<f32>) + Sync,
    {
        assert!(
            block >= codec.per_byte() && block.is_multiple_of(codec.per_byte()),
            "page_tokens must be a multiple of {} for {}",
            codec.per_byte(),
            codec.name()
        );
        let n_blocks = n.div_ceil(block).max(1);
        let cpb = codes_per_block(dim, block, codec);
        let cbytes = cent.kind.bytes_per_block(dim);
        let mut key_cent = vec![0u8; n_blocks * cbytes];
        let mut key_norms = vec![0u16; n_blocks * block];
        let mut key_codes = vec![0u8; n_blocks * cpb];
        let mut value_norms = vec![0u16; n_blocks * block];
        let mut value_codes = vec![0u8; n_blocks * cpb];
        // Blocks are independent: encode them in parallel over disjoint output slices.
        let mut jobs: Vec<_> = key_cent
            .chunks_mut(cbytes)
            .zip(key_norms.chunks_mut(block))
            .zip(key_codes.chunks_mut(cpb))
            .zip(value_norms.chunks_mut(block))
            .zip(value_codes.chunks_mut(cpb))
            .enumerate()
            .map(|(b, ((((kcent, knorm), kcode), vnorm), vcode))| {
                (b, kcent, knorm, kcode, vnorm, vcode)
            })
            .collect();
        // one pair of block-sized scratch buffers per worker, not per block
        jobs.par_iter_mut().for_each_init(
            || {
                (
                    Vec::<f32>::with_capacity(block * dim),
                    Vec::<f32>::with_capacity(block * dim),
                )
            },
            |(kbuf, vbuf), (b, kcent, knorm, kcode, vnorm, vcode)| {
                let lo = *b * block;
                let hi = ((*b + 1) * block).min(n);
                let valid = hi.saturating_sub(lo);
                fill(true, lo, hi, kbuf);
                fill(false, lo, hi, vbuf);
                encode_block(
                    kbuf,
                    valid,
                    dim,
                    block,
                    rot,
                    Some((cent, &mut **kcent)),
                    knorm,
                    kcode,
                    codec,
                );
                // values are coded against zero (`night_e3`: the block-mean residual is actively
                // harmful for values)
                encode_block(vbuf, valid, dim, block, rot, None, vnorm, vcode, codec);
            },
        );
        drop(jobs);
        TqHead {
            codec,
            centroid: cent.kind,
            n_tokens: n as u32,
            n_blocks: n_blocks as u32,
            dim,
            block,
            key_cent: Arr::from_vec(key_cent),
            key_norms: Arr::from_vec(key_norms),
            key_codes: Arr::from_vec(key_codes),
            value_norms: Arr::from_vec(value_norms),
            value_codes: Arr::from_vec(value_codes),
        }
    }

    #[inline]
    pub fn cents(&self) -> Cents<'_> {
        Cents {
            kind: self.centroid,
            dim: self.dim,
            n_blocks: self.n_blocks as usize,
            bytes: &self.key_cent,
        }
    }

    pub fn heap_bytes(&self) -> u64 {
        (self.key_cent.heap_bytes()
            + self.key_norms.heap_bytes()
            + self.key_codes.heap_bytes()
            + self.value_norms.heap_bytes()
            + self.value_codes.heap_bytes()) as u64
    }

    pub fn is_mapped(&self) -> bool {
        self.key_codes.is_mapped()
    }

    pub fn bytes(&self) -> u64 {
        (self.key_cent.len()
            + self.key_norms.len() * 2
            + self.key_codes.len()
            + self.value_norms.len() * 2
            + self.value_codes.len()) as u64
    }

    pub fn bits_per_dim(&self) -> f64 {
        if self.n_tokens == 0 {
            return 0.0;
        }
        self.bytes() as f64 * 8.0 / (self.n_tokens as f64 * self.dim as f64)
    }

    #[inline]
    pub fn block_of(&self, pos: u32) -> usize {
        pos as usize / self.block
    }

    #[inline]
    pub fn centroid_of_pos(&self, pos: u32, out: &mut [f32]) {
        self.cents().into_f32(self.block_of(pos), out);
    }

    #[inline]
    pub fn block_codes_at(&self, keys: bool, b: usize) -> &[u8] {
        let cpb = codes_per_block(self.dim, self.block, self.codec);
        let src = if keys {
            &self.key_codes
        } else {
            &self.value_codes
        };
        &src[b * cpb..(b + 1) * cpb]
    }

    #[inline]
    pub fn code(&self, keys: bool, pos: u32, i: usize) -> u8 {
        let (per_byte, bits) = (self.codec.per_byte(), self.codec.bits());
        let b = self.block_of(pos);
        let t = pos as usize % self.block;
        let row = codes_per_row(self.block, self.codec);
        let byte = self.block_codes_at(keys, b)[i * row + t / per_byte];
        (byte >> ((t % per_byte) * bits)) & ((1u8 << bits) - 1)
    }

    #[inline]
    pub fn norm(&self, keys: bool, pos: u32) -> f32 {
        let src = if keys {
            &self.key_norms
        } else {
            &self.value_norms
        };
        f16::from_bits(src[pos as usize]).to_f32()
    }

    pub fn for_each_block_g(&self, keys: bool, b: usize, mut f: impl FnMut(usize, &[f32])) {
        let dim = self.dim;
        let mut g = vec![0.0f32; dim];
        for t in 0..self.block {
            self.g_at(keys, (b * self.block + t) as u32, &mut g);
            f(t, &g);
        }
    }

    pub fn g_at(&self, keys: bool, pos: u32, g: &mut [f32]) {
        let dim = self.dim;
        let inv = 1.0f32 / (dim as f32).sqrt();
        let cb = self.codec.codebook();
        let n = self.norm(keys, pos);
        for (i, gi) in g[..dim].iter_mut().enumerate() {
            *gi = n * cb[self.code(keys, pos, i) as usize] * inv;
        }
    }

    pub fn requant_s4(&self, g: &[f32], codes: &mut [u8]) -> f32 {
        let dim = self.dim;
        let norm16 = f16::from_f32(
            g[..dim]
                .iter()
                .map(|&v| (v as f64) * (v as f64))
                .sum::<f64>()
                .sqrt() as f32,
        );
        let norm = norm16.to_f32();
        codes[..dim].fill(0);
        if norm > 0.0 {
            let s = (dim as f32).sqrt() / norm;
            for i in 0..dim {
                codes[i] = self.codec.quantise(g[i] * s);
            }
        }
        norm
    }

    #[inline]
    pub fn score_key(&self, pos: u32, lut: &Lut, mean_dot: f32) -> f32 {
        let (dim, block) = (self.dim, self.block);
        let b = self.block_of(pos);
        let tb = pos as usize % block;
        let codes = self.block_codes_at(true, b);
        let (per_byte, bits) = (self.codec.per_byte(), self.codec.bits());
        let mask = (1u8 << bits) - 1;
        let row = codes_per_row(block, self.codec);
        let (byte, shift) = (tb / per_byte, ((tb % per_byte) * bits) as u32);
        let mut s = 0.0f32;
        for i in 0..dim {
            let c = ((codes[i * row + byte] >> shift) & mask) as usize;
            s += lut.table[i * lut.levels + c];
        }
        mean_dot + self.norm(true, pos) * s
    }

    pub fn dequantise(&self, keys: bool, pos: u32, rot: &Rotation, out: &mut [f32]) {
        let dim = self.dim;
        let mut g = vec![0.0f32; dim];
        self.g_at(keys, pos, &mut g);
        rot.apply_t(&g, out);
        if keys {
            let mut c = vec![0.0f32; dim];
            self.centroid_of_pos(pos, &mut c);
            for i in 0..dim {
                out[i] += c[i];
            }
        }
    }

    pub fn dequantise_bf16(&self, keys: bool, pos: u32, rot: &Rotation, out: &mut [u8]) {
        let dim = self.dim;
        let mut buf = vec![0.0f32; dim];
        self.dequantise(keys, pos, rot, &mut buf);
        for (i, &v) in buf.iter().enumerate() {
            out[2 * i..2 * i + 2].copy_from_slice(&f32_to_bf16(v).to_le_bytes());
        }
    }

    // ---- wire payloads ---------------------------------------------------------------------

    pub fn page_slices(&self, b: usize) -> BlockSlices<'_> {
        let cpb = codes_per_block(self.dim, self.block, self.codec);
        (
            self.cents().row(b),
            &self.key_norms[b * self.block..(b + 1) * self.block],
            &self.key_codes[b * cpb..(b + 1) * cpb],
            &self.value_norms[b * self.block..(b + 1) * self.block],
            &self.value_codes[b * cpb..(b + 1) * cpb],
        )
    }
}

pub type BlockSlices<'a> = (&'a [u8], &'a [u16], &'a [u8], &'a [u16], &'a [u8]);

fn block_mean(x: &[f32], valid: usize, dim: usize) -> Vec<f32> {
    let mut mean = vec![0.0f32; dim];
    if valid > 0 {
        let mut acc = vec![0.0f64; dim];
        for t in 0..valid {
            for i in 0..dim {
                acc[i] += x[t * dim + i] as f64;
            }
        }
        for i in 0..dim {
            mean[i] = (acc[i] / valid as f64) as f32;
        }
    }
    mean
}

#[allow(clippy::too_many_arguments)]
fn encode_block(
    x: &[f32],
    valid: usize,
    dim: usize,
    block: usize,
    rot: &Rotation,
    cent: Option<(CentroidSpec, &mut [u8])>,
    norms: &mut [u16],
    codes: &mut [u8],
    codec: Codec,
) {
    let mut c = vec![0.0f32; dim];
    if let Some((spec, row)) = cent {
        let mean = block_mean(x, valid, dim);
        quantise_centroid(spec.kind, &mean, row, &mut c);
        if valid > 0 {
            let mut mg = vec![0.0f32; dim];
            let mut back = vec![0.0f32; dim];
            let mut next = vec![0.0f32; dim];
            for _ in 0..spec.iters {
                code_block(
                    x,
                    valid,
                    dim,
                    block,
                    rot,
                    &c,
                    None,
                    None,
                    codec,
                    Some(&mut mg),
                );
                rot.apply_t(&mg, &mut back);
                for i in 0..dim {
                    next[i] = mean[i] - back[i];
                }
                quantise_centroid(spec.kind, &next, row, &mut c);
            }
        }
    }
    code_block(
        x,
        valid,
        dim,
        block,
        rot,
        &c,
        Some(norms),
        Some(codes),
        codec,
        None,
    );
    // padding tokens: norm 0, codes 0 (already zeroed) â€” n_tokens tells the reader which are real
}

#[allow(clippy::too_many_arguments)]
fn code_block(
    x: &[f32],
    valid: usize,
    dim: usize,
    block: usize,
    rot: &Rotation,
    c: &[f32],
    mut norms: Option<&mut [u16]>,
    mut codes: Option<&mut [u8]>,
    codec: Codec,
    mean_g: Option<&mut [f32]>,
) {
    let (per_byte, bits) = (codec.per_byte(), codec.bits());
    let row = codes_per_row(block, codec);
    let cb = codec.codebook();
    let mut r = vec![0.0f32; dim];
    let mut rr = vec![0.0f32; dim];
    let sqrt_d = (dim as f32).sqrt();
    let inv_d = 1.0f32 / sqrt_d;
    let mut acc = vec![0.0f64; if mean_g.is_some() { dim } else { 0 }];
    for t in 0..valid {
        for i in 0..dim {
            r[i] = x[t * dim + i] - c[i];
        }
        rot.apply(&r, &mut rr);
        let norm_exact = rr
            .iter()
            .map(|&v| (v as f64) * (v as f64))
            .sum::<f64>()
            .sqrt() as f32;
        let norm16 = f16::from_f32(norm_exact);
        if let Some(n) = norms.as_deref_mut() {
            n[t] = norm16.to_bits();
        }
        let norm = norm16.to_f32();
        if norm <= 0.0 {
            continue;
        }
        let s = sqrt_d / norm;
        let (byte, shift) = (t / per_byte, ((t % per_byte) * bits) as u32);
        for i in 0..dim {
            let code = codec.quantise(rr[i] * s);
            if let Some(cd) = codes.as_deref_mut() {
                cd[i * row + byte] |= code << shift;
            }
            if !acc.is_empty() {
                acc[i] += (norm * cb[code as usize] * inv_d) as f64;
            }
        }
    }
    if let Some(out) = mean_g {
        let d = if valid > 0 { valid as f64 } else { 1.0 };
        for i in 0..dim {
            out[i] = (acc[i] / d) as f32;
        }
    }
}

pub fn bf16_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    (0..bytes.len() / 2)
        .map(|i| bf16_to_f32(u16::from_le_bytes([bytes[2 * i], bytes[2 * i + 1]])))
        .collect()
}
