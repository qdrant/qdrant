use super::{
    index::{with_scratch, HeadIndex, MissScratch, EDGE_TOPK},
    search::{bf16_to_f32, dot_bf16_bytes},
};
use rayon::prelude::*;
const MISS_CHUNK_MIN: usize = 16;
const MISS_EF: usize = 256;
pub(super) fn centred_f32(keys_bf16: &[u8], dim: usize) -> Vec<f32> {
    let n = keys_bf16.len() / (dim * 2);
    let mut mean = vec![0.0f64; dim];
    let val = |i: usize, d: usize| -> f32 {
        let o = (i * dim + d) * 2;
        bf16_to_f32(u16::from_le_bytes([keys_bf16[o], keys_bf16[o + 1]]))
    };
    for i in 0..n {
        for (d, m) in mean.iter_mut().enumerate() {
            *m += val(i, d) as f64;
        }
    }
    if n > 0 {
        for m in mean.iter_mut() {
            *m /= n as f64;
        }
    }
    let mean: Vec<f32> = mean.into_iter().map(|m| m as f32).collect();
    let mut out = vec![0.0f32; n * dim];
    for i in 0..n {
        for d in 0..dim {
            out[i * dim + d] = val(i, d) - mean[d];
        }
    }
    out
}
fn exact_topk_causal_into(
    q: &[u8],
    keys: &[u8],
    dim: usize,
    position: u32,
    k: usize,
    scored: &mut Vec<(f32, u32)>,
    out: &mut Vec<u32>,
) {
    let n = keys.len() / (dim * 2);
    let limit = (position as usize).saturating_add(1).min(n);
    let row = dim * 2;
    scored.clear();
    scored.extend((0..limit).map(|i| (dot_bf16_bytes(q, &keys[i * row..(i + 1) * row]), i as u32)));
    out.clear();
    let k = k.min(scored.len());
    if k == 0 {
        return;
    }
    if k < scored.len() {
        scored.select_nth_unstable_by(k - 1, |a, b| b.0.total_cmp(&a.0));
        scored.truncate(k);
    }
    scored.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
    out.extend(scored.iter().map(|&(_, i)| i));
}
pub(super) fn miss_edges(
    idx: &mut HeadIndex,
    centred: &[f32],
    keys: &[u8],
    dim: usize,
    queries: &[(&[u8], u32)],
    miss_cap: usize,
) -> (usize, u64) {
    let n = idx.n as usize;
    let row = dim * 2;
    let chunk = queries
        .len()
        .div_ceil((2 * rayon::current_num_threads()).max(1))
        .max(MISS_CHUNK_MIN);
    let g: &HeadIndex = idx;
    let parts: Vec<(usize, Vec<u64>)> = queries
        .par_chunks(chunk)
        .map(|chunk| {
            let mut scored: Vec<(f32, u32)> = Vec::new();
            let mut one: Vec<u32> = Vec::with_capacity(EDGE_TOPK);
            let mut sc = MissScratch::default();
            for &(q, pos) in chunk {
                exact_topk_causal_into(q, keys, dim, pos, EDGE_TOPK, &mut scored, &mut one);
                let mut scorer = |id: u32| -> f32 {
                    dot_bf16_bytes(q, &keys[id as usize * row..(id as usize + 1) * row])
                };
                // a query at `pos` may see the keys at positions <= pos, i.e. ids <= pos
                with_scratch(n, |st, epoch| {
                    g.miss_pairs_for_query(
                        &mut scorer,
                        &one,
                        pos,
                        MISS_EF,
                        centred,
                        dim,
                        st,
                        epoch,
                        &mut sc,
                    )
                });
            }
            (chunk.len(), sc.pairs)
        })
        .collect();
    let ran: usize = parts.iter().map(|p| p.0).sum();
    let mut pairs: Vec<u64> = Vec::with_capacity(parts.iter().map(|p| p.1.len()).sum());
    for (_, p) in parts {
        pairs.extend_from_slice(&p);
    }
    let added = idx.add_miss_edges(&mut pairs, miss_cap);
    (ran, added)
}
