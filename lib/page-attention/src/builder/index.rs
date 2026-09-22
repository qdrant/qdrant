use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use rayon::prelude::*;

use crate::arr::Arr;

pub const LMAX: usize = 8;

pub const EDGE_TOPK: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexParams {
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub min_links: usize,
    pub cooc_links: usize,
}

impl Default for IndexParams {
    fn default() -> Self {
        IndexParams {
            m: 16,
            m0: 32,
            ef_construct: 100,
            min_links: 8,
            cooc_links: 8,
        }
    }
}

pub fn head_scratch_bytes(n: u64, dim: u64, n_queries: u64, params: &IndexParams) -> u64 {
    let keys = 2 * n * dim;
    let centred = 4 * n * dim;
    let stride = (params.m0 + params.cooc_links) as u64;
    let graph = 4 * n * stride + 4 * n + 4 * n + 2 * n;
    let pairs = 8 * n_queries * EDGE_TOPK as u64;
    keys + graph + centred + pairs + 8 * n
}

#[derive(Clone, Copy, Debug)]
pub struct BuildPlan {
    pub parallel: usize,
    pub budget_mb: u64,
    pub requested: usize,
    pub per_head_bytes: u64,
    pub build_parallel: usize,
}

impl Default for BuildPlan {
    fn default() -> Self {
        BuildPlan {
            parallel: rayon::current_num_threads().max(1),
            budget_mb: 0,
            requested: 0,
            per_head_bytes: 0,
            build_parallel: 1,
        }
    }
}

impl BuildPlan {
    pub const MAX_DEFAULT_PARALLEL: usize = 8;

    pub fn resolve(
        requested: usize,
        budget_mb: u64,
        n_heads: usize,
        n: u64,
        dim: u64,
        n_queries_per_head: u64,
        params: &IndexParams,
    ) -> BuildPlan {
        let ceiling = if requested > 0 {
            requested
        } else {
            rayon::current_num_threads()
                .min(Self::MAX_DEFAULT_PARALLEL)
                .max(1)
        };
        let per_head_bytes = head_scratch_bytes(n, dim, n_queries_per_head, params);
        let mut parallel = ceiling.min(n_heads.max(1));
        if budget_mb > 0 && per_head_bytes > 0 {
            let fits = (budget_mb * 1024 * 1024 / per_head_bytes).max(1) as usize;
            parallel = parallel.min(fits);
        }
        BuildPlan {
            parallel: parallel.max(1),
            budget_mb,
            requested,
            per_head_bytes,
            build_parallel: 1,
        }
    }

    pub fn note(&self) -> String {
        format!(
            "{} head(s) at a time (--finalize-parallel {}, --finalize-mem-mb {},              estimate {:.0} MB of scratch per head), --build-parallel {}",
            self.parallel,
            if self.requested == 0 { "auto".to_string() } else { self.requested.to_string() },
            if self.budget_mb == 0 { "off".to_string() } else { self.budget_mb.to_string() },
            self.per_head_bytes as f64 / 1e6,
            self.build_parallel,
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HeadCost {
    pub layer: u32,
    pub kv_head: u32,
    pub n_keys: u32,
    pub n_queries: u32,
    pub graph_secs: f64,
    pub edge_secs: f64,
    pub encode_secs: f64,
    pub rss_mb: f64,
}

impl HeadCost {
    pub fn total_secs(&self) -> f64 {
        self.graph_secs + self.edge_secs + self.encode_secs
    }
}

// ---------------------------------------------------------------------------------------------
// level assignment
// ---------------------------------------------------------------------------------------------

pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

pub fn level_from_u(u: f64, m: usize) -> usize {
    let u = u.max(f64::MIN_POSITIVE);
    let lv = (-u.ln() / (m as f64).ln()).round();
    if lv <= 0.0 {
        0
    } else {
        (lv as usize).min(LMAX)
    }
}

pub fn random_levels(n: usize, m: usize, seed: u64) -> Vec<u8> {
    let mut rng = SplitMix64::new(seed);
    (0..n)
        .map(|_| level_from_u(rng.next_f64(), m) as u8)
        .collect()
}

pub const SEQ_HEAD: usize = 256;
pub const BUILD_BLOCKS: usize = 16;

pub fn qdrant_insertion_order(n: usize, head: usize, blocks: usize) -> Vec<u32> {
    let head = head.min(n);
    let mut order: Vec<u32> = (0..head as u32).collect();
    let rest = n - head;
    if rest == 0 {
        return order;
    }
    let blocks = blocks.max(1).min(rest);
    // np.array_split: the first `rest % blocks` parts are one longer
    let big = rest % blocks;
    let small = rest / blocks;
    let mut starts = Vec::with_capacity(blocks);
    let mut lens = Vec::with_capacity(blocks);
    let mut at = head;
    for b in 0..blocks {
        let len = small + usize::from(b < big);
        starts.push(at);
        lens.push(len);
        at += len;
    }
    let longest = lens.iter().copied().max().unwrap_or(0);
    for t in 0..longest {
        for b in 0..blocks {
            if t < lens[b] {
                order.push((starts[b] + t) as u32);
            }
        }
    }
    order
}

// ---------------------------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------------------------

#[inline]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut s = 0.0f32;
    for i in 0..a.len() {
        s += a[i] * b[i];
    }
    s
}

pub fn dots_into(q: &[f32], keys: &[f32], dim: usize, ids: &[u32], out: &mut [f32]) {
    debug_assert_eq!(out.len(), ids.len());
    let mut j = 0;
    // 8 chains is where Zen 4 saturates its two FP pipes; the 4- and 2-wide steps matter as much,
    // because a warm beam expands a node whose neighbours are mostly visited already and the
    // batch it hands over is often 2-6 keys wide.
    while j + 8 <= ids.len() {
        dots_group::<8>(q, keys, dim, &ids[j..j + 8], &mut out[j..j + 8]);
        j += 8;
    }
    if j + 4 <= ids.len() {
        dots_group::<4>(q, keys, dim, &ids[j..j + 4], &mut out[j..j + 4]);
        j += 4;
    }
    if j + 2 <= ids.len() {
        dots_group::<2>(q, keys, dim, &ids[j..j + 2], &mut out[j..j + 2]);
        j += 2;
    }
    if j < ids.len() {
        out[j] = dot_f32(q, &keys[ids[j] as usize * dim..(ids[j] as usize + 1) * dim]);
    }
}

#[inline]
fn dots_group<const G: usize>(q: &[f32], keys: &[f32], dim: usize, ids: &[u32], out: &mut [f32]) {
    let mut row = [0usize; G];
    for (r, &id) in row.iter_mut().zip(ids) {
        *r = id as usize * dim;
    }
    let mut acc = [0.0f32; G];
    for d in 0..dim {
        let qd = q[d];
        for g in 0..G {
            // one multiply and one add per lane, in coordinate order: the same arithmetic
            // `dot_f32` performs, with G chains instead of one
            acc[g] += qd * keys[row[g] + d];
        }
    }
    out.copy_from_slice(&acc);
}

struct BuildScorer<'a> {
    keys: &'a [f32],
    bias: &'a [f32],
    dim: usize,
    q: &'a [f32],
}

impl BuildScorer<'_> {
    #[inline]
    fn one(&self, i: u32) -> f32 {
        dot_f32(
            self.q,
            &self.keys[i as usize * self.dim..(i as usize + 1) * self.dim],
        ) - self.bias[i as usize]
    }
}

impl Scorer for BuildScorer<'_> {
    #[inline]
    fn score_many(&mut self, ids: &[u32], out: &mut [f32]) {
        dots_into(self.q, self.keys, self.dim, ids, out);
        for (o, &id) in out.iter_mut().zip(ids) {
            *o -= self.bias[id as usize];
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Sc(u64);

#[inline(always)]
fn ordered_bits(x: f32) -> u32 {
    let b = x.to_bits();
    if b >> 31 == 1 {
        !b
    } else {
        b | 0x8000_0000
    }
}

#[inline(always)]
fn from_ordered(k: u32) -> f32 {
    f32::from_bits(if k >> 31 == 1 { k & 0x7fff_ffff } else { !k })
}

impl Sc {
    #[inline(always)]
    fn new(score: f32, id: u32) -> Sc {
        Sc(((ordered_bits(score) as u64) << 32) | id as u64)
    }
    #[inline(always)]
    fn score(self) -> f32 {
        from_ordered((self.0 >> 32) as u32)
    }
    #[inline(always)]
    fn id(self) -> u32 {
        self.0 as u32
    }
}

pub const MAX_DEG: usize = 128;

pub struct Visited<'a> {
    st: &'a mut [u16],
    ep: u16,
    pub distinct: usize,
}

pub const MAX_EPOCH: u16 = (1 << 15) - 1;

impl<'a> Visited<'a> {
    pub fn new(st: &'a mut [u16], epoch: u16) -> Visited<'a> {
        debug_assert!((1..=MAX_EPOCH).contains(&epoch));
        Visited {
            st,
            ep: epoch,
            distinct: 0,
        }
    }

    #[inline]
    fn claim(&mut self, i: u32) -> bool {
        let m: u16 = (self.ep << 1) | 1;
        let cur = unsafe { *self.st.get_unchecked(i as usize) };
        if cur == m {
            return false;
        }
        if (cur >> 1) != self.ep {
            self.distinct += 1;
        }
        unsafe { *self.st.get_unchecked_mut(i as usize) = m };
        true
    }

    #[inline]
    fn touch(&mut self, i: u32) {
        let cur = unsafe { *self.st.get_unchecked(i as usize) };
        if (cur >> 1) != self.ep {
            unsafe { *self.st.get_unchecked_mut(i as usize) = self.ep << 1 };
            self.distinct += 1;
        }
    }
}

pub trait Scorer {
    fn score_many(&mut self, ids: &[u32], out: &mut [f32]);

    #[inline]
    fn expanded(&mut self, _node: u32, _kind: u8) {}

    #[inline]
    fn prefetch(&mut self, _ids: &[i32]) {}
}

impl<F: FnMut(u32) -> f32> Scorer for F {
    #[inline]
    fn score_many(&mut self, ids: &[u32], out: &mut [f32]) {
        for (o, &id) in out.iter_mut().zip(ids) {
            *o = self(id);
        }
    }
}

pub fn lookahead() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("KVSTORE_NO_LOOKAHEAD").is_err())
}

pub trait Adj {
    fn with_nbrs<R>(&self, pos: &[i32], node: u32, f: impl FnOnce(&[i32]) -> R) -> R;
    fn prefetch_row(&self, pos: &[i32], node: u32);
}

pub struct Csr {
    offs: Arr<u32>,
    links: Arr<i32>,
}

impl Csr {
    fn from_level(lvl: &Lvl, n: usize) -> Csr {
        let mut offs = Vec::with_capacity(n + 1);
        let mut links = Vec::with_capacity(lvl.cnt.iter().map(|&c| c as usize).sum());
        for node in 0..n {
            offs.push(links.len() as u32);
            let off = node * lvl.stride;
            links.extend_from_slice(&lvl.links[off..off + lvl.cnt[node] as usize]);
        }
        offs.push(links.len() as u32);
        Csr {
            offs: Arr::from_vec(offs),
            links: Arr::from_vec(links),
        }
    }

    pub fn from_raw(offs: Arr<u32>, links: Arr<i32>) -> Csr {
        Csr { offs, links }
    }

    pub fn raw(&self) -> (&[u32], &[i32]) {
        (&self.offs, &self.links)
    }

    pub fn bytes(&self) -> u64 {
        (self.offs.len() * 4 + self.links.len() * 4) as u64
    }

    pub fn heap_bytes(&self) -> u64 {
        (self.offs.heap_bytes() + self.links.heap_bytes()) as u64
    }

    pub fn is_mapped(&self) -> bool {
        self.links.is_mapped()
    }
}

impl Adj for Csr {
    #[inline]
    fn with_nbrs<R>(&self, _pos: &[i32], node: u32, f: impl FnOnce(&[i32]) -> R) -> R {
        let a = self.offs[node as usize] as usize;
        let b = self.offs[node as usize + 1] as usize;

        f(&self.links[a..b])
    }
    #[inline]
    fn prefetch_row(&self, _pos: &[i32], node: u32) {
        let a = self.offs[node as usize] as usize;
        let b = self.offs[node as usize + 1] as usize;
        unsafe {
            crate::builder::kernel::prefetch(self.links.as_ptr().add(a) as *const u8, (b - a) * 4)
        };
    }
}

impl Csr {
    #[inline]
    fn nbrs(&self, _pos: &[i32], node: u32) -> &[i32] {
        let (a, b) = (
            self.offs[node as usize] as usize,
            self.offs[node as usize + 1] as usize,
        );
        &self.links[a..b]
    }
}

struct Lvl {
    links: Arr<i32>,
    cnt: Arr<u32>,
    stride: usize,
    m: usize,
    ident: bool,
}

impl Lvl {
    fn new(rows: usize, stride: usize, m: usize, ident: bool) -> Self {
        Lvl {
            links: Arr::from_vec(vec![-1; rows * stride]),
            cnt: Arr::from_vec(vec![0; rows]),
            stride,
            m,
            ident,
        }
    }
    #[inline]
    fn row(&self, pos: &[i32], node: u32) -> usize {
        if self.ident {
            node as usize
        } else {
            pos[node as usize] as usize
        }
    }
    #[inline]
    fn nbrs(&self, pos: &[i32], node: u32) -> &[i32] {
        let r = self.row(pos, node);
        let off = r * self.stride;

        &self.links[off..off + self.cnt[r] as usize]
    }
    #[inline]
    fn prefetch_row_of(&self, pos: &[i32], node: u32) {
        let r = self.row(pos, node);
        let off = r * self.stride;
        unsafe {
            crate::builder::kernel::prefetch(
                self.links.as_ptr().add(off) as *const u8,
                self.stride * 4,
            )
        };
    }

    fn raw(&self) -> (&[i32], &[u32]) {
        (&self.links, &self.cnt)
    }

    fn set(&mut self, pos: &[i32], node: u32, ids: &[u32]) {
        let r = self.row(pos, node);
        let off = r * self.stride;
        let links = self.links.as_mut_slice();
        for (j, &id) in ids.iter().enumerate() {
            links[off + j] = id as i32;
        }
        self.cnt.as_mut_slice()[r] = ids.len() as u32;
    }

    fn heap_bytes(&self) -> u64 {
        (self.links.heap_bytes() + self.cnt.heap_bytes()) as u64
    }
}

impl Adj for Lvl {
    #[inline]
    fn with_nbrs<R>(&self, pos: &[i32], node: u32, f: impl FnOnce(&[i32]) -> R) -> R {
        f(Lvl::nbrs(self, pos, node))
    }
    #[inline]
    fn prefetch_row(&self, pos: &[i32], node: u32) {
        self.prefetch_row_of(pos, node)
    }
}

// ---------------------------------------------------------------------------------------------
// the index
// ---------------------------------------------------------------------------------------------

pub struct HeadIndex {
    pub n: u32,
    pub dim: usize,
    pub params: IndexParams,
    pub levels: Arr<u8>,
    pub entry: u32,
    pub entry_level: usize,
    pub miss_edges: u64,
    degrees0: u64,
    miss_got: Vec<u32>,
    l0: Lvl,
    l0_csr: Option<Csr>,
    up: Vec<Lvl>,
    up_pos: Arr<i32>,
}

pub struct GraphParts<'a> {
    pub n: u32,
    pub dim: usize,
    pub params: IndexParams,
    pub entry: u32,
    pub entry_level: usize,
    pub miss_edges: u64,
    pub degrees0: u64,
    pub levels: &'a [u8],
    pub up_pos: &'a [i32],
    pub l0_offs: &'a [u32],
    pub l0_links: &'a [i32],
    pub up: Vec<(&'a [i32], &'a [u32])>,
}

pub struct StoredGraph {
    pub n: u32,
    pub dim: usize,
    pub params: IndexParams,
    pub entry: u32,
    pub entry_level: usize,
    pub miss_edges: u64,
    pub degrees0: u64,
    pub levels: Arr<u8>,
    pub up_pos: Arr<i32>,
    pub l0_offs: Arr<u32>,
    pub l0_links: Arr<i32>,
    pub up: Vec<(Arr<i32>, Arr<u32>)>,
    pub trust_l0: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn heuristic_select(
    cands: &[(f32, u32)],
    keys: &[f32],
    dim: usize,
    bias: &[f32],
    m: usize,
    target: u32,
    min_links: usize,
    out: &mut Vec<u32>,
) {
    heuristic_select_with(
        &mut SelBuf::default(),
        cands,
        keys,
        dim,
        bias,
        m,
        target,
        min_links,
        out,
    )
}

#[derive(Default)]
pub struct SelBuf {
    sel_s: Vec<f32>,
    pruned: Vec<u32>,
}

#[allow(clippy::too_many_arguments)]
fn heuristic_select_with(
    buf: &mut SelBuf,
    cands: &[(f32, u32)],
    keys: &[f32],
    dim: usize,
    bias: &[f32],
    m: usize,
    target: u32,
    min_links: usize,
    out: &mut Vec<u32>,
) {
    out.clear();
    let SelBuf { sel_s, pruned } = buf;
    sel_s.clear();
    pruned.clear();
    let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];
    for &(cand_s, ci) in cands {
        // score of the candidate against the target, in the target's own frame
        let cs = cand_s + bias[ci as usize] - bias[target as usize];
        let mut good = true;
        for (&e, &es) in out.iter().zip(sel_s.iter()) {
            let ce = dot_f32(key(ci), key(e)) - bias[e as usize];
            if cs == es {
                if ce >= cs {
                    good = false;
                    break;
                }
            } else if ce > cs {
                good = false;
                break;
            }
        }
        if good {
            out.push(ci);
            sel_s.push(cs);
            if out.len() >= m {
                break;
            }
        } else {
            pruned.push(ci);
        }
    }
    let floor = min_links.min(m);
    let mut p = 0;
    while out.len() < floor && p < pruned.len() {
        out.push(pruned[p]);
        p += 1;
    }
}

pub(crate) fn greedy_on_level<A: Adj + ?Sized, S: Scorer>(
    lvl: &A,
    pos: &[i32],
    scorer: &mut S,
    seen: Option<&mut Visited<'_>>,
    entry: u32,
    entry_score: f32,
    n: u32,
) -> (u32, f32, usize) {
    let mut seen = seen;
    let (mut cur, mut cs) = (entry, entry_score);
    let mut scored = 0usize;
    let mut ids = [0u32; MAX_DEG];
    let mut sc = [0.0f32; MAX_DEG];
    loop {
        scorer.expanded(cur, 0);
        let deg = lvl.with_nbrs(pos, cur, |nbrs| {
            let mut deg = 0usize;
            for &nb in nbrs.iter() {
                // an out-of-range id can only come from a corrupt file, and a mapped graph is
                // not scanned link by link on load (`StoredGraph::trust_l0`), so it is dropped
                // here: one predictable compare in front of a random record read
                if (nb as u32) < n && deg < MAX_DEG {
                    ids[deg] = nb as u32;
                    deg += 1;
                }
            }
            deg
        });
        let n = deg;
        if n == 0 {
            return (cur, cs, scored);
        }
        if let Some(v) = seen.as_deref_mut() {
            for &id in &ids[..n] {
                v.touch(id);
            }
        }
        scorer.score_many(&ids[..n], &mut sc[..n]);
        scored += n;
        let (mut best, mut bs) = (cur, cs);
        for j in 0..n {
            if sc[j] > bs {
                bs = sc[j];
                best = ids[j];
            }
        }
        if best == cur {
            return (cur, cs, scored);
        }
        cur = best;
        cs = bs;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn beam_on_level<A: Adj + ?Sized, S: Scorer>(
    lvl: &A,
    pos: &[i32],
    scorer: &mut S,
    entries: &[(f32, u32)],
    ef: usize,
    seen: &mut Visited<'_>,
    out: &mut Vec<(f32, u32)>,
    n_nodes: u32,
) -> usize {
    let mut scored = 0usize;
    let mut cand: BinaryHeap<Sc> = BinaryHeap::with_capacity(ef + MAX_DEG);
    let mut near: BinaryHeap<std::cmp::Reverse<Sc>> = BinaryHeap::with_capacity(ef + 1);
    // One entry is Qdrant's search. Several entries are accepted (the frontier and the nearest
    // heap start with all of them, their scores already known) but nothing served uses them any
    // more: the page-seeded multi-start that did was removed as a measured substitute for the
    // miss-driven edges.
    for &(sc, id) in entries {
        if id >= n_nodes || !seen.claim(id) {
            continue;
        }
        cand.push(Sc::new(sc, id));
        near.push(std::cmp::Reverse(Sc::new(sc, id)));
        if near.len() > ef {
            near.pop();
        }
    }
    let mut batch = [0u32; MAX_DEG];
    let mut sc = [0.0f32; MAX_DEG];
    let ahead = lookahead();
    while let Some(top) = cand.pop() {
        let (cs, ci) = (top.0, top.id());
        // both sides are packed keys, so the "is this candidate hopeless" test is one u64 compare
        if near.len() >= ef && cs < near.peek().map(|r| r.0 .0).unwrap_or(0) {
            break;
        }
        let mut n = 0usize;
        scorer.expanded(ci, 0);
        lvl.with_nbrs(pos, ci, |nbrs| {
            for &nb in nbrs.iter() {
                let nb = nb as u32;
                // see `greedy_on_level`: a mapped graph's links are range-checked here, not on
                // load
                if nb >= n_nodes || !seen.claim(nb) {
                    continue;
                }
                if n < MAX_DEG {
                    batch[n] = nb;
                    n += 1;
                }
            }
        });
        if n == 0 {
            continue;
        }
        scorer.score_many(&batch[..n], &mut sc[..n]);
        scored += n;
        for j in 0..n {
            let key = Sc::new(sc[j], batch[j]);
            if near.len() < ef || key.0 > near.peek().map(|r| r.0 .0).unwrap_or(0) {
                near.push(std::cmp::Reverse(key));
                if near.len() > ef {
                    near.pop();
                }
                cand.push(key);
                // the frontier's new members are the rows the next pops will read
                lvl.prefetch_row(pos, batch[j]);
            }
        }
        // ... and the next pop is the top of the frontier: fetch its neighbours' rows now, while
        // this expansion's arithmetic still has to finish.
        if ahead {
            if let Some(&next) = cand.peek() {
                scorer.expanded(next.id(), 1);
                lvl.with_nbrs(pos, next.id(), |r| scorer.prefetch(r));
            }
        }
    }
    out.clear();
    out.resize(near.len(), (0.0, 0));
    for i in (0..out.len()).rev() {
        let std::cmp::Reverse(key) = near.pop().unwrap();
        out[i] = (key.score(), key.id());
    }
    scored
}

struct Packing {
    bias: Vec<f32>,
    up_pos: Vec<i32>,
    rows: Vec<usize>,
    top: usize,
}

fn pack_levels(keys: &[f32], dim: usize, levels: &[u8]) -> Packing {
    let n = levels.len();
    let bias: Vec<f32> = (0..n)
        .map(|i| {
            let k = &keys[i * dim..(i + 1) * dim];
            (0.5 * k.iter().map(|&x| x as f64 * x as f64).sum::<f64>()) as f32
        })
        .collect();
    let mut upper: Vec<u32> = (0..n as u32).filter(|&i| levels[i as usize] >= 1).collect();
    upper.sort_by_key(|&i| (std::cmp::Reverse(levels[i as usize]), i));
    let mut up_pos = vec![-1i32; n];
    for (r, &i) in upper.iter().enumerate() {
        up_pos[i as usize] = r as i32;
    }
    let top = levels.iter().copied().max().unwrap_or(0) as usize;
    let rows = (1..=top)
        .map(|l| {
            upper
                .iter()
                .filter(|&&i| levels[i as usize] as usize >= l)
                .count()
        })
        .collect();
    Packing {
        bias,
        up_pos,
        rows,
        top,
    }
}

// ---------------------------------------------------------------------------------------------
// the parallel insertion (`--build-parallel N`)
// ---------------------------------------------------------------------------------------------

struct Ready(Vec<AtomicU64>);

impl Ready {
    fn new(n: usize) -> Ready {
        Ready((0..n.div_ceil(64)).map(|_| AtomicU64::new(0)).collect())
    }
    #[inline]
    fn get(&self, i: u32) -> bool {
        let i = i as usize;
        self.0[i / 64].load(Ordering::Acquire) & (1u64 << (i % 64)) != 0
    }
    #[inline]
    fn publish(&self, i: u32) {
        let i = i as usize;
        self.0[i / 64].fetch_or(1u64 << (i % 64), Ordering::Release);
    }
}

struct ParLvl {
    rows: Vec<RwLock<Vec<u32>>>,
    stride: usize,
    m: usize,
    ident: bool,
}

impl ParLvl {
    fn new(rows: usize, stride: usize, m: usize, ident: bool) -> ParLvl {
        ParLvl {
            rows: (0..rows)
                .map(|_| RwLock::new(Vec::with_capacity(stride)))
                .collect(),
            stride,
            m,
            ident,
        }
    }
    #[inline]
    fn row(&self, pos: &[i32], node: u32) -> usize {
        if self.ident {
            node as usize
        } else {
            pos[node as usize] as usize
        }
    }
    fn into_lvl(self) -> Lvl {
        let (stride, m, ident) = (self.stride, self.m, self.ident);
        let rows = self.rows.len();
        let mut links = vec![-1i32; rows * stride];
        let mut cnt = vec![0u32; rows];
        for (r, lock) in self.rows.into_iter().enumerate() {
            let row = lock.into_inner().unwrap_or_else(|e| e.into_inner());
            for (j, &id) in row.iter().take(stride).enumerate() {
                links[r * stride + j] = id as i32;
            }
            cnt[r] = row.len().min(stride) as u32;
        }
        Lvl {
            links: Arr::from_vec(links),
            cnt: Arr::from_vec(cnt),
            stride,
            m,
            ident,
        }
    }
}

struct ParView<'a> {
    lvl: &'a ParLvl,
    ready: &'a Ready,
}

impl Adj for ParView<'_> {
    fn with_nbrs<R>(&self, pos: &[i32], node: u32, f: impl FnOnce(&[i32]) -> R) -> R {
        let mut buf = [0i32; MAX_DEG];
        let mut n = 0usize;
        {
            let row = self.lvl.rows[self.lvl.row(pos, node)]
                .read()
                .unwrap_or_else(|e| e.into_inner());
            for &l in row.iter() {
                if n < MAX_DEG && self.ready.get(l) {
                    buf[n] = l as i32;
                    n += 1;
                }
            }
            // the lock dies HERE, before `f` scores anything (Qdrant's discipline: never hold a
            // link lock across a distance computation)
        }
        f(&buf[..n])
    }
    #[inline]
    fn prefetch_row(&self, _pos: &[i32], _node: u32) {}
}

#[allow(clippy::too_many_arguments)]
fn connect_row(
    row: &mut Vec<u32>,
    m: usize,
    target: u32,
    new_id: u32,
    keys: &[f32],
    dim: usize,
    bias: &[f32],
    min_links: usize,
    buf: &mut ConnBuf,
) {
    if row.len() < m {
        row.push(new_id);
        return;
    }
    let ConnBuf {
        cands,
        ids,
        dots,
        out,
        sel,
    } = buf;
    ids.clear();
    ids.extend_from_slice(row);
    ids.push(new_id);
    dots.clear();
    dots.resize(ids.len(), 0.0);
    let q = &keys[target as usize * dim..(target as usize + 1) * dim];
    dots_into(q, keys, dim, ids, dots);
    cands.clear();
    cands.extend(
        ids.iter()
            .zip(dots.iter())
            .map(|(&id, &d)| (d - bias[id as usize], id)),
    );
    cands.sort_by(|a, b| b.0.total_cmp(&a.0));
    heuristic_select_with(sel, cands, keys, dim, bias, m, target, min_links, out);
    row.clear();
    row.extend_from_slice(out);
}

struct Scratch {
    mark: Vec<u16>,
    epoch: u16,
    found: Vec<(f32, u32)>,
    sel: Vec<u32>,
    selbuf: SelBuf,
    conn: ConnBuf,
}

impl Scratch {
    fn new(n: usize) -> Scratch {
        Scratch {
            mark: vec![0u16; n],
            epoch: 0,
            found: Vec::new(),
            sel: Vec::new(),
            selbuf: SelBuf::default(),
            conn: ConnBuf::default(),
        }
    }
    fn next_epoch(&mut self) -> u16 {
        self.epoch += 1;
        if self.epoch >= MAX_EPOCH {
            self.mark.iter_mut().for_each(|x| *x = 0);
            self.epoch = 1;
        }
        self.epoch
    }
}

struct ParGraph<'a> {
    keys: &'a [f32],
    dim: usize,
    params: IndexParams,
    bias: &'a [f32],
    levels: &'a [u8],
    up_pos: &'a [i32],
    l0: ParLvl,
    up: Vec<ParLvl>,
    ready: Ready,
    entry: Mutex<(i64, usize)>,
    n: u32,
}

impl ParGraph<'_> {
    fn insert(&self, pid: u32, sc: &mut Scratch) {
        let dim = self.dim;
        let lv = self.levels[pid as usize] as usize;
        let q = &self.keys[pid as usize * dim..(pid as usize + 1) * dim];
        // ---- the entry point: copy it out, then let go of the lock -------------------------
        let (entry, entry_level) = {
            let mut g = self.entry.lock().unwrap_or_else(|e| e.into_inner());
            if g.0 < 0 {
                // the very first point of the head; nothing to link to
                *g = (pid as i64, lv);
                drop(g);
                self.ready.publish(pid);
                return;
            }
            (g.0 as u32, g.1)
        };
        let mut score = BuildScorer {
            keys: self.keys,
            bias: self.bias,
            dim,
            q,
        };
        let mut cur = entry;
        let mut cs = score.one(cur);
        for l in ((lv + 1)..=entry_level).rev() {
            let view = ParView {
                lvl: &self.up[l - 1],
                ready: &self.ready,
            };
            let (c, s, _) = greedy_on_level(&view, self.up_pos, &mut score, None, cur, cs, self.n);
            cur = c;
            cs = s;
        }
        for l in (0..=lv.min(entry_level)).rev() {
            let epoch = sc.next_epoch();
            let lvl: &ParLvl = if l == 0 { &self.l0 } else { &self.up[l - 1] };
            let view = ParView {
                lvl,
                ready: &self.ready,
            };
            beam_on_level(
                &view,
                self.up_pos,
                &mut score,
                &[(cs, cur)],
                self.params.ef_construct,
                &mut Visited::new(&mut sc.mark, epoch),
                &mut sc.found,
                self.n,
            );
            if sc.found.is_empty() {
                continue;
            }
            cur = sc.found[0].1;
            cs = sc.found[0].0;
            let m = if l == 0 {
                self.params.m0
            } else {
                self.params.m
            };
            heuristic_select_with(
                &mut sc.selbuf,
                &sc.found,
                self.keys,
                dim,
                self.bias,
                m,
                pid,
                self.params.min_links,
                &mut sc.sel,
            );
            // ---- own row: one write lock, filled, released --------------------------------
            {
                let mut row = lvl.rows[lvl.row(self.up_pos, pid)]
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                row.clear();
                row.extend_from_slice(&sc.sel);
            }
            // ---- reverse links: one neighbour's lock at a time, never two --------------
            for &e in sc.sel.iter() {
                let mut row = lvl.rows[lvl.row(self.up_pos, e)]
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                connect_row(
                    &mut row,
                    m,
                    e,
                    pid,
                    self.keys,
                    dim,
                    self.bias,
                    self.params.min_links,
                    &mut sc.conn,
                );
            }
        }
        // ---- publish, then offer the point as an entry point ---------------------------------
        self.ready.publish(pid);
        if lv > entry_level {
            let mut g = self.entry.lock().unwrap_or_else(|e| e.into_inner());
            if lv > g.1 {
                *g = (pid as i64, lv);
            }
        }
    }
}

impl HeadIndex {
    pub fn build(keys: &[f32], dim: usize, params: IndexParams, seed: u64) -> HeadIndex {
        HeadIndex::build_par(keys, dim, params, seed, 1)
    }

    pub fn build_par(
        keys: &[f32],
        dim: usize,
        params: IndexParams,
        seed: u64,
        threads: usize,
    ) -> HeadIndex {
        let n = keys.len() / dim;
        let levels = random_levels(n, params.m, seed);
        let order = qdrant_insertion_order(n, SEQ_HEAD, BUILD_BLOCKS);
        if threads <= 1 || n <= SEQ_HEAD {
            return HeadIndex::build_with(keys, dim, params, levels, &order);
        }
        HeadIndex::build_with_par(keys, dim, params, levels, &order, threads)
    }

    pub fn build_with_par(
        keys: &[f32],
        dim: usize,
        params: IndexParams,
        levels: Vec<u8>,
        order: &[u32],
        threads: usize,
    ) -> HeadIndex {
        let n = keys.len() / dim;
        assert_eq!(levels.len(), n);
        let Packing {
            bias,
            up_pos,
            rows,
            top,
        } = pack_levels(keys, dim, &levels);
        let g = ParGraph {
            keys,
            dim,
            params,
            bias: &bias,
            levels: &levels,
            up_pos: &up_pos,
            l0: ParLvl::new(n, params.m0 + params.cooc_links, params.m0, true),
            up: rows
                .iter()
                .map(|&r| ParLvl::new(r, params.m, params.m, false))
                .collect(),
            ready: Ready::new(n),
            entry: Mutex::new((-1, 0)),
            n: n as u32,
        };
        let _ = top;
        // ---- the sequential head: one worker, the real order ---------------------------------
        let head = SEQ_HEAD.min(order.len());
        {
            let mut sc = Scratch::new(n);
            for &pid in &order[..head] {
                g.insert(pid, &mut sc);
            }
        }
        // ---- the tail: `threads` workers pulling the emulated order, one point at a time -----
        let cursor = AtomicUsize::new(head);
        let tail = order;
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    let mut sc = Scratch::new(n);
                    loop {
                        let i = cursor.fetch_add(1, Ordering::Relaxed);
                        if i >= tail.len() {
                            break;
                        }
                        g.insert(tail[i], &mut sc);
                    }
                });
            }
        });
        let (entry, entry_level) = *g.entry.lock().unwrap();
        let ParGraph { l0, up, .. } = g;
        HeadIndex {
            n: n as u32,
            dim,
            params,
            levels: Arr::from_vec(levels),
            entry: entry.max(0) as u32,
            entry_level,
            miss_edges: 0,
            degrees0: 0,
            miss_got: Vec::new(),
            l0: l0.into_lvl(),
            l0_csr: None,
            up: up.into_iter().map(|l| l.into_lvl()).collect(),
            up_pos: Arr::from_vec(up_pos),
        }
    }

    pub fn build_with(
        keys: &[f32],
        dim: usize,
        params: IndexParams,
        levels: Vec<u8>,
        order: &[u32],
    ) -> HeadIndex {
        let n = keys.len() / dim;
        assert_eq!(levels.len(), n);
        let Packing {
            bias,
            up_pos,
            rows,
            top,
        } = pack_levels(keys, dim, &levels);
        let mut up: Vec<Lvl> = rows
            .iter()
            .map(|&r| Lvl::new(r, params.m, params.m, false))
            .collect();
        let _ = top;
        let mut l0 = Lvl::new(n, params.m0 + params.cooc_links, params.m0, true);

        let mut mark = vec![0u16; n];
        let mut epoch = 0u16;
        let mut entry: i64 = -1;
        let mut entry_level = 0usize;
        let mut found: Vec<(f32, u32)> = Vec::new();
        let mut sel: Vec<u32> = Vec::new();
        let mut selbuf = SelBuf::default();
        let mut conn = ConnBuf::default();
        let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];

        let mut t_beam = 0.0f64;
        let mut t_heur = 0.0f64;
        let mut t_conn = 0.0f64;
        let trace = std::env::var("KVSTORE_MEM_TRACE").is_ok();
        for &pid in order {
            let lv = levels[pid as usize] as usize;
            let q = key(pid);
            if entry < 0 {
                entry = pid as i64;
                entry_level = lv;
                continue;
            }
            let mut score = BuildScorer {
                keys,
                bias: &bias,
                dim,
                q,
            };
            let mut cur = entry as u32;
            let mut cs = score.one(cur);
            // greedy descent above the point's level
            for l in ((lv + 1)..=entry_level).rev() {
                let (c, s, _) =
                    greedy_on_level(&up[l - 1], &up_pos, &mut score, None, cur, cs, n as u32);
                cur = c;
                cs = s;
            }
            for l in (0..=lv.min(entry_level)).rev() {
                epoch += 1;
                if epoch >= MAX_EPOCH {
                    mark.iter_mut().for_each(|x| *x = 0);
                    epoch = 1;
                }
                let lvl: &Lvl = if l == 0 { &l0 } else { &up[l - 1] };
                let tb = std::time::Instant::now();
                beam_on_level(
                    lvl,
                    &up_pos,
                    &mut score,
                    &[(cs, cur)],
                    params.ef_construct,
                    &mut Visited::new(&mut mark, epoch),
                    &mut found,
                    n as u32,
                );
                t_beam += tb.elapsed().as_secs_f64();
                if found.is_empty() {
                    continue;
                }
                cur = found[0].1;
                cs = found[0].0;
                let m = if l == 0 { params.m0 } else { params.m };
                let th = std::time::Instant::now();
                heuristic_select_with(
                    &mut selbuf,
                    &found,
                    keys,
                    dim,
                    &bias,
                    m,
                    pid,
                    params.min_links,
                    &mut sel,
                );
                t_heur += th.elapsed().as_secs_f64();
                let lvl: &mut Lvl = if l == 0 { &mut l0 } else { &mut up[l - 1] };
                lvl.set(&up_pos, pid, &sel);
                let min_links = params.min_links;
                let tc = std::time::Instant::now();
                for &e in sel.iter() {
                    connect_with_heuristic(
                        lvl, &up_pos, e, pid, keys, dim, &bias, min_links, &mut conn,
                    );
                }
                t_conn += tc.elapsed().as_secs_f64();
            }
            if lv > entry_level {
                entry = pid as i64;
                entry_level = lv;
            }
        }

        if trace {
            eprintln!(
                "[build] n {n}: beam {t_beam:.2}s heuristic {t_heur:.2}s connect {t_conn:.2}s"
            );
        }
        HeadIndex {
            n: n as u32,
            dim,
            params,
            levels: Arr::from_vec(levels),
            entry: entry.max(0) as u32,
            entry_level,
            miss_edges: 0,
            degrees0: 0,
            miss_got: Vec::new(),
            l0,
            l0_csr: None,
            up,
            up_pos: Arr::from_vec(up_pos),
        }
    }

    pub fn links0(&self, node: u32) -> &[i32] {
        match &self.l0_csr {
            Some(csr) => csr.nbrs(&[], node),
            None => self.l0.nbrs(&[], node),
        }
    }

    pub fn freeze(&mut self) {
        if self.l0_csr.is_some() {
            return;
        }
        let csr = Csr::from_level(&self.l0, self.n as usize);
        self.degrees0 = self.l0.cnt.iter().map(|&c| c as u64).sum();
        self.l0.links = Arr::empty();
        self.l0.cnt = Arr::empty();
        self.miss_got = Vec::new();
        self.l0_csr = Some(csr);
    }

    pub fn stored_parts(&self) -> Option<GraphParts<'_>> {
        let csr = self.l0_csr.as_ref()?;
        let (l0_offs, l0_links) = csr.raw();
        Some(GraphParts {
            n: self.n,
            dim: self.dim,
            params: self.params,
            entry: self.entry,
            entry_level: self.entry_level,
            miss_edges: self.miss_edges,
            degrees0: self.degrees0,
            levels: &self.levels,
            up_pos: &self.up_pos,
            l0_offs,
            l0_links,
            up: self.up.iter().map(|l| l.raw()).collect(),
        })
    }

    pub fn from_stored(g: StoredGraph) -> Result<HeadIndex, String> {
        Self::restore_graph(g, false)
    }

    pub(crate) fn from_prepared(g: StoredGraph) -> Result<HeadIndex, String> {
        Self::restore_graph(g, true)
    }

    fn restore_graph(g: StoredGraph, prepared: bool) -> Result<HeadIndex, String> {
        let n = g.n as usize;
        if g.levels.len() != n || g.up_pos.len() != n {
            return Err(format!(
                "graph: {} levels and {} positions for {n} nodes",
                g.levels.len(),
                g.up_pos.len()
            ));
        }
        if g.l0_offs.len() != n + 1 {
            return Err(format!(
                "graph: {} level-0 offsets for {n} nodes",
                g.l0_offs.len()
            ));
        }
        if !prepared {
            let mut prev = 0u32;
            for (i, &o) in g.l0_offs.iter().enumerate() {
                if o < prev {
                    return Err(format!(
                        "graph: level-0 offset {i} goes backwards ({o} < {prev})"
                    ));
                }
                prev = o;
            }
            if prev as usize != g.l0_links.len() {
                return Err(format!(
                    "graph: level-0 offsets end at {prev} but there are {} links",
                    g.l0_links.len()
                ));
            }
        }
        if !g.trust_l0 {
            for &l in g.l0_links.iter() {
                if l < 0 || l as usize >= n {
                    return Err(format!("graph: level-0 link {l} out of range (n = {n})"));
                }
            }
        }
        if n > 0 && g.entry as usize >= n {
            return Err(format!(
                "graph: entry point {} out of range (n = {n})",
                g.entry
            ));
        }
        let mut up = Vec::with_capacity(g.up.len());
        for (l, (links, cnt)) in g.up.into_iter().enumerate() {
            let stride = g.params.m;
            if stride == 0 || links.len() != cnt.len() * stride {
                return Err(format!(
                    "graph: level {} has {} links for {} rows of width {stride}",
                    l + 1,
                    links.len(),
                    cnt.len()
                ));
            }
            if !prepared {
                for (r, &c) in cnt.iter().enumerate() {
                    if c as usize > stride {
                        return Err(format!(
                            "graph: level {} row {r} holds {c} > {stride} links",
                            l + 1
                        ));
                    }
                }
                for &x in links.iter() {
                    if x >= n as i32 {
                        return Err(format!(
                            "graph: level {} link {x} out of range (n = {n})",
                            l + 1
                        ));
                    }
                }
            }
            up.push(Lvl {
                links,
                cnt,
                stride,
                m: g.params.m,
                ident: false,
            });
        }
        Ok(HeadIndex {
            n: g.n,
            dim: g.dim,
            params: g.params,
            levels: g.levels,
            entry: g.entry,
            entry_level: g.entry_level,
            miss_edges: g.miss_edges,
            degrees0: g.degrees0,
            miss_got: Vec::new(),
            // the dense build-time level 0 stays empty: a loaded graph is read-only
            l0: Lvl::new(0, g.params.m0 + g.params.cooc_links, g.params.m0, true),
            l0_csr: Some(Csr::from_raw(g.l0_offs, g.l0_links)),
            up,
            up_pos: g.up_pos,
        })
    }

    pub fn mean_degree0(&self) -> f64 {
        if self.n == 0 {
            return 0.0;
        }
        if !self.l0.cnt.is_empty() {
            return self.l0.cnt.iter().map(|&c| c as f64).sum::<f64>() / self.l0.cnt.len() as f64;
        }
        self.degrees0 as f64 / self.n as f64
    }

    pub fn bytes(&self) -> u64 {
        let mut b = self.levels.len() as u64 + self.up_pos.len() as u64 * 4;
        b += (self.l0.links.len() * 4 + self.l0.cnt.len() * 4) as u64;
        b += self.l0_csr.as_ref().map(|c| c.bytes()).unwrap_or(0);
        for l in &self.up {
            b += (l.links.len() * 4 + l.cnt.len() * 4) as u64;
        }
        b + std::mem::size_of::<HeadIndex>() as u64
    }

    pub fn heap_bytes(&self) -> u64 {
        let mut b = self.levels.heap_bytes() as u64 + self.up_pos.heap_bytes() as u64;
        b += self.l0.heap_bytes();
        b += self.l0_csr.as_ref().map(|c| c.heap_bytes()).unwrap_or(0);
        for l in &self.up {
            b += l.heap_bytes();
        }
        b + std::mem::size_of::<HeadIndex>() as u64
    }

    pub fn is_mapped(&self) -> bool {
        self.l0_csr.as_ref().map(|c| c.is_mapped()).unwrap_or(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search<S: Scorer>(
        &self,
        scorer: &mut S,
        ef: usize,
        k: usize,
        state: &mut [u16],
        epoch: u16,
        out: &mut Vec<(f32, u32)>,
    ) -> usize {
        if self.n == 0 {
            out.clear();
            return 0;
        }
        // `Visited` counts DISTINCT scored keys (`keys_scored`) as it goes: the greedy descent has
        // no visited set of its own, so it can revisit a node, and the beam can rescore a node the
        // descent already looked at.
        let mut seen = Visited::new(state, epoch);
        let mut cur = self.entry;
        let mut cs = [0.0f32; 1];
        seen.touch(cur);
        scorer.score_many(&[cur], &mut cs);
        let mut cs = cs[0];
        for l in (1..=self.entry_level).rev() {
            let (c, s, _) = greedy_on_level(
                &self.up[l - 1],
                &self.up_pos,
                scorer,
                Some(&mut seen),
                cur,
                cs,
                self.n,
            );
            cur = c;
            cs = s;
        }
        self.beam0(scorer, &[(cs, cur)], ef, k, &mut seen, out);
        seen.distinct
    }

    #[allow(clippy::too_many_arguments)]
    fn beam0<S: Scorer>(
        &self,
        scorer: &mut S,
        entries: &[(f32, u32)],
        ef: usize,
        k: usize,
        seen: &mut Visited<'_>,
        out: &mut Vec<(f32, u32)>,
    ) -> usize {
        let width = ef.max(k).max(1);
        match &self.l0_csr {
            Some(csr) => beam_on_level(csr, &[], scorer, entries, width, seen, out, self.n),
            None => beam_on_level(&self.l0, &[], scorer, entries, width, seen, out, self.n),
        }
    }

    pub fn l0_degree(&self, node: u32) -> usize {
        self.l0.cnt.get(node as usize).copied().unwrap_or(0) as usize
    }

    #[allow(clippy::too_many_arguments)]
    pub fn miss_pairs_for_query<S: Scorer>(
        &self,
        scorer: &mut S,
        truth: &[u32],
        last: u32,
        ef: usize,
        keys: &[f32],
        dim: usize,
        state: &mut [u16],
        epoch: u16,
        sc: &mut MissScratch,
    ) -> usize {
        let k = truth.len();
        if k == 0 || self.n == 0 {
            return 0;
        }
        self.search(scorer, ef, k, state, epoch, &mut sc.out);
        // the beam's top-k, in its order, with the ineligible keys masked out
        sc.ret.clear();
        sc.ret.extend(
            sc.out
                .iter()
                .take(k)
                .map(|&(_, id)| id)
                .filter(|&id| id <= last),
        );
        if sc.ret.is_empty() {
            return 0;
        }
        let mut misses = 0usize;
        for &m in truth {
            if m >= self.n || sc.ret.contains(&m) {
                continue;
            }
            // the returned key most similar to the missed one is the link's source
            sc.dots.clear();
            sc.dots.resize(sc.ret.len(), 0.0);
            let km = &keys[m as usize * dim..(m as usize + 1) * dim];
            dots_into(km, keys, dim, &sc.ret, &mut sc.dots);
            let mut best = 0usize;
            for j in 1..sc.dots.len() {
                if sc.dots[j] > sc.dots[best] {
                    best = j;
                }
            }
            sc.pairs.push(((sc.ret[best] as u64) << 32) | m as u64);
            misses += 1;
        }
        misses
    }

    pub fn add_miss_edges(&mut self, pairs: &mut Vec<u64>, cap_new: usize) -> u64 {
        assert!(self.l0_csr.is_none(), "add_miss_edges after freeze");
        let n = self.n as usize;
        let cap = self.l0.stride;
        if cap <= self.params.m0 || pairs.is_empty() || n == 0 {
            return 0;
        }
        // (a, b) multiset -> (a, count, b), by one sort of the packed keys and a run-length count
        pairs.par_sort_unstable();
        let mut agg: Vec<(u32, u32, u32)> = Vec::new();
        let mut i = 0usize;
        while i < pairs.len() {
            let p = pairs[i];
            let mut j = i + 1;
            while j < pairs.len() && pairs[j] == p {
                j += 1;
            }
            agg.push(((p >> 32) as u32, (j - i) as u32, p as u32));
            i = j;
        }
        // (a asc, count desc, b desc)
        agg.sort_unstable_by(|x, y| x.0.cmp(&y.0).then(y.1.cmp(&x.1)).then(y.2.cmp(&x.2)));
        // the per-node budget of the cap, kept across calls (the reference's `got`)
        if cap_new > 0 && self.miss_got.len() != n {
            self.miss_got.resize(n, 0);
        }
        let got = self.miss_got.as_mut_slice();
        let links = self.l0.links.as_mut_slice();
        let cnt = self.l0.cnt.as_mut_slice();
        let mut added = 0u64;
        for &(a, _, b) in &agg {
            let (a, b) = (a as usize, b as usize);
            if a >= n || b >= n || a == b {
                continue;
            }
            let c = cnt[a] as usize;
            if c >= cap {
                continue;
            }
            if cap_new > 0 && got[a] as usize >= cap_new {
                continue;
            }
            let off = a * cap;
            if links[off..off + c].contains(&(b as i32)) {
                continue;
            }
            links[off + c] = b as i32;
            cnt[a] = c as u32 + 1;
            if cap_new > 0 {
                got[a] += 1;
            }
            added += 1;
        }
        self.miss_edges += added;
        added
    }
}

#[derive(Default)]
pub struct MissScratch {
    out: Vec<(f32, u32)>,
    ret: Vec<u32>,
    dots: Vec<f32>,
    pub pairs: Vec<u64>,
}

#[derive(Default)]
struct ConnBuf {
    cands: Vec<(f32, u32)>,
    ids: Vec<u32>,
    dots: Vec<f32>,
    out: Vec<u32>,
    sel: SelBuf,
}

#[allow(clippy::too_many_arguments)]
fn connect_with_heuristic(
    lvl: &mut Lvl,
    pos: &[i32],
    target: u32,
    new_id: u32,
    keys: &[f32],
    dim: usize,
    bias: &[f32],
    min_links: usize,
    buf: &mut ConnBuf,
) {
    let row = lvl.row(pos, target);
    let c = lvl.cnt[row] as usize;
    let m = lvl.m;
    if c < m {
        let stride = lvl.stride;
        lvl.links.as_mut_slice()[row * stride + c] = new_id as i32;
        lvl.cnt.as_mut_slice()[row] = c as u32 + 1;
        return;
    }
    let ConnBuf {
        cands,
        ids,
        dots,
        out,
        sel,
    } = buf;
    ids.clear();
    ids.extend(
        lvl.links[row * lvl.stride..row * lvl.stride + c]
            .iter()
            .map(|&l| l as u32),
    );
    ids.push(new_id);
    dots.clear();
    dots.resize(ids.len(), 0.0);
    let q = &keys[target as usize * dim..(target as usize + 1) * dim];
    dots_into(q, keys, dim, ids, dots);
    cands.clear();
    cands.extend(
        ids.iter()
            .zip(dots.iter())
            .map(|(&id, &d)| (d - bias[id as usize], id)),
    );
    // stable sort by score descending (numpy's argsort(-cs) is a stable sort on the negated key)
    cands.sort_by(|a, b| b.0.total_cmp(&a.0));
    heuristic_select_with(sel, cands, keys, dim, bias, m, target, min_links, out);
    lvl.set(pos, target, out);
}

// ---------------------------------------------------------------------------------------------
// per-thread visited buffer
// ---------------------------------------------------------------------------------------------

thread_local! {
    static SCRATCH: RefCell<(Vec<u16>, u16)> = const { RefCell::new((Vec::new(), 0)) };
}

pub fn with_scratch<R>(n: usize, f: impl FnOnce(&mut [u16], u16) -> R) -> R {
    SCRATCH.with(|cell| {
        let mut g = cell.borrow_mut();
        let (buf, epoch) = &mut *g;
        if buf.len() < n {
            buf.resize(n, 0);
        }
        *epoch += 1;
        if *epoch >= MAX_EPOCH {
            buf.iter_mut().for_each(|x| *x = 0);
            *epoch = 1;
        }
        let ep = *epoch;
        f(&mut buf[..n], ep)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- level assignment ------------------------------------------------------------------
    #[test]
    fn batched_dots_are_bit_identical() {
        let dim = 128;
        let mut rng = SplitMix64::new(7);
        let n = 37;
        let keys: Vec<f32> = (0..n * dim)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 3.0)
            .collect();
        let q: Vec<f32> = (0..dim)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 3.0)
            .collect();
        // every batch length from 0 to n: the 8/4/2/1 steps must all agree with `dot_f32`
        for len in 0..=n {
            let ids: Vec<u32> = (0..len as u32).collect();
            let mut got = vec![0.0f32; len];
            dots_into(&q, &keys, dim, &ids, &mut got);
            for (j, &id) in ids.iter().enumerate() {
                let want = dot_f32(&q, &keys[id as usize * dim..(id as usize + 1) * dim]);
                assert_eq!(
                    got[j].to_bits(),
                    want.to_bits(),
                    "len {len} slot {j}: {} vs {}",
                    got[j],
                    want
                );
            }
        }
    }

    #[test]
    fn the_memory_budget_bounds_the_heads_in_flight() {
        let p = IndexParams::default();
        let (n, dim) = (97_257u64, 128u64);
        let per = head_scratch_bytes(n, dim, 2740, &p);
        // the acceptance head measured 90 MB of scratch (docs/plan.md, night 4)
        assert!((80e6..110e6).contains(&(per as f64)), "{per}");
        // no budget: the requested ceiling, capped by the number of heads
        let plan = BuildPlan::resolve(8, 0, 256, n, dim, 2740, &p);
        assert_eq!(plan.parallel, 8);
        assert_eq!(BuildPlan::resolve(8, 0, 3, n, dim, 2740, &p).parallel, 3);
        // a budget of three heads' worth allows three
        let mb = (3 * per).div_ceil(1024 * 1024);
        assert_eq!(BuildPlan::resolve(8, mb, 256, n, dim, 2740, &p).parallel, 3);
        // and a budget smaller than one head still builds one
        assert_eq!(BuildPlan::resolve(8, 1, 256, n, dim, 2740, &p).parallel, 1);
        // auto = min(cores, 8)
        let auto = BuildPlan::resolve(0, 0, 256, n, dim, 2740, &p);
        assert_eq!(auto.parallel, rayon::current_num_threads().min(8).max(1));
    }

    #[test]
    fn level_rule_matches_qdrant_rounding() {
        // level = round(-ln u / ln M): u > 1/sqrt(M) -> 0, and the cut points are at M^-(l+1/2)
        let m = 16usize;
        assert_eq!(level_from_u(0.9, m), 0);
        assert_eq!(level_from_u(0.26, m), 0); // -ln/ln16 = 0.486 -> 0
        assert_eq!(level_from_u(0.24, m), 1); // 0.514 -> 1
                                              // the level-1/2 cut sits at 16^-1.5 = 0.015625
        assert_eq!(level_from_u(0.0157, m), 1);
        assert_eq!(level_from_u(0.0156, m), 2);
        assert_eq!(level_from_u(1e-300, m), LMAX); // clamped
        assert_eq!(level_from_u(0.0, m), LMAX); // u = 0 is clamped to tiny, not -inf
    }

    #[test]
    fn random_levels_are_geometric_in_m() {
        let n = 200_000;
        let lv = random_levels(n, 16, 1);
        let mut hist = [0usize; LMAX + 1];
        for &l in &lv {
            hist[l as usize] += 1;
        }
        // P(level >= 1) = M^-0.5 = 0.25, P(level >= 2) = M^-1.5 = 0.0156
        let ge1 = lv.iter().filter(|&&l| l >= 1).count() as f64 / n as f64;
        let ge2 = lv.iter().filter(|&&l| l >= 2).count() as f64 / n as f64;
        assert!((ge1 - 0.25).abs() < 0.01, "{ge1} {hist:?}");
        assert!((ge2 - 0.0156).abs() < 0.003, "{ge2} {hist:?}");
        // deterministic
        assert_eq!(lv, random_levels(n, 16, 1));
        assert_ne!(lv, random_levels(n, 16, 2));
    }

    #[test]
    fn insertion_order_matches_the_qdrant_emulation() {
        // the reference: 256 sequential ids, then np.array_split(arange(256, n), 16) advancing in
        // lockstep. Small case, head 2 and 3 blocks: rest = [2..9) -> [2,3,4], [5,6], [7,8]
        assert_eq!(
            qdrant_insertion_order(9, 2, 3),
            vec![0, 1, 2, 5, 7, 3, 6, 8, 4]
        );
        // every id exactly once, for a range of sizes
        for n in [1usize, 5, 256, 257, 1000, 97_257] {
            let o = qdrant_insertion_order(n, SEQ_HEAD, BUILD_BLOCKS);
            assert_eq!(o.len(), n);
            let mut s = o.clone();
            s.sort_unstable();
            assert!(
                s.iter().enumerate().all(|(i, &x)| x as usize == i),
                "n = {n}"
            );
            // the head is sequential
            let head = SEQ_HEAD.min(n);
            assert_eq!(o[..head], (0..head as u32).collect::<Vec<_>>()[..]);
        }
    }

    // ---- the heuristic ---------------------------------------------------------------------
    fn line_keys() -> (Vec<f32>, usize) {
        (vec![0.0, 1.0, 1.05], 1)
    }

    fn bias_of(keys: &[f32], dim: usize) -> Vec<f32> {
        (0..keys.len() / dim)
            .map(|i| {
                0.5 * keys[i * dim..(i + 1) * dim]
                    .iter()
                    .map(|x| x * x)
                    .sum::<f32>()
            })
            .collect()
    }

    fn cands_for(
        keys: &[f32],
        dim: usize,
        bias: &[f32],
        target: u32,
        ids: &[u32],
    ) -> Vec<(f32, u32)> {
        let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];
        let mut v: Vec<(f32, u32)> = ids
            .iter()
            .map(|&i| (dot_f32(key(target), key(i)) - bias[i as usize], i))
            .collect();
        v.sort_by(|a, b| b.0.total_cmp(&a.0));
        v
    }

    #[test]
    fn heuristic_prunes_the_near_duplicate() {
        let (keys, dim) = line_keys();
        let bias = bias_of(&keys, dim);
        let cands = cands_for(&keys, dim, &bias, 0, &[1, 2]);
        assert_eq!(cands[0].1, 1, "1 is the better candidate for 0");
        let mut out = Vec::new();
        // m = 2 but no min_links: the second candidate is redundant and dropped
        heuristic_select(&cands, &keys, dim, &bias, 2, 0, 0, &mut out);
        assert_eq!(out, vec![1]);
    }

    #[test]
    fn heuristic_min_links_refills_with_pruned_candidates() {
        let (keys, dim) = line_keys();
        let bias = bias_of(&keys, dim);
        let cands = cands_for(&keys, dim, &bias, 0, &[1, 2]);
        let mut out = Vec::new();
        // min_links = 2: the pruned near-duplicate comes back (keep-pruned-connections), which is
        // what keeps in-degree-0 keys reachable.
        heuristic_select(&cands, &keys, dim, &bias, 2, 0, 2, &mut out);
        assert_eq!(out, vec![1, 2]);
        // min_links is capped by m
        let mut out2 = Vec::new();
        heuristic_select(&cands, &keys, dim, &bias, 1, 0, 8, &mut out2);
        assert_eq!(out2, vec![1]);
    }

    #[test]
    fn heuristic_min_links_keeps_pruned_in_score_order() {
        // 0 at the origin, then a tight cluster far away: only the cluster head survives the
        // heuristic, the rest come back through min_links in candidate (score) order.
        let dim = 1;
        let keys: Vec<f32> = vec![0.0, 10.0, 10.1, 10.2, 10.3];
        let bias = bias_of(&keys, dim);
        let cands = cands_for(&keys, dim, &bias, 0, &[1, 2, 3, 4]);
        assert_eq!(
            cands.iter().map(|c| c.1).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let mut out = Vec::new();
        heuristic_select(&cands, &keys, dim, &bias, 4, 0, 3, &mut out);
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn heuristic_with_the_bias_is_exactly_the_l2_rule() {
        // The point of the bias: `score(c, e) > score(c, target)` with bias = |k|^2/2 must be the
        // same decision as `d(c, e) < d(c, target)`. Checked against a direct L2 implementation on
        // random points (this is what makes the build "the L2 heuristic" of REPORT.md 3).
        let (dim, n) = (5usize, 60usize);
        let mut rng = SplitMix64::new(42);
        let keys: Vec<f32> = (0..n * dim)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 4.0)
            .collect();
        let bias = bias_of(&keys, dim);
        let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];
        let d2 = |a: u32, b: u32| -> f32 {
            key(a)
                .iter()
                .zip(key(b))
                .map(|(x, y)| (x - y) * (x - y))
                .sum()
        };
        for target in [0u32, 7, 23, 59] {
            let ids: Vec<u32> = (0..n as u32).filter(|&i| i != target).collect();
            let cands = cands_for(&keys, dim, &bias, target, &ids);
            let mut out = Vec::new();
            heuristic_select(&cands, &keys, dim, &bias, 8, target, 0, &mut out);
            // reference: plain L2 heuristic over the same candidate order
            let mut want: Vec<u32> = Vec::new();
            for &(_, c) in &cands {
                if want.iter().all(|&e| d2(c, e) >= d2(c, target)) {
                    want.push(c);
                    if want.len() >= 8 {
                        break;
                    }
                }
            }
            assert_eq!(out, want, "target {target}");
        }
    }

    // ---- build + search --------------------------------------------------------------------
    fn grid_keys(side: usize) -> (Vec<f32>, usize) {
        let mut v = Vec::new();
        for i in 0..side {
            for j in 0..side {
                v.push(i as f32);
                v.push(j as f32);
            }
        }
        (v, 2)
    }

    #[test]
    fn build_produces_a_connected_reachable_graph() {
        let (keys, dim) = grid_keys(30); // 900 points
        let n = keys.len() / dim;
        let params = IndexParams {
            m: 8,
            m0: 16,
            ef_construct: 64,
            min_links: 4,
            cooc_links: 4,
        };
        let idx = HeadIndex::build(&keys, dim, params, 1);
        assert_eq!(idx.n as usize, n);
        // every node has links, and no link is out of range or a self-loop
        for i in 0..n as u32 {
            let l = idx.links0(i);
            assert!(!l.is_empty(), "node {i} has no level-0 links");
            assert!(l.len() <= params.m0);
            for &x in l {
                assert!(x >= 0 && (x as usize) < n);
                assert_ne!(x as u32, i);
            }
        }
        // reachability from the entry on level 0 (the diagnostic of REPORT.md 3)
        let mut seen = vec![false; n];
        let mut stack = vec![idx.entry];
        seen[idx.entry as usize] = true;
        while let Some(u) = stack.pop() {
            for &v in idx.links0(u) {
                if !seen[v as usize] {
                    seen[v as usize] = true;
                    stack.push(v as u32);
                }
            }
        }
        assert!(seen.iter().all(|&b| b), "level 0 is not fully reachable");
    }

    #[test]
    fn search_finds_the_exact_top_k_on_a_grid() {
        let (keys, dim) = grid_keys(30);
        let n = keys.len() / dim;
        let idx = HeadIndex::build(&keys, dim, IndexParams::default(), 1);
        let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];
        let mut hits = 0usize;
        let k = 10;
        // irrational-ish directions, so the dot products have no ties
        let queries = [
            [1.0f32, 0.618_034],
            [0.31, 1.0],
            [-1.0, 0.372_1],
            [0.77, -0.913],
        ];
        for q in queries {
            let mut exact: Vec<(f32, u32)> =
                (0..n as u32).map(|i| (dot_f32(&q, key(i)), i)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0));
            let mut out = Vec::new();
            with_scratch(n, |st, epoch| {
                idx.search(&mut |i| dot_f32(&q, key(i)), 64, k, st, epoch, &mut out)
            });
            let got: std::collections::HashSet<u32> = out.iter().take(k).map(|&(_, i)| i).collect();
            hits += exact[..k].iter().filter(|(_, i)| got.contains(i)).count();
        }
        // ef 64 >> k on a well-connected graph: recall must be perfect
        assert_eq!(hits, k * queries.len());
    }

    #[test]
    fn search_reports_the_number_of_distinct_scored_keys() {
        let (keys, dim) = grid_keys(20);
        let n = keys.len() / dim;
        let idx = HeadIndex::build(&keys, dim, IndexParams::default(), 3);
        let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];
        let q = [0.31f32, 1.0];
        let mut out = Vec::new();
        let mut touched = std::collections::HashSet::new();
        let scored = with_scratch(n, |st, epoch| {
            idx.search(
                &mut |i| {
                    touched.insert(i);
                    dot_f32(&q, key(i))
                },
                32,
                10,
                st,
                epoch,
                &mut out,
            )
        });
        // `keys_scored` counts distinct keys, even though the greedy descent has no visited set
        assert_eq!(scored, touched.len());
        assert!(scored > 10 && scored < n);
        assert!(out.windows(2).all(|w| w[0].0 >= w[1].0), "not best first");
        assert!(out.len() <= 32);
    }

    #[test]
    fn build_is_deterministic() {
        let (keys, dim) = grid_keys(16);
        let a = HeadIndex::build(&keys, dim, IndexParams::default(), 7);
        let b = HeadIndex::build(&keys, dim, IndexParams::default(), 7);
        assert_eq!(a.entry, b.entry);
        assert_eq!(&a.l0.links[..], &b.l0.links[..]);
        assert_eq!(&a.l0.cnt[..], &b.l0.cnt[..]);
    }

    #[test]
    fn a_parallel_build_answers_like_the_sequential_one() {
        let (keys, dim) = grid_keys(30); // 900 points, so the tail really is parallel
        let n = keys.len() / dim;
        let params = IndexParams {
            m: 8,
            m0: 16,
            ef_construct: 32,
            min_links: 4,
            cooc_links: 0,
        };
        let seq = HeadIndex::build_par(&keys, dim, params, 5, 1);
        let par = HeadIndex::build_par(&keys, dim, params, 5, 4);
        // the level assignment, the entry level and the node count are shape, not order
        assert_eq!(par.n, seq.n);
        assert_eq!(&par.levels[..], &seq.levels[..]);
        assert_eq!(par.entry_level, seq.entry_level);
        // every node has links, and no link is out of range or a self-loop
        for i in 0..n as u32 {
            let nb = par.links0(i);
            assert!(!nb.is_empty(), "node {i} has no level-0 links");
            assert!(nb.len() <= params.m0, "node {i} holds {} links", nb.len());
            assert!(nb
                .iter()
                .all(|&l| l >= 0 && (l as usize) < n && l != i as i32));
        }
        // the degree is in the same place (the heuristic is unchanged, only the order it sees is)
        let (ds, dp) = (seq.mean_degree0(), par.mean_degree0());
        assert!((ds - dp).abs() < 0.15 * ds, "mean degree {dp} vs {ds}");
        // ... and the answers agree
        let key = |i: u32| &keys[i as usize * dim..(i as usize + 1) * dim];
        let k = 10;
        let queries = [
            [1.0f32, 0.618_034],
            [0.31, 1.0],
            [-1.0, 0.372_1],
            [0.77, -0.913],
        ];
        for q in queries {
            let mut exact: Vec<(f32, u32)> =
                (0..n as u32).map(|i| (dot_f32(&q, key(i)), i)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0));
            for idx in [&seq, &par] {
                let mut out = Vec::new();
                with_scratch(n, |st, epoch| {
                    idx.search(&mut |i| dot_f32(&q, key(i)), 64, k, st, epoch, &mut out)
                });
                let got: std::collections::HashSet<u32> =
                    out.iter().take(k).map(|&(_, i)| i).collect();
                assert_eq!(
                    exact[..k].iter().filter(|(_, i)| got.contains(i)).count(),
                    k,
                    "recall@{k}"
                );
            }
        }
    }

    #[test]
    fn one_thread_is_the_sequential_build() {
        let (keys, dim) = grid_keys(16);
        let a = HeadIndex::build(&keys, dim, IndexParams::default(), 7);
        let b = HeadIndex::build_par(&keys, dim, IndexParams::default(), 7, 1);
        assert_eq!(&a.l0.links[..], &b.l0.links[..]);
        assert_eq!(&a.l0.cnt[..], &b.l0.cnt[..]);
        assert_eq!(a.entry, b.entry);
    }

    #[test]
    fn a_tiny_head_is_built_sequentially_whatever_the_flag_says() {
        let (keys, dim) = grid_keys(8); // 64 points < SEQ_HEAD
        let a = HeadIndex::build_par(&keys, dim, IndexParams::default(), 2, 1);
        let b = HeadIndex::build_par(&keys, dim, IndexParams::default(), 2, 8);
        assert_eq!(&a.l0.links[..], &b.l0.links[..]);
    }

    #[test]
    fn build_handles_tiny_inputs() {
        for n in 1..4usize {
            let keys: Vec<f32> = (0..n).map(|i| i as f32).collect();
            let idx = HeadIndex::build(&keys, 1, IndexParams::default(), 1);
            assert_eq!(idx.n as usize, n);
            let mut out = Vec::new();
            let scored = with_scratch(n, |st, ep| {
                idx.search(&mut |i| keys[i as usize] * 2.0, 16, 4, st, ep, &mut out)
            });
            assert!(scored >= 1 && !out.is_empty());
        }
    }

    // ---- miss-driven edges -------------------------------------------------------------------
    fn empty_graph(n: usize, m0: usize, extra: usize) -> HeadIndex {
        HeadIndex {
            n: n as u32,
            dim: 1,
            params: IndexParams {
                m: m0 / 2,
                m0,
                ef_construct: 10,
                min_links: 0,
                cooc_links: extra,
            },
            levels: Arr::from_vec(vec![0; n]),
            entry: 0,
            entry_level: 0,
            miss_edges: 0,
            degrees0: 0,
            miss_got: Vec::new(),
            l0: Lvl::new(n, m0 + extra, m0, true),
            l0_csr: None,
            up: Vec::new(),
            up_pos: Arr::from_vec(vec![-1; n]),
        }
    }

    fn set_l0(g: &mut HeadIndex, at: usize, link: i32, cnt_at: usize, cnt: u32) {
        g.l0.links.as_mut_slice()[at] = link;
        g.l0.cnt.as_mut_slice()[cnt_at] = cnt;
    }

    fn chain_with_an_island() -> (HeadIndex, Vec<f32>) {
        let keys = vec![0.0f32, 1.0, 2.0, 10.0];
        let mut g = empty_graph(4, 2, 1); // stride 3: room for one extra link per node
        set_l0(&mut g, 0, 1, 0, 1); // 0 -> 1
        set_l0(&mut g, 3, 2, 1, 1); // 1 -> 2 (row 1 starts at slot 3)
        (g, keys)
    }

    fn top2(g: &HeadIndex, keys: &[f32], q: f32) -> Vec<u32> {
        let mut out = Vec::new();
        with_scratch(g.n as usize, |st, ep| {
            g.search(&mut |i: u32| q * keys[i as usize], 4, 2, st, ep, &mut out)
        });
        out.iter().take(2).map(|&(_, i)| i).collect()
    }

    #[test]
    fn miss_rule_bridges_the_key_the_beam_cannot_reach() {
        let (mut g, keys) = chain_with_an_island();
        let q = 1.0f32;
        let truth = [3u32, 2]; // exact top-2 of q over every key
        assert_eq!(top2(&g, &keys, q), vec![2, 1], "the cold beam cannot see 3");

        let mut sc = MissScratch::default();
        let misses = with_scratch(4, |st, ep| {
            g.miss_pairs_for_query(
                &mut |i: u32| q * keys[i as usize],
                &truth,
                u32::MAX,
                4,
                &keys,
                1,
                st,
                ep,
                &mut sc,
            )
        });
        assert_eq!(misses, 1, "only 3 is missed; 2 was returned");
        // dot(k3, k2) = 20 > dot(k3, k1) = 10: the source is 2, the target the missed 3
        assert_eq!(sc.pairs, vec![(2u64 << 32) | 3]);

        let added = g.add_miss_edges(&mut sc.pairs, 0);
        assert_eq!((added, g.miss_edges), (1, 1));
        assert_eq!(g.links0(2), &[3], "the bridge 2 -> 3");
        assert_eq!(g.links0(0), &[1], "nothing else moved");
        assert_eq!(g.links0(1), &[2]);
        assert_eq!(g.links0(3), &[] as &[i32]);
        // the same single-entry beam now walks 0 -> 1 -> 2 -> 3
        assert_eq!(top2(&g, &keys, q), vec![3, 2]);

        // one round: running the rule again finds nothing to repair
        let mut again = MissScratch::default();
        let misses = with_scratch(4, |st, ep| {
            g.miss_pairs_for_query(
                &mut |i: u32| q * keys[i as usize],
                &truth,
                u32::MAX,
                4,
                &keys,
                1,
                st,
                ep,
                &mut again,
            )
        });
        assert_eq!(misses, 0);
        assert!(again.pairs.is_empty());
        assert_eq!(g.add_miss_edges(&mut again.pairs, 0), 0);
    }

    #[test]
    fn miss_rule_masks_the_keys_a_causal_query_cannot_see() {
        let (mut g, keys) = chain_with_an_island();
        let q = 1.0f32;
        // the query sits at position 1: it may see keys 0 and 1 only, and its exact top-2 among
        // them is [1, 0]. The beam still returns {2, 1}; masked, that is {1}, so 0 is missed and
        // its only possible source is 1.
        let truth = [1u32, 0];
        let mut sc = MissScratch::default();
        let misses = with_scratch(4, |st, ep| {
            g.miss_pairs_for_query(
                &mut |i: u32| q * keys[i as usize],
                &truth,
                1,
                4,
                &keys,
                1,
                st,
                ep,
                &mut sc,
            )
        });
        assert_eq!(misses, 1);
        assert_eq!(
            sc.pairs,
            vec![(1u64 << 32)],
            "1 -> 0, and never from the ineligible 2"
        );
        assert_eq!(g.add_miss_edges(&mut sc.pairs, 0), 1);
        assert_eq!(g.links0(1), &[2, 0], "the existing link stays first");
        assert_eq!(
            g.links0(2),
            &[] as &[i32],
            "2 was masked out: it is not a source"
        );

        // a query that may see nothing the beam returned contributes nothing
        let mut none = MissScratch::default();
        let misses = with_scratch(4, |st, ep| {
            g.miss_pairs_for_query(
                &mut |i: u32| q * keys[i as usize],
                &[0u32],
                0,
                4,
                &keys,
                1,
                st,
                ep,
                &mut none,
            )
        });
        assert_eq!((misses, none.pairs.len()), (0, 0));
    }

    #[test]
    fn miss_edges_go_in_count_then_id_order_and_respect_capacity() {
        let mut g = empty_graph(6, 1, 2); // capacity 3 per row
        set_l0(&mut g, 0, 1, 0, 1); // node 0 already links to 1
        let pair = |a: u32, b: u32| ((a as u64) << 32) | b as u64;
        let mut pairs = vec![
            pair(0, 5),
            pair(0, 2),
            pair(0, 3),
            pair(0, 5),
            pair(0, 4),
            pair(0, 2),
            pair(0, 5),
            pair(0, 1), // a duplicate of the existing link
            pair(0, 0), // a self-loop
            pair(3, 0),
        ];
        let added = g.add_miss_edges(&mut pairs, 0);
        // 5 (x3) first, then 2 (x2), then the row is full: 4, 3 (x1 each, id desc) never fit
        assert_eq!(g.links0(0), &[1, 5, 2]);
        assert_eq!(g.links0(3), &[0]);
        assert_eq!((added, g.miss_edges), (3, 3));

        // no candidates, or no slots: a no-op
        assert_eq!(g.add_miss_edges(&mut Vec::new(), 0), 0);
        let mut g2 = empty_graph(4, 2, 0);
        assert_eq!(g2.add_miss_edges(&mut vec![pair(0, 1)], 0), 0);
        assert_eq!(g2.links0(0), &[] as &[i32]);
    }

    #[test]
    fn miss_edges_respect_the_per_node_cap() {
        let pair = |a: u32, b: u32| ((a as u64) << 32) | b as u64;
        // ten distinct, valid, single-count candidates for node 0: (count desc, id desc) puts
        // them in id order 10, 9, 8, ...
        let cands = || (1..=10u32).map(|b| pair(0, b)).collect::<Vec<u64>>();

        // room for sixteen links per row, so nothing but the cap can bind
        let mut capped = empty_graph(11, 8, 8);
        assert_eq!(capped.add_miss_edges(&mut cands(), 4), 4);
        assert_eq!(capped.links0(0), &[10, 9, 8, 7]);
        assert_eq!(capped.miss_edges, 4);
        // the budget is per node and carried across calls: a second round adds nothing
        assert_eq!(capped.add_miss_edges(&mut cands(), 4), 0);
        assert_eq!(capped.links0(0).len(), 4);

        // cap 0 = unlimited: all ten go in
        let mut unlimited = empty_graph(11, 8, 8);
        assert_eq!(unlimited.add_miss_edges(&mut cands(), 0), 10);
        assert_eq!(unlimited.links0(0).len(), 10);
        assert_eq!(unlimited.miss_edges, 10);

        // capacity still wins when it is the tighter of the two (stride 2, so one free slot
        // after the m0 one, and the cap of 4 never gets to bind)
        let mut narrow = empty_graph(11, 1, 1);
        assert_eq!(narrow.add_miss_edges(&mut cands(), 4), 2);
        assert_eq!(narrow.links0(0), &[10, 9]);
    }
}
