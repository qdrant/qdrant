use std::cell::RefCell;
use std::collections::BinaryHeap;

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
    /// Uniform in `[0, 1)` with 53 significant bits.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut s = 0.0f32;
    for i in 0..a.len() {
        s += a[i] * b[i];
    }
    s
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Sc(u64);

/// f32 -> order-preserving u32 (flip the sign bit for positives, invert everything for negatives).
#[inline(always)]
fn ordered_bits(x: f32) -> u32 {
    let b = x.to_bits();
    if b >> 31 == 1 {
        !b
    } else {
        b | 0x8000_0000
    }
}

/// The inverse of [`ordered_bits`].
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

/// Largest level-0 row width the beam batches in one go (`m0 + cooc_links`).
pub const MAX_DEG: usize = 128;

/// The beam's visited set: one epoch-tagged `u16` per node, holding BOTH bits of state the search
/// needs — "already scored in this search" (for `keys_scored`) and "already claimed for scoring by the beam"
/// — as `epoch << 1 | beam_claimed`.
///
/// Every scored key touches this array once, at a random index, so its size is part of the beam's
/// cost. The first version used two separate `u32` arrays (one visited set, one distinct-key
/// counter): 780 KB per head at 97k keys, two random accesses per key, competing with the codes
/// for L2. One `u16` array is 195 KB — small enough to stay in L2 next to everything else — and
/// one access. The price is a 15-bit epoch, so the buffer is wiped before that epoch wraps
/// (195 KB of `memset`, i.e. ~12 ns amortised per search).
pub struct Visited<'a> {
    st: &'a mut [u16],
    ep: u16,
    /// Distinct keys scored in this search (`SearchResponse.keys_scored`).
    pub distinct: usize,
}

/// Epochs above this wrap and the visited buffer is wiped (see [`Visited`]).
pub const MAX_EPOCH: u16 = (1 << 15) - 1;

impl<'a> Visited<'a> {
    /// `epoch` must be in `1..=MAX_EPOCH` (see [`with_scratch`]).
    pub fn new(st: &'a mut [u16], epoch: u16) -> Visited<'a> {
        debug_assert!((1..=MAX_EPOCH).contains(&epoch));
        Visited {
            st,
            ep: epoch,
            distinct: 0,
        }
    }

    /// Claim `i` for the beam: `false` if it was already scored by the beam in this search.
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

    /// Count a score taken outside the beam (the greedy descent, which has no visited set of its
    /// own and may revisit a node).
    #[inline]
    fn touch(&mut self, i: u32) {
        let cur = unsafe { *self.st.get_unchecked(i as usize) };
        if (cur >> 1) != self.ep {
            unsafe { *self.st.get_unchecked_mut(i as usize) = self.ep << 1 };
            self.distinct += 1;
        }
    }
}

/// A source of key scores for the graph walk.
///
/// The beam hands over a whole neighbour list at once, which is what lets a quantised scorer
/// issue all of its record loads before doing any arithmetic (one round of parallel cache misses
/// per expansion instead of one dependent miss per key) — see [`crate::nodes::NodeRecords::score`].
/// Any `FnMut(u32) -> f32` is a `Scorer`, so the build and the exact path can stay closures.
pub trait Scorer {
    /// Score every id in `ids` into `out` (same length).
    fn score_many(&mut self, ids: &[u32], out: &mut [f32]);

    /// "The adjacency list of this node is being read."
    ///
    /// A page-layout study needs to separate the record pages a query touches from the graph
    /// pages it touches, and only the traversal knows which nodes were expanded. `kind` is 0 for
    /// an expansion the beam or the greedy descent actually needed and 1 for a lookahead read
    /// (`prefetch` below), which reads an adjacency list that may never be used. Default: nothing,
    /// so no serving path pays for it.
    #[inline]
    fn expanded(&mut self, _node: u32, _kind: u8) {}

    /// "These keys will very probably be scored next" — pull their rows towards L1.
    ///
    /// The beam calls this with the neighbour list of the node at the top of its frontier heap,
    /// i.e. the node it is about to pop, one expansion before it needs the data. `night_e1`
    /// measured that the beam's next target is predictable 81-95 % of the time, and a key's row
    /// is 1-4 cache lines away in DRAM, so this is where the memory latency of a graph walk goes
    /// to hide. Set `KVSTORE_NO_LOOKAHEAD=1` to switch it off (the A/B in `bench/`).
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

/// Whether the beam issues the one-expansion-ahead prefetch (see [`Scorer::prefetch`]).
pub fn lookahead() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("KVSTORE_NO_LOOKAHEAD").is_err())
}

/// What the walk needs of a level: a node's neighbour list, and a prefetch of it.
pub trait Adj {
    /// Hand `node`'s neighbour row to `f`.
    ///
    /// It is a closure and not a returned slice because a row is not always *borrowable*: the
    /// flat levels and the frozen CSR hand out a window into their own array (no copy, inlined
    /// away), while the parallel build's rows live behind per-node locks and are copied out
    /// under a read lock — which is the whole discipline that makes a concurrent build safe
    /// (Qdrant's `for_each_link`: collect the ids, drop the lock, *then* score them; no lock is
    /// ever held across a distance computation, and never two at a time).
    fn with_nbrs<R>(&self, pos: &[i32], node: u32, f: impl FnOnce(&[i32]) -> R) -> R;
    fn prefetch_row(&self, pos: &[i32], node: u32);
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

/// Qdrant's beam search on one level (`hnsw_sim.search_on_level`).
///
/// A node is marked visited when it is *scored* (Qdrant's `visited == scored`). `out` receives the
/// `min(ef, scored)` best `(score, id)` pairs, best first. Returns the number of scored nodes.
///
/// The one difference from the reference is **how** the scores are obtained: the unvisited
/// neighbours of the expanded node are collected first and scored as one batch, then the heap
/// updates run over them in the original neighbour order — so the visited set, the frontier and
/// the answer are bit-identical to scoring them one at a time, while the scorer gets to overlap
/// all of the batch's memory reads. The link rows of the nodes that enter the frontier are
/// prefetched, since those are exactly the rows the next pops will read.
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

thread_local! {
    static SCRATCH: RefCell<(Vec<u16>, u16)> = const { RefCell::new((Vec::new(), 0)) };
}

/// Run `f` with one per-thread epoch-marked buffer of `n` slots (the beam's [`Visited`] state)
/// plus a fresh epoch, so a search never allocates or zeroes `n` words.
///
/// Epochs increase monotonically per thread, so stale marks left by another head (a different `n`)
/// can never match the current epoch. The state word is `epoch << 1 | beam_claimed`, so the epoch is
/// capped at [`MAX_EPOCH`] and the buffer is wiped when it wraps.
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
