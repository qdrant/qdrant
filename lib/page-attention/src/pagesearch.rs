//! Page beam, coarse tail and optional original-row rescoring.
use crate::index::{self, Adj, Scorer, Visited};
use crate::pagekernel::{attend_page, score_page, PageQuery, VAcc, VLevels, WeightBits};
use crate::pages::{PageCsr, PagesHead, MAX_ROW};
use crate::tq4::Rotation;
#[cfg(test)]
use crate::{pages::TAIL_BLOCK, tq4};

/// Per-index settings; no process-wide serving flags.
#[derive(Clone, Copy, Debug)]
pub struct Parameters {
    pub tail: bool,
    pub weight_bits: WeightBits,
}
impl Default for Parameters {
    fn default() -> Self {
        Self {
            tail: true,
            weight_bits: WeightBits::Eight,
        }
    }
}
fn vlevels() -> &'static VLevels {
    static LV: std::sync::OnceLock<VLevels> = std::sync::OnceLock::new();
    LV.get_or_init(VLevels::new)
}

// ---------------------------------------------------------------------------------------------
// `--pages-rescore`: the originals, seen through one q-head's query
// ---------------------------------------------------------------------------------------------

/// What `--pages-rescore` needs of the session's original rows.
///
/// A trait and not `service::ExactRows` directly for two reasons: the page path must not depend on
/// how a session happens to hold its bf16 (originals, or the legacy codes dequantised on the fly --
/// `ExactRows` already hides that), and the tests below build a `PagesHead` with no session behind
/// it at all. `pos` is always a **logical** token id, which is what every row reader means by a
/// position.
pub trait ExactRowsSource {
    /// `q . k(pos)` on the originals, unscaled -- the same number the legacy path rescores with.
    fn dot_key(&mut self, pos: u32) -> f32;
    /// The value row of `pos`, decoded to f32 in the original (unrotated) space.
    fn value(&mut self, pos: u32, out: &mut [f32]);
    /// Cold path: ask the kernel for these rows before the first one is read.
    fn will_need(&mut self, positions: &[u32]);
}

// ---------------------------------------------------------------------------------------------
// the beam over pages
// ---------------------------------------------------------------------------------------------

/// The page graph as [`index::Adj`], so the walk is literally `HeadIndex::search`'s.
///
/// The page CSR hands out a row by copying it (`PageCsr::row`) rather than by lending a window,
/// because its links are u16 on disk while the walk wants i32; the copy is 32 links into a stack
/// buffer, which is nothing next to the 4 KiB page each of them stands for. Upper levels are full
/// CSRs over all pages, so there is no `pos` indirection to pass -- unlike `index::Lvl`, whose
/// upper levels are packed.
struct PageAdj<'a>(&'a PageCsr);

impl Adj for PageAdj<'_> {
    fn with_nbrs<R>(&self, _pos: &[i32], node: u32, f: impl FnOnce(&[i32]) -> R) -> R {
        let mut buf = [0i32; MAX_ROW];
        let n = self.0.row(node, &mut buf).min(MAX_ROW);
        f(&buf[..n])
    }

    /// The links are in RAM (`.graph` is read whole), so there is nothing to pull in: the only
    /// lookahead that pays here is [`PageScorer::prefetch`], which advises the 4 KiB data pages.
    fn prefetch_row(&self, _pos: &[i32], _node: u32) {}
}

/// The beam's scorer: one page scored whole on first touch, its `T` raw scores kept for the
/// softmax and the answer (PAGES.md section 8, steps 1-2).
struct PageScorer<'a> {
    head: &'a PagesHead,
    pq: PageQuery,
    /// `P * T` raw scores (before `score_scale`); NaN = the page was never touched.
    s: Vec<f32>,
    /// Pages whose `T` scores are in `s`.
    scanned: Vec<bool>,
    /// Pages already handed to `advise_pages`. A `MADV_WILLNEED` is a syscall; issuing it twice
    /// for the same page in one search buys nothing, and the beam's lookahead does revisit the
    /// same neighbour from two different expansions.
    advised: Vec<bool>,
    /// `local_tokens`: the client already holds everything below it densely, so those tokens
    /// neither set a page's score nor enter the answer.
    lo: u32,
    /// Pages scored (`SearchResponse.pages_scanned`'s share for this q-head).
    pages: usize,
    /// Scratch for `advise_pages`, reused across expansions.
    want: Vec<u32>,
}

impl<'a> PageScorer<'a> {
    fn new(head: &'a PagesHead, pq: PageQuery, lo: u32) -> PageScorer<'a> {
        PageScorer {
            s: vec![f32::NAN; head.pages * head.t],
            scanned: vec![false; head.pages],
            advised: vec![false; head.pages],
            head,
            pq,
            lo,
            pages: 0,
            want: Vec::with_capacity(MAX_ROW),
        }
    }

    /// Score page `p` whole, once.
    fn scan(&mut self, p: usize) {
        if p >= self.head.pages || self.scanned[p] {
            return;
        }
        let (t, base) = (self.head.t, p * self.head.t);
        let mut out = [[0.0f32; 32]; 1];
        score_page(
            self.head.page(p),
            self.head.dim,
            t,
            self.head.valid(p),
            &self.head.scale_k[base..base + t],
            &self.head.shift_k[base..base + t],
            &[&self.pq],
            &mut out,
        );
        self.s[base..base + t].copy_from_slice(&out[0][..t]);
        self.scanned[p] = true;
        self.pages += 1;
    }

    /// What the beam's heap ranks a page by: the best of its tokens that could enter the answer.
    /// A page whose every token is below `local_tokens` is worthless to this query however close
    /// its keys are, and giving it `-inf` keeps the beam from spending its `ef` on the prefix.
    fn page_score(&self, p: usize) -> f32 {
        let base = p * self.head.t;
        let mut best = f32::NEG_INFINITY;
        for slot in 0..self.head.valid(p) {
            let l = self.head.logical[base + slot];
            if l != u32::MAX && l >= self.lo && self.s[base + slot] > best {
                best = self.s[base + slot];
            }
        }
        best
    }
}

impl Scorer for PageScorer<'_> {
    fn score_many(&mut self, ids: &[u32], out: &mut [f32]) {
        for (o, &id) in out.iter_mut().zip(ids) {
            let p = id as usize;
            self.scan(p);
            *o = self.page_score(p);
        }
    }

    /// The beam's one-expansion lookahead, which on this path is the whole cold story: the pages
    /// the next pop will score are advised now, while this expansion's arithmetic still runs.
    fn prefetch(&mut self, ids: &[i32]) {
        self.want.clear();
        for &id in ids {
            let p = id as usize;
            if p < self.head.pages && !self.scanned[p] && !self.advised[p] {
                self.advised[p] = true;
                self.want.push(p as u32);
            }
        }
        if !self.want.is_empty() {
            self.head.advise_pages(&self.want);
        }
    }
}

/// Greedy descent on the upper levels, then one beam of `ef` **pages** on level 0 -- the
/// semantics of `HeadIndex::search`, over pages instead of tokens.
fn walk(head: &PagesHead, scorer: &mut PageScorer<'_>, ef: usize) {
    let g = &head.graph;
    let p = head.pages as u32;
    if p == 0 {
        return;
    }
    let entry = if g.entry < p { g.entry } else { 0 };
    let mut cs = [0.0f32; 1];
    scorer.score_many(&[entry], &mut cs);
    let (mut cur, mut cs) = (entry, cs[0]);
    for l in (1..=g.entry_level.min(g.up.len())).rev() {
        let (c, s, _) =
            index::greedy_on_level(&PageAdj(&g.up[l - 1]), &[], scorer, None, cur, cs, p);
        cur = c;
        cs = s;
    }
    let mut out = Vec::new();
    index::with_scratch(head.pages, |st, epoch| {
        let mut seen = Visited::new(st, epoch);
        index::beam_on_level(
            &PageAdj(&g.l0),
            &[],
            scorer,
            &[(cs, cur)],
            ef.max(1),
            &mut seen,
            &mut out,
            p,
        );
    });
}

// ---------------------------------------------------------------------------------------------
// one q-head's whole attention
// ---------------------------------------------------------------------------------------------

/// One q-head's answer: the remote attention itself, plus what the reply reports about it.
pub struct PageAttention {
    /// `m + ln z` in the request's `score_scale` units.
    pub lse: f32,
    /// The attention output over everything the store holds, `dim` f32.
    pub out: Vec<f32>,
    /// Diagnostic only (PAGES.md section 8, step 8): the best `k` logical ids of `U`, best first.
    pub positions: Vec<u32>,
    /// Their raw (unscaled) scores, as the legacy path reports them.
    pub scores: Vec<f32>,
    /// `|U|` -- `GroupResult.scored`.
    pub scored: usize,
    /// Pages scored whole.
    pub pages: usize,
    /// Rows re-read on the originals (`--pages-rescore`).
    pub rescored: usize,
}

impl PageAttention {
    /// Nothing to attend to (an empty head, or a `local_tokens` that covers the whole context).
    /// `pages` is still what the beam cost: the work happened whether or not it found anything.
    fn empty(dim: usize, pages: usize) -> PageAttention {
        PageAttention {
            lse: f32::NEG_INFINITY,
            out: vec![0.0; dim],
            positions: Vec::new(),
            scores: Vec::new(),
            scored: 0,
            pages,
            rescored: 0,
        }
    }
}

/// The whole of PAGES.md section 8 for one query: beam, sinks, `U`, groups, `m`, the V pass and
/// the coarse tail.
///
/// `exact` is `--pages-rescore`: when `Some` and `rescore > 0`, the best `rescore` tokens of `U`
/// by their TQ score get their score AND their value from the originals instead -- the
/// like-for-like knob against a vector database's `rescore`/`oversampling` on float16. Their
/// weight is removed from the page pass (`w[t] = 0`, so `attend_page` skips them) and added to the
/// accumulator in the original space, where the exact value already lives; everything else about
/// the answer is unchanged, which is what makes the two comparable.
#[allow(clippy::too_many_arguments)]
pub fn attend(
    head: &PagesHead,
    rot: &Rotation,
    q: &[f32],
    scale: f32,
    local_tokens: usize,
    ef: usize,
    k: usize,
    sinks: bool,
    rescore: usize,
    mut exact: Option<&mut dyn ExactRowsSource>,
    params: Parameters,
) -> PageAttention {
    let (d, t) = (head.dim, head.t);
    if head.n == 0 || local_tokens >= head.n {
        return PageAttention::empty(d, 0);
    }
    let lo = local_tokens as u32;

    // ---- 1-2: the beam, every page it touches scored whole ---------------------------------
    let mut sc = PageScorer::new(head, PageQuery::new(q, &head.cent_k, rot), lo);
    walk(head, &mut sc, ef);

    // ---- 3: the sinks, then U ---------------------------------------------------------------
    if sinks {
        for l in 0..16usize.min(head.n) {
            let phys = head.inverse[l] as usize;
            sc.scan(phys / t);
        }
    }
    let block = head.groups.block.max(1);
    let n_groups = head.groups.n_groups;
    let mut removed = vec![0u32; n_groups];
    let mut u: Vec<u32> = Vec::with_capacity(sc.pages * t);
    for p in 0..head.pages {
        if !sc.scanned[p] {
            continue;
        }
        let base = p * t;
        for slot in 0..head.valid(p) {
            let l = head.logical[base + slot];
            if l == u32::MAX || l < lo {
                continue;
            }
            u.push((base + slot) as u32);
            let g = l as usize / block;
            if g < n_groups {
                removed[g] += 1;
            }
        }
    }
    if u.is_empty() {
        return PageAttention::empty(d, sc.pages);
    }

    // ---- `--pages-rescore`: the best R of U, on the originals -------------------------------
    //
    // Before `m`, because a corrected score moves the softmax. Sorted by physical id afterwards so
    // that the V pass can test membership with a binary search instead of another `P * T` array.
    let mut exact_ids: Vec<u32> = Vec::new();
    if rescore > 0 && exact.is_some() {
        let r = rescore.min(u.len());
        let mut best = u.clone();
        if r < best.len() {
            best.select_nth_unstable_by(r - 1, |&a, &b| {
                sc.s[b as usize].total_cmp(&sc.s[a as usize])
            });
            best.truncate(r);
        }
        let pos: Vec<u32> = best.iter().map(|&id| head.logical[id as usize]).collect();
        let src = exact.as_deref_mut().expect("checked");
        src.will_need(&pos);
        for (&id, &p) in best.iter().zip(&pos) {
            sc.s[id as usize] = src.dot_key(p);
        }
        best.sort_unstable();
        exact_ids = best;
    }

    // ---- 4-6: scale, the coarse groups, and the softmax maximum ------------------------------
    let mut m = f32::NEG_INFINITY;
    for &id in &u {
        let s = sc.s[id as usize] * scale;
        if s > m {
            m = s;
        }
    }
    // (score, how many of the group's tokens the answer did NOT take explicitly, group)
    let mut groups: Vec<(f32, f64, usize)> = Vec::new();
    if params.tail {
        for g in 0..n_groups {
            if g * block < local_tokens || head.groups.count[g] <= removed[g] {
                continue;
            }
            let s_g = index::dot_f32(q, &head.groups.mean_k[g * d..(g + 1) * d]) * scale;
            groups.push((s_g, (head.groups.count[g] - removed[g]) as f64, g));
            if s_g > m {
                m = s_g;
            }
        }
    }
    if !m.is_finite() {
        return PageAttention::empty(d, sc.pages);
    }

    // ---- 7: the V pass over exactly the pages the beam scored --------------------------------
    let lv = vlevels();
    let bits = params.weight_bits;
    let mut acc = VAcc::new(d);
    // the exactly-valued tokens' share, already in the original space
    let mut exact_num = vec![0.0f64; d];
    let mut exact_z = 0.0f64;
    let mut vrow = vec![0.0f32; d];
    for p in 0..head.pages {
        if !sc.scanned[p] {
            continue;
        }
        let base = p * t;
        let mut w = [0.0f32; 32];
        let mut any = false;
        for slot in 0..head.valid(p) {
            let id = base + slot;
            let l = head.logical[id];
            if l == u32::MAX || l < lo {
                continue;
            }
            let wt = ((sc.s[id] * scale - m) as f64).exp();
            if exact_ids.binary_search(&(id as u32)).is_ok() {
                let src = exact
                    .as_deref_mut()
                    .expect("exact ids come from an exact source");
                src.value(l, &mut vrow);
                for (a, &x) in exact_num.iter_mut().zip(&vrow) {
                    *a += wt * x as f64;
                }
                exact_z += wt;
            } else {
                w[slot] = wt as f32;
                any = true;
            }
        }
        if any {
            attend_page(
                head.page(p),
                d,
                t,
                head.valid(p),
                &head.scale_v[base..base + t],
                &head.shift_v[base..base + t],
                &w,
                lv,
                bits,
                &mut acc,
            );
        }
    }

    // `num_rot` is in the rotated space and the two scalar corrections are not, exactly as
    // PAGES.md section 2 splits them: R^T of the codebook sum, plus the shifts along R^T 1, plus
    // the head's value centroid once per unit of mass.
    let mut rt = vec![0.0f32; d];
    rot.apply_t(&acc.num_rot, &mut rt);
    let mut num: Vec<f64> = (0..d)
        .map(|i| {
            rt[i] as f64
                + acc.shift_sum as f64 * head.rt_one[i] as f64
                + acc.z * head.cent_v[i] as f64
                + exact_num[i]
        })
        .collect();
    let mut z = acc.z + exact_z;
    for &(s_g, count, g) in &groups {
        let wg = ((s_g - m) as f64).exp() * count;
        z += wg;
        for (a, &x) in num.iter_mut().zip(&head.groups.mean_v[g * d..(g + 1) * d]) {
            *a += wg * x as f64;
        }
    }
    if !(z > 0.0) {
        return PageAttention::empty(d, sc.pages);
    }

    // ---- 8: the reply ------------------------------------------------------------------------
    // The normal attention response needs no diagnostic candidates.
    let mut order = if k == 0 { Vec::new() } else { u.clone() };
    let kk = k.min(order.len());
    if kk < order.len() {
        order.select_nth_unstable_by(kk, |&a, &b| sc.s[b as usize].total_cmp(&sc.s[a as usize]));
        order.truncate(kk);
    }
    order.sort_unstable_by(|&a, &b| sc.s[b as usize].total_cmp(&sc.s[a as usize]));
    PageAttention {
        lse: (m as f64 + z.ln()) as f32,
        out: num.iter().map(|&x| (x / z) as f32).collect(),
        positions: order.iter().map(|&id| head.logical[id as usize]).collect(),
        scores: order.iter().map(|&id| sc.s[id as usize]).collect(),
        scored: u.len(),
        pages: sc.pages,
        rescored: exact_ids.len(),
    }
}

// ---------------------------------------------------------------------------------------------
// the service entry point
#[cfg(test)]
mod tests {
    use super::*;
    use crate::arr::Arr;
    use crate::index::SplitMix64;
    use crate::pages::{k_index, v_index, Groups, PageGraph, PageLinks, PAGE};

    // ---- the spec's scalar codec, written out here so the tests do not lean on `pages.rs` -----

    /// PAGES.md section 2, encode: `g = R(x - c)`, shift = mean, scale = RMS of the rest.
    fn encode(x: &[f32], c: &[f32], rot: &Rotation, codes: &mut [u8]) -> (f32, f32) {
        let d = x.len();
        let r: Vec<f32> = x.iter().zip(c).map(|(a, b)| a - b).collect();
        let mut g = vec![0.0f32; d];
        rot.apply(&r, &mut g);
        let shift = g.iter().sum::<f32>() / d as f32;
        let z: Vec<f32> = g.iter().map(|v| v - shift).collect();
        let scale = (z.iter().map(|v| v * v).sum::<f32>() / d as f32).sqrt();
        for (o, &v) in codes.iter_mut().zip(&z) {
            *o = tq4::quantise(if scale > 0.0 { v / scale } else { 0.0 });
        }
        (scale, shift)
    }

    /// PAGES.md section 2, decode: `x_hat = R^T (scale * CB[code] + shift) + c`.
    fn decode(codes: &[u8], scale: f32, shift: f32, c: &[f32], rot: &Rotation, out: &mut [f32]) {
        let g: Vec<f32> = codes
            .iter()
            .map(|&v| scale * tq4::CODEBOOK4[v as usize] + shift)
            .collect();
        rot.apply_t(&g, out);
        for (o, &cc) in out.iter_mut().zip(c) {
            *o += cc;
        }
    }

    fn mean(rows: &[f32], n: usize, d: usize) -> Vec<f32> {
        let mut c = vec![0.0f32; d];
        for r in 0..n {
            for i in 0..d {
                c[i] += rows[r * d + i];
            }
        }
        for v in &mut c {
            *v /= n.max(1) as f32;
        }
        c
    }

    struct Synth {
        head: PagesHead,
        rot: Rotation,
        /// `k_hat` / `v_hat`: what the codec actually stored, which is what the reference
        /// attention must be taken over -- the page path's only other approximation is the int8
        /// query grid.
        k_hat: Vec<f32>,
        v_hat: Vec<f32>,
    }

    /// A `PagesHead` built field by field: random keys and values, the spec's codec, the spec's
    /// nibble layouts, identity token order, decoded group means, and a page graph that is a ring
    /// plus each page's nearest neighbours by centroid dot (connected by construction, so
    /// `ef >= P` really does reach every page).
    fn synth(n: usize, d: usize, seed: u64) -> Synth {
        let t = PAGE / d;
        let pages = n.div_ceil(t);
        let rot = Rotation::new(seed, d);
        let mut rng = SplitMix64::new(seed ^ 0x5bd1_e995);
        let keys: Vec<f32> = (0..n * d)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 2.0)
            .collect();
        let values: Vec<f32> = (0..n * d)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 2.0)
            .collect();
        let cent_k = mean(&keys, n, d);
        let cent_v = mean(&values, n, d);

        let mut data = vec![0u8; pages * PAGE];
        let (mut scale_k, mut shift_k) = (vec![0.0f32; pages * t], vec![0.0f32; pages * t]);
        let (mut scale_v, mut shift_v) = (vec![0.0f32; pages * t], vec![0.0f32; pages * t]);
        let mut logical = vec![u32::MAX; pages * t];
        let mut inverse = vec![0u32; n];
        let (mut k_hat, mut v_hat) = (vec![0.0f32; n * d], vec![0.0f32; n * d]);
        let mut codes = vec![0u8; d];
        let mut hat = vec![0.0f32; d];
        for tok in 0..n {
            // identity layout: physical id == logical id
            let (p, slot) = (tok / t, tok % t);
            logical[tok] = tok as u32;
            inverse[tok] = tok as u32;
            for keys_half in [true, false] {
                let (src, c) = if keys_half {
                    (&keys, &cent_k)
                } else {
                    (&values, &cent_v)
                };
                let (s, sh) = encode(&src[tok * d..(tok + 1) * d], c, &rot, &mut codes);
                let base = if keys_half { 0 } else { PAGE / 2 };
                for (i, &code) in codes.iter().enumerate() {
                    let (byte, bits) = if keys_half {
                        k_index(d, t, slot, i)
                    } else {
                        v_index(d, t, slot, i)
                    };
                    data[p * PAGE + base + byte] |= code << bits;
                }
                decode(&codes, s, sh, c, &rot, &mut hat);
                let dst = if keys_half { &mut k_hat } else { &mut v_hat };
                dst[tok * d..(tok + 1) * d].copy_from_slice(&hat);
                if keys_half {
                    scale_k[tok] = s;
                    shift_k[tok] = sh;
                } else {
                    scale_v[tok] = s;
                    shift_v[tok] = sh;
                }
            }
        }

        // groups of 2048 LOGICAL tokens, means of the decoded vectors
        let n_groups = n.div_ceil(TAIL_BLOCK);
        let (mut mean_k, mut mean_v) = (vec![0.0f32; n_groups * d], vec![0.0f32; n_groups * d]);
        let mut count = vec![0u32; n_groups];
        for tok in 0..n {
            let g = tok / TAIL_BLOCK;
            count[g] += 1;
            for i in 0..d {
                mean_k[g * d + i] += k_hat[tok * d + i];
                mean_v[g * d + i] += v_hat[tok * d + i];
            }
        }
        for g in 0..n_groups {
            for i in 0..d {
                mean_k[g * d + i] /= count[g] as f32;
                mean_v[g * d + i] /= count[g] as f32;
            }
        }

        let mut rt_one = vec![0.0f32; d];
        let ones = vec![1.0f32; d];
        rot.apply_t(&ones, &mut rt_one);

        Synth {
            head: PagesHead {
                n,
                dim: d,
                t,
                pages,
                data: Arr::from_vec(data),
                scale_k,
                shift_k,
                scale_v,
                shift_v,
                logical,
                inverse,
                cent_k,
                cent_v,
                rt_one,
                graph: ring_graph(&k_hat, n, d, t, pages),
                groups: Groups {
                    block: TAIL_BLOCK,
                    n_groups,
                    mean_k,
                    mean_v,
                    count,
                },
            },
            rot,
            k_hat,
            v_hat,
        }
    }

    /// One level, `M` links per page: the two ring neighbours (so the graph is connected whatever
    /// the data does) plus the nearest pages by centroid dot.
    fn ring_graph(k_hat: &[f32], n: usize, d: usize, t: usize, pages: usize) -> PageGraph {
        let mut cent = vec![0.0f32; pages * d];
        for tok in 0..n {
            let p = tok / t;
            for i in 0..d {
                cent[p * d + i] += k_hat[tok * d + i];
            }
        }
        const M: usize = 32;
        let mut offs = Vec::with_capacity(pages + 1);
        let mut links: Vec<u16> = Vec::with_capacity(pages * (M + 2));
        for p in 0..pages {
            offs.push(links.len() as u32);
            let mut row: Vec<u32> = Vec::new();
            if pages > 1 {
                row.push(((p + pages - 1) % pages) as u32);
                row.push(((p + 1) % pages) as u32);
            }
            let mut by: Vec<(f32, u32)> = (0..pages)
                .filter(|&o| o != p && !row.contains(&(o as u32)))
                .map(|o| {
                    (
                        index::dot_f32(&cent[p * d..(p + 1) * d], &cent[o * d..(o + 1) * d]),
                        o as u32,
                    )
                })
                .collect();
            by.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
            row.extend(by.into_iter().take(M).map(|(_, o)| o));
            links.extend(row.into_iter().map(|o| o as u16));
        }
        offs.push(links.len() as u32);
        PageGraph {
            pages: pages as u32,
            levels: Arr::from_vec(vec![0u8; pages]),
            entry: 0,
            entry_level: 0,
            l0: PageCsr {
                offs: Arr::from_vec(offs),
                links: PageLinks::U16(Arr::from_vec(links)),
            },
            up: Vec::new(),
            rule: crate::pages::PageLevels::Shift,
        }
    }

    /// Softmax attention over `lo..n`, on the DECODED vectors, in f64.
    fn reference(s: &Synth, q: &[f32], scale: f32, lo: usize) -> (f32, Vec<f32>) {
        let (n, d) = (s.head.n, s.head.dim);
        let sc: Vec<f32> = (lo..n)
            .map(|tok| index::dot_f32(q, &s.k_hat[tok * d..(tok + 1) * d]) * scale)
            .collect();
        let m = sc.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let mut z = 0.0f64;
        let mut num = vec![0.0f64; d];
        for (j, tok) in (lo..n).enumerate() {
            let w = ((sc[j] - m) as f64).exp();
            z += w;
            for i in 0..d {
                num[i] += w * s.v_hat[tok * d + i] as f64;
            }
        }
        (
            (m as f64 + z.ln()) as f32,
            num.iter().map(|&x| (x / z) as f32).collect(),
        )
    }

    fn query(d: usize, seed: u64) -> Vec<f32> {
        let mut rng = SplitMix64::new(seed);
        (0..d)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 2.0)
            .collect()
    }

    /// `rel` bounds every coordinate against the largest one; the relative L2 error is printed
    /// and bounded at 1.5e-2. Three approximations sit between the page path and the f64
    /// reference over the decoded vectors, none of which the reference makes: the int8 query
    /// grid in the scores (~0.4-1 % of the residual term), the int8 codebook levels in the V
    /// pass and the u8 per-page weight grid (together ~0.4-0.8 % of the output norm,
    /// `pagekernel` tests) -- so a per-coordinate bound has to be several times the kernels'
    /// own RMS to hold on the small coordinates too.
    fn close(got: &[f32], want: &[f32], rel: f32, what: &str) {
        let norm = want.iter().fold(0.0f32, |a, &b| a.max(b.abs())).max(1e-6);
        let (mut num, mut den) = (0.0f64, 0.0f64);
        for (&g, &w) in got.iter().zip(want) {
            num += ((g - w) as f64).powi(2);
            den += (w as f64).powi(2);
        }
        let l2 = (num / den.max(1e-30)).sqrt();
        eprintln!("{what}: relative L2 error {l2:.3e} (per-coordinate bound {rel:.0e} of max)");
        assert!(l2 <= 1.5e-2, "{what}: relative L2 error {l2:.3e} > 1.5e-2");
        for (i, (&g, &w)) in got.iter().zip(want).enumerate() {
            assert!(
                (g - w).abs() <= rel * norm,
                "{what}: coordinate {i}: {g} vs {w} (tolerance {})",
                rel * norm
            );
        }
    }

    /// (a) The dense limit. With `ef >= P` the beam reaches every page, `U` is every token, every
    /// group is fully removed, and what comes back must be the plain softmax over the decoded
    /// vectors -- the int8 query grid is then the only thing between the two numbers.
    #[test]
    fn diagnostic_top_k_never_changes_attention() {
        let s = synth(3000, 128, 71);
        let q = query(128, 33);
        let run = |k| {
            attend(
                &s.head,
                &s.rot,
                &q,
                1.0 / (128f32).sqrt(),
                0,
                4,
                k,
                true,
                0,
                None,
                Parameters::default(),
            )
        };
        let baseline = run(0);
        assert!(baseline.positions.is_empty());
        assert!(baseline.scores.is_empty());
        for k in [1, 16, 128, 10000] {
            let answer = run(k);
            assert_eq!(baseline.out, answer.out);
            assert_eq!(baseline.lse, answer.lse);
            assert_eq!(baseline.scored, answer.scored);
            assert_eq!(answer.positions.len(), k.min(answer.scored));
            assert!(answer.scores.windows(2).all(|w| w[0] >= w[1]));
            let mut ids = answer.positions.clone();
            ids.sort_unstable();
            ids.dedup();
            assert_eq!(ids.len(), answer.positions.len());
        }
    }

    #[test]
    fn dense_limit_equals_a_scalar_reference() {
        for (n, d) in [(3000usize, 256usize), (3000, 128)] {
            let s = synth(n, d, 7);
            let scale = 1.0 / (d as f32).sqrt();
            for seed in [1u64, 2, 3] {
                let q = query(d, seed);
                let got = attend(
                    &s.head,
                    &s.rot,
                    &q,
                    scale,
                    0,
                    s.head.pages,
                    n,
                    false,
                    0,
                    None,
                    Parameters::default(),
                );
                let (lse, out) = reference(&s, &q, scale, 0);
                assert_eq!(got.scored, n, "d {d}: every token must be in U");
                assert_eq!(got.pages, s.head.pages, "d {d}: every page must be scanned");
                assert!(
                    (got.lse - lse).abs() <= 1e-3 * lse.abs().max(1.0),
                    "d {d} seed {seed}: lse {} vs {lse}",
                    got.lse
                );
                close(&got.out, &out, 8e-3, &format!("d {d} seed {seed}: out"));
            }
        }
    }

    /// (b) `local_tokens` excludes exactly the tokens below it -- from `U`, from the answer, and
    /// from the groups (the group they fall in is skipped whole, since its start is below the cut).
    #[test]
    fn local_tokens_excludes_exactly_the_prefix() {
        let (n, d) = (3000usize, 256usize);
        let s = synth(n, d, 11);
        let scale = 1.0 / (d as f32).sqrt();
        let q = query(d, 4);
        let got = attend(
            &s.head,
            &s.rot,
            &q,
            scale,
            TAIL_BLOCK,
            s.head.pages,
            n,
            false,
            0,
            None,
            Parameters::default(),
        );
        assert_eq!(got.scored, n - TAIL_BLOCK);
        let mut pos = got.positions.clone();
        pos.sort_unstable();
        assert_eq!(pos, (TAIL_BLOCK as u32..n as u32).collect::<Vec<_>>());
        let (lse, out) = reference(&s, &q, scale, TAIL_BLOCK);
        assert!(
            (got.lse - lse).abs() <= 1e-3 * lse.abs().max(1.0),
            "lse {} vs {lse}",
            got.lse
        );
        close(&got.out, &out, 8e-3, "out");
    }

    /// (c) `always_include_sinks` puts the pages of logical tokens 0..16 in the answer even when a
    /// narrow beam would never have gone near them.
    #[test]
    fn sinks_pages_are_always_scanned() {
        let (n, d) = (3000usize, 256usize);
        let s = synth(n, d, 13);
        let scale = 1.0 / (d as f32).sqrt();
        let q = query(d, 5);
        let narrow = attend(
            &s.head,
            &s.rot,
            &q,
            scale,
            0,
            4,
            n,
            false,
            0,
            None,
            Parameters::default(),
        );
        let with = attend(
            &s.head,
            &s.rot,
            &q,
            scale,
            0,
            4,
            n,
            true,
            0,
            None,
            Parameters::default(),
        );
        for sink in 0..16usize as u32 {
            assert!(with.positions.contains(&sink), "sink {sink} missing from U");
        }
        assert!(with.scored >= narrow.scored);
        assert!(with.pages >= narrow.pages);
    }

    #[test]
    fn rescoring_every_token_matches_exact_attention() {
        let s = synth(300, 256, 41);
        let q = query(256, 9);
        struct Exact<'a> {
            s: &'a Synth,
            q: &'a [f32],
        }
        impl ExactRowsSource for Exact<'_> {
            fn dot_key(&mut self, pos: u32) -> f32 {
                let d = self.q.len();
                index::dot_f32(
                    self.q,
                    &self.s.k_hat[pos as usize * d..(pos as usize + 1) * d],
                )
            }
            fn value(&mut self, pos: u32, out: &mut [f32]) {
                let d = self.q.len();
                out.copy_from_slice(&self.s.v_hat[pos as usize * d..(pos as usize + 1) * d]);
            }
            fn will_need(&mut self, _: &[u32]) {}
        }
        let mut exact = Exact { s: &s, q: &q };
        let got = attend(
            &s.head,
            &s.rot,
            &q,
            1.0 / 16.0,
            0,
            s.head.pages,
            0,
            true,
            s.head.n,
            Some(&mut exact),
            Parameters::default(),
        );
        let (lse, out) = reference(&s, &q, 1.0 / 16.0, 0);
        assert_eq!(got.rescored, s.head.n);
        assert!((got.lse - lse).abs() < 1e-6);
        close(&got.out, &out, 2e-6, "fully rescored");
    }

    #[test]
    fn persisted_generation_roundtrip_and_invalid_inverse() {
        let mut s = synth(300, 256, 17);
        // Match the production builder, which rounds these arrays before serving or writing.
        for array in [
            &mut s.head.scale_k,
            &mut s.head.shift_k,
            &mut s.head.scale_v,
            &mut s.head.shift_v,
            &mut s.head.groups.mean_k,
            &mut s.head.groups.mean_v,
        ] {
            for value in array {
                *value = half::f16::from_f32(*value).to_f32();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        s.head.write(dir.path(), 0, 0).unwrap();
        let loaded = PagesHead::load(dir.path(), 0, 0, &s.rot).unwrap();
        assert_eq!(&*s.head.data, &*loaded.data);
        assert_eq!(s.head.logical, loaded.logical);
        assert_eq!(s.head.inverse, loaded.inverse);
        let q = query(256, 5);
        let before = attend(
            &s.head,
            &s.rot,
            &q,
            1.0 / 16.0,
            0,
            4,
            0,
            true,
            0,
            None,
            Parameters::default(),
        );
        let after = attend(
            &loaded,
            &s.rot,
            &q,
            1.0 / 16.0,
            0,
            4,
            0,
            true,
            0,
            None,
            Parameters::default(),
        );
        assert_eq!(after.out, before.out);
        assert_eq!(after.lse, before.lse);
        let path = dir.path().join("l0000h0000.inverse");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
        assert!(PagesHead::load(dir.path(), 0, 0, &s.rot).is_err());
    }
}
