//! The `pages/` generation: page-major TQ4 K+V with per-token shift/scale, the page graph, the
//! group summaries, `pages-prepare` and the loader. The contract is `storage/PAGES.md`
//! (sections 1-5 and 7); this file is its implementation. The signatures below are the ones
//! the kernels' tests and the search path compile against -- keep them.
//!
//! Two things are worth reading before the code:
//!
//! * **every per-token quantity is f16 on disk, so it is f16 in RAM too.** `build` rounds
//!   `scale`/`shift` and the group means through `f16` and keeps the rounded f32, so a head that
//!   was just built and one that was just loaded are the same head down to the bit. Anything
//!   else would make the encoder disagree with the server about the value of a token by ~1e-3
//!   and would make a build -> write -> load roundtrip untestable.
//! * **the physical/logical split is the whole point.** A physical id `p = page * T + slot` is
//!   where a token *lives*; its logical id is the position every RPC field means. `perm` maps
//!   physical -> logical, `inverse` maps logical -> physical, and both are validated as a
//!   bijection before a byte is written (`paired::inverse` does the same for the same reason:
//!   silently falling back to token order would produce a generation that looks right and
//!   scores the wrong tokens).

use std::path::Path;
use std::{fs, io};

use half::f16;

use crate::arr::{self, Arr, Pod};
use crate::tq4::{self, Rotation};

/// Bytes per page: the IO unit, the compute unit and the attention unit.
pub const PAGE: usize = 4096;
/// Level-0 links per page and upper-level links per page (PAGES.md section 5).
pub const M0: usize = 32;
pub const M: usize = 16;
/// Logical tokens per tail group.
pub const TAIL_BLOCK: usize = 2048;
/// Longest neighbour row a page beam hands to the scorer (`PageCsr::row`).
pub const MAX_ROW: usize = 128;

/// Alignment of every stored array (PAGES.md section 3: "every array starts at a multiple of 64
/// bytes"). Files are padded out to the same boundary so that a loader can check the length it
/// computed against the length on disk without a special case for the tail.
const ALIGN: usize = 64;
const MAGIC: &[u8; 8] = b"KVPAGE01";
const GRAPH_MAGIC: &[u8; 8] = b"KVPGRF01";

fn err(s: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s.into())
}

#[cfg(feature = "builder")]
#[path = "builder/pages.rs"]
mod build;

/// The `lLLLLhHHHH` file stem of a `(layer, kv-head)`, as `paired` spells it.
fn stem(layer: u32, kv_head: u32) -> String {
    format!("l{layer:04}h{kv_head:04}")
}

/// `(T, P)` for a head of `n` tokens at `dim`.
///
/// PAGES.md section 1 asks for `4096 % d == 0`, `d % 4 == 0` and `d <= 256`. Two more rules are
/// implied by the nibble layouts of section 4 and are checked here because a head that breaks
/// them would silently leave nibbles of every page unwritten: `k_index` needs `d/4` to be even
/// (it splits the coordinate quads into two nibble halves) and `v_index` needs `d` to be a
/// multiple of 16 (it tiles coordinates in runs of 16) and `T/4` to be even. `d % 16 == 0`
/// implies `d % 8 == 0`, and `T = 4096/d` is then one of 16..256, all multiples of 8.
fn geometry(n: usize, dim: usize) -> io::Result<(usize, usize)> {
    if dim == 0 || PAGE % dim != 0 || dim > 256 || dim % 16 != 0 {
        return Err(err(format!(
            "pages: head_dim {dim} must divide {PAGE}, be a multiple of 16 and at most 256"
        )));
    }
    let t = PAGE / dim;
    if t % 8 != 0 {
        return Err(err(format!(
            "pages: {t} tokens per page is not a multiple of 8"
        )));
    }
    Ok((t, n.div_ceil(t)))
}

/// How a page's level is derived from its tokens' levels (PAGES.md section 5).
///
/// A page is `T` tokens, and Qdrant's `round(-ln u / ln M)` puts 25 % of tokens on level >= 1, so
/// "the max of my tokens" makes `1 - 0.75^16 = 99 %` of pages reach level 1: the hierarchy
/// collapses and the greedy descent walks nearly the whole head before the beam starts (measured:
/// 334 scanned pages per q-head at `ef = 16`). [`PageLevels::Shift`] drops every page one level,
/// so a page reaches level `l >= 1` only if one of its tokens reaches level `l + 1` -- 22 % on
/// level 1 and 1.5 % on level 2 at `T = 16`, which is the shape a hierarchy is supposed to have.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageLevels {
    /// `level(page) = max token level`; page level `l` is linked by the tokens' level-`l` links.
    Max,
    /// `level(page) = max(0, max token level - 1)`; page level `l >= 1` is linked by the tokens'
    /// level-`(l + 1)` links. The default.
    Shift,
}

impl PageLevels {
    pub fn parse(s: &str) -> Option<PageLevels> {
        match s {
            "max" => Some(PageLevels::Max),
            "shift" => Some(PageLevels::Shift),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            PageLevels::Max => "max",
            PageLevels::Shift => "shift",
        }
    }

    /// The `.meta` word. `Max` is 0 so that a generation written before the word existed -- its
    /// header was followed by zero padding -- reads back as the rule it was actually built with.
    const fn code(self) -> u32 {
        match self {
            PageLevels::Max => 0,
            PageLevels::Shift => 1,
        }
    }

    const fn from_code(v: u32) -> Option<PageLevels> {
        match v {
            0 => Some(PageLevels::Max),
            1 => Some(PageLevels::Shift),
            _ => None,
        }
    }
}

pub enum PageLinks {
    U16(Arr<u16>),
    U32(Arr<u32>),
}

impl PageLinks {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            PageLinks::U16(a) => a.len(),
            PageLinks::U32(a) => a.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, i: usize) -> u32 {
        match self {
            PageLinks::U16(a) => a[i] as u32,
            PageLinks::U32(a) => a[i],
        }
    }

    /// 2 for a u16 row, 4 for a u32 one -- the `width` word of the `.graph` header.
    fn width(&self) -> u32 {
        match self {
            PageLinks::U16(_) => 2,
            PageLinks::U32(_) => 4,
        }
    }
}

/// One level of the page graph: a CSR over ALL pages (empty rows below the level).
pub struct PageCsr {
    pub offs: Arr<u32>,
    pub links: PageLinks,
}

impl PageCsr {
    /// Copy `page`'s neighbour row into `buf` (as i32 page ids); returns how many.
    ///
    /// The row is truncated at [`MAX_ROW`], which no written row ever reaches (`M0` = 32): the
    /// cap exists so that a hand-edited file cannot overrun the beam's stack buffer.
    pub fn row(&self, page: u32, buf: &mut [i32; MAX_ROW]) -> usize {
        let a = self.offs[page as usize] as usize;
        let b = self.offs[page as usize + 1] as usize;
        let k = (b - a).min(MAX_ROW);
        for (slot, out) in buf[..k].iter_mut().enumerate() {
            *out = self.links.get(a + slot) as i32;
        }
        k
    }

    /// Out-degree of `page` on this level.
    #[inline]
    pub fn degree(&self, page: u32) -> usize {
        (self.offs[page as usize + 1] - self.offs[page as usize]) as usize
    }

    pub fn bytes(&self) -> u64 {
        (self.offs.len() * 4 + self.links.len() * self.links.width() as usize) as u64
    }
}

pub struct PageGraph {
    pub pages: u32,
    pub levels: Arr<u8>,
    pub entry: u32,
    pub entry_level: usize,
    pub l0: PageCsr,
    /// `up[0]` is level 1.
    pub up: Vec<PageCsr>,
    /// The rule `levels` was derived with, as recorded in `.meta`.
    pub rule: PageLevels,
}

impl PageGraph {
    /// Level `l`'s CSR (`l == 0` is [`PageGraph::l0`]).
    #[inline]
    pub fn level(&self, l: usize) -> Option<&PageCsr> {
        match l {
            0 => Some(&self.l0),
            _ => self.up.get(l - 1),
        }
    }

    /// Levels `0..n_levels`, i.e. `up.len() + 1`.
    #[inline]
    pub fn n_levels(&self) -> usize {
        self.up.len() + 1
    }

    pub fn bytes(&self) -> u64 {
        self.levels.len() as u64 + self.l0.bytes() + self.up.iter().map(|c| c.bytes()).sum::<u64>()
    }
}

/// Tail groups over `TAIL_BLOCK` logical tokens: means of the DECODED `k_hat` / `v_hat`.
pub struct Groups {
    pub block: usize,
    pub n_groups: usize,
    pub mean_k: Vec<f32>,
    pub mean_v: Vec<f32>,
    pub count: Vec<u32>,
}

pub struct PagesHead {
    pub n: usize,
    pub dim: usize,
    /// Tokens per page (`PAGE / dim`).
    pub t: usize,
    pub pages: usize,
    /// `pages * PAGE` bytes, mapped with `MapUse::PageMajor` when loaded from disk: `MADV_RANDOM`
    /// and no whole-file prefetch, so the cold IO is the pages the beam visits (plus its
    /// one-expansion lookahead through [`PagesHead::advise_pages`]).
    pub data: Arr<u8>,
    /// Per physical slot (`pages * t` entries, padding = 0).
    pub scale_k: Vec<f32>,
    pub shift_k: Vec<f32>,
    pub scale_v: Vec<f32>,
    pub shift_v: Vec<f32>,
    /// Per physical slot; `u32::MAX` for padding.
    pub logical: Vec<u32>,
    /// Per logical token: its physical id.
    pub inverse: Vec<u32>,
    pub cent_k: Vec<f32>,
    pub cent_v: Vec<f32>,
    /// `R^T 1`, the direction a per-token shift adds in the original space.
    pub rt_one: Vec<f32>,
    pub graph: PageGraph,
    pub groups: Groups,
}

impl PagesHead {
    /// The 4096 bytes of page `p`.
    #[inline]
    pub fn page(&self, p: usize) -> &[u8] {
        &self.data[p * PAGE..(p + 1) * PAGE]
    }

    /// Real tokens on page `p` (only the last page can hold fewer than `t`).
    #[inline]
    pub fn valid(&self, p: usize) -> usize {
        self.t.min(self.n.saturating_sub(p * self.t))
    }

    /// `MADV_WILLNEED` these pages (one call each; a no-op when the data is not mapped).
    ///
    /// One call per page rather than one over their span: the beam's lookahead hands over a
    /// handful of scattered page ids, and advising the hull of a scattered set would pull in
    /// everything between them -- the exact cost the page-major layout exists to avoid.
    /// `Arr::advise_at` is already a no-op for an owned array and when `--prefetch` is off.
    pub fn advise_pages(&self, pages: &[u32]) {
        for &p in pages {
            let p = p as usize;
            if p < self.pages {
                self.data.advise_at(p * PAGE, PAGE, arr::Advice::WillNeed);
            }
        }
    }

    /// Bytes this head occupies on disk (what the session's `bytes` reports).
    pub fn bytes(&self) -> u64 {
        self.kv_bytes() + self.index_bytes()
    }

    /// The `.pages` file: the K and V codes themselves.
    pub fn kv_bytes(&self) -> u64 {
        (self.pages * PAGE) as u64
    }

    /// Everything that is not codes: side arrays, page graph, group summaries, inverse. This is
    /// what `Session.index_bytes` reports, the same split `paired` makes.
    pub fn index_bytes(&self) -> u64 {
        (self.logical.len() * 12 + self.inverse.len() * 4) as u64
            + self.graph.bytes()
            + (self.groups.n_groups * (4 * self.dim + 4)) as u64
    }

    /// Write the head's files into `dir` (`lLLLLhHHHH.*`, PAGES.md section 3).
    pub fn write(&self, dir: &Path, layer: u32, kv_head: u32) -> io::Result<()> {
        let stem = stem(layer, kv_head);
        let path = |ext: &str| dir.join(format!("{stem}.{ext}"));

        let mut meta = Sink::new(MAGIC);
        meta.u32s(&[
            self.n as u32,
            self.dim as u32,
            self.t as u32,
            self.pages as u32,
            M0 as u32,
            M as u32,
            self.graph.n_levels() as u32,
            self.groups.block as u32,
            // Appended after the format shipped, and `Max` is 0, so the zero padding of a
            // generation written before this word reads back as the rule it was built with --
            // which is why the magic does not change.
            self.graph.rule.code(),
        ]);
        meta.pod(&self.cent_k);
        meta.pod(&self.cent_v);
        fs::write(path("meta"), meta.finish())?;

        fs::write(path("pages"), &self.data[..])?;

        let mut side = Vec::with_capacity(self.logical.len() * 12);
        for i in 0..self.logical.len() {
            for x in [
                self.scale_k[i],
                self.shift_k[i],
                self.scale_v[i],
                self.shift_v[i],
            ] {
                side.extend_from_slice(&f16::from_f32(x).to_le_bytes());
            }
            side.extend_from_slice(&self.logical[i].to_le_bytes());
        }
        fs::write(path("side"), side)?;

        let g = &self.graph;
        let mut graph = Sink::new(GRAPH_MAGIC);
        graph.u32s(&[
            g.pages,
            g.l0.links.width(),
            M0 as u32,
            M as u32,
            g.n_levels() as u32,
            g.entry,
            g.entry_level as u32,
        ]);
        graph.pod(&g.levels[..]);
        for level in 0..g.n_levels() {
            let csr = g.level(level).expect("a CSR per level");
            graph.pod(&csr.offs[..]);
            match &csr.links {
                PageLinks::U16(a) => graph.pod(&a[..]),
                PageLinks::U32(a) => graph.pod(&a[..]),
            }
        }
        fs::write(path("graph"), graph.finish())?;

        let mut summary = Vec::with_capacity(self.groups.n_groups * (4 * self.dim + 4));
        for gr in 0..self.groups.n_groups {
            let r = gr * self.dim..(gr + 1) * self.dim;
            for &x in self.groups.mean_k[r.clone()]
                .iter()
                .chain(&self.groups.mean_v[r])
            {
                summary.extend_from_slice(&f16::from_f32(x).to_le_bytes());
            }
            summary.extend_from_slice(&self.groups.count[gr].to_le_bytes());
        }
        fs::write(path("summary"), summary)?;

        fs::write(path("inverse"), arr::pod_bytes(&self.inverse))
    }

    /// Map/read the head's files back; every check of section 3 applies.
    pub fn load(dir: &Path, layer: u32, kv_head: u32, rot: &Rotation) -> io::Result<PagesHead> {
        let stem = stem(layer, kv_head);
        let path = |ext: &str| dir.join(format!("{stem}.{ext}"));

        // ---- meta ---------------------------------------------------------------------------
        let raw = fs::read(path("meta"))?;
        let mut src = Src::new(&raw, MAGIC, "pages meta")?;
        let [n, dim, t, pages, m0, m, n_levels, block, rule] = src.u32s::<9>()?;
        let (n, dim, t, pages, n_levels, block) = (
            n as usize,
            dim as usize,
            t as usize,
            pages as usize,
            n_levels as usize,
            block as usize,
        );
        let rule = PageLevels::from_code(rule)
            .ok_or_else(|| err(format!("pages meta: unknown page-level rule {rule}")))?;
        if dim != rot.dim {
            return Err(err(format!(
                "pages meta: dim {dim} against a rotation of {}",
                rot.dim
            )));
        }
        let (want_t, want_pages) = geometry(n, dim)?;
        if t != want_t || pages != want_pages {
            return Err(err(format!(
                "pages meta: {t} tokens on each of {pages} pages, geometry says {want_t} on {want_pages}"
            )));
        }
        if m0 as usize != M0 || m as usize != M || block != TAIL_BLOCK || n_levels == 0 {
            return Err(err("pages meta: graph or tail parameters out of contract"));
        }
        let cent_k = src.pod::<f32>(dim)?;
        let cent_v = src.pod::<f32>(dim)?;
        src.end(raw.len(), "pages meta")?;

        // ---- pages: the only mapped file ----------------------------------------------------
        let blob = arr::map_file_for(&path("pages"), arr::MapUse::PageMajor)?;
        if blob.len() != pages * PAGE {
            return Err(err(format!(
                "pages: {} bytes of page data for {pages} pages",
                blob.len()
            )));
        }
        let data = Arr::window(&blob, 0, pages * PAGE)
            .ok_or_else(|| err("pages: the page data did not map"))?;

        // ---- side: read whole, it is 12 B per slot ------------------------------------------
        let slots = pages * t;
        let raw = fs::read(path("side"))?;
        if raw.len() != slots * 12 {
            return Err(err(format!(
                "pages side: {} bytes for {slots} slots",
                raw.len()
            )));
        }
        let mut scale_k = vec![0f32; slots];
        let mut shift_k = vec![0f32; slots];
        let mut scale_v = vec![0f32; slots];
        let mut shift_v = vec![0f32; slots];
        let mut logical = vec![0u32; slots];
        for (i, r) in raw.chunks_exact(12).enumerate() {
            scale_k[i] = f16at(r, 0);
            shift_k[i] = f16at(r, 2);
            scale_v[i] = f16at(r, 4);
            shift_v[i] = f16at(r, 6);
            logical[i] = u32at(r, 8);
            if logical[i] != u32::MAX && logical[i] as usize >= n {
                return Err(err(format!(
                    "pages side: slot {i} claims logical {}",
                    logical[i]
                )));
            }
        }

        // ---- inverse: the bijection the whole format rests on --------------------------------
        let raw = fs::read(path("inverse"))?;
        if raw.len() != 4 * n {
            return Err(err(format!(
                "pages inverse: {} bytes for {n} tokens",
                raw.len()
            )));
        }
        let inverse: Vec<u32> = raw.chunks_exact(4).map(|b| u32at(b, 0)).collect();
        for (l, &p) in inverse.iter().enumerate() {
            if p as usize >= slots || logical[p as usize] as usize != l {
                return Err(err(format!(
                    "pages inverse: logical {l} says physical {p}, which holds {}",
                    logical.get(p as usize).copied().unwrap_or(u32::MAX)
                )));
            }
        }

        // ---- summaries ------------------------------------------------------------------------
        let n_groups = n.div_ceil(TAIL_BLOCK);
        let stride = 4 * dim + 4;
        let raw = fs::read(path("summary"))?;
        if raw.len() != n_groups * stride {
            return Err(err(format!(
                "pages summary: {} bytes for {n_groups} groups of {stride}",
                raw.len()
            )));
        }
        let mut mean_k = vec![0f32; n_groups * dim];
        let mut mean_v = vec![0f32; n_groups * dim];
        let mut count = vec![0u32; n_groups];
        for (g, row) in raw.chunks_exact(stride).enumerate() {
            for j in 0..dim {
                mean_k[g * dim + j] = f16at(row, 2 * j);
                mean_v[g * dim + j] = f16at(row, 2 * dim + 2 * j);
            }
            count[g] = u32at(row, 4 * dim);
            let want = (TAIL_BLOCK.min(n - g * TAIL_BLOCK)) as u32;
            if count[g] != want {
                return Err(err(format!(
                    "pages summary: group {g} counts {} of {want}",
                    count[g]
                )));
            }
        }

        let graph = read_page_graph(&path("graph"), pages, rule)?;
        let mut rt_one = vec![0f32; dim];
        rot.apply_t(&vec![1.0f32; dim], &mut rt_one);

        Ok(PagesHead {
            n,
            dim,
            t,
            pages,
            data,
            scale_k,
            shift_k,
            scale_v,
            shift_v,
            logical,
            inverse,
            cent_k,
            cent_v,
            rt_one,
            graph,
            groups: Groups {
                block: TAIL_BLOCK,
                n_groups,
                mean_k,
                mean_v,
                count,
            },
        })
    }
}

fn read_page_graph(path: &Path, pages: usize, rule: PageLevels) -> io::Result<PageGraph> {
    let raw = fs::read(path)?;
    let mut src = Src::new(&raw, GRAPH_MAGIC, "pages graph")?;
    let [p, width, m0, m, n_levels, entry, entry_level] = src.u32s::<7>()?;
    let (p, n_levels, entry_level) = (p as usize, n_levels as usize, entry_level as usize);
    if p != pages {
        return Err(err(format!(
            "pages graph: {p} pages, the meta says {pages}"
        )));
    }
    if m0 as usize != M0 || m as usize != M || n_levels == 0 {
        return Err(err("pages graph: link or level counts out of contract"));
    }
    if width != 2 && width != 4 {
        return Err(err(format!("pages graph: link width {width}")));
    }
    if width == 2 && pages > u16::MAX as usize + 1 {
        return Err(err(format!(
            "pages graph: {pages} pages cannot be addressed by u16 links"
        )));
    }
    let levels = src.pod::<u8>(pages)?;
    if levels.iter().any(|&l| l as usize >= n_levels) {
        return Err(err("pages graph: a page sits above the top level"));
    }
    if entry as usize >= pages.max(1) || entry_level >= n_levels {
        return Err(err(format!(
            "pages graph: entry {entry} at level {entry_level}"
        )));
    }
    if pages > 0 && levels[entry as usize] as usize != entry_level {
        return Err(err("pages graph: the entry page is not on the entry level"));
    }
    let mut csrs = Vec::with_capacity(n_levels);
    for level in 0..n_levels {
        let offs = src.pod::<u32>(pages + 1)?;
        if offs[0] != 0 || offs.windows(2).any(|w| w[1] < w[0]) {
            return Err(err(format!(
                "pages graph: level {level} offsets are not a CSR"
            )));
        }
        let total = offs[pages] as usize;
        let keep = if level == 0 { M0 } else { M };
        if offs.windows(2).any(|w| (w[1] - w[0]) as usize > keep) {
            return Err(err(format!(
                "pages graph: a level-{level} row holds more than {keep} links"
            )));
        }
        let links = if width == 2 {
            let v = src.pod::<u16>(total)?;
            if v.iter().any(|&x| x as usize >= pages) {
                return Err(err(format!(
                    "pages graph: a level-{level} link leaves the head"
                )));
            }
            PageLinks::U16(Arr::from_vec(v))
        } else {
            let v = src.pod::<u32>(total)?;
            if v.iter().any(|&x| x as usize >= pages) {
                return Err(err(format!(
                    "pages graph: a level-{level} link leaves the head"
                )));
            }
            PageLinks::U32(Arr::from_vec(v))
        };
        csrs.push(PageCsr {
            offs: Arr::from_vec(offs),
            links,
        });
    }
    src.end(raw.len(), "pages graph")?;
    let mut csrs = csrs.into_iter();
    Ok(PageGraph {
        pages: pages as u32,
        levels: Arr::from_vec(levels),
        entry,
        entry_level,
        l0: csrs.next().expect("n_levels >= 1"),
        up: csrs.collect(),
        rule,
    })
}

struct Sink {
    buf: Vec<u8>,
}

impl Sink {
    fn new(magic: &[u8; 8]) -> Sink {
        Sink {
            buf: magic.to_vec(),
        }
    }
    fn u32s(&mut self, xs: &[u32]) {
        for &x in xs {
            self.buf.extend_from_slice(&x.to_le_bytes());
        }
    }
    fn pad(&mut self) {
        let n = self.buf.len().next_multiple_of(ALIGN);
        self.buf.resize(n, 0);
    }
    fn pod<T: Pod>(&mut self, v: &[T]) {
        self.pad();
        self.buf.extend_from_slice(arr::pod_bytes(v));
    }
    fn finish(mut self) -> Vec<u8> {
        self.pad();
        self.buf
    }
}

struct Src<'a> {
    b: &'a [u8],
    off: usize,
    what: &'static str,
}

impl<'a> Src<'a> {
    fn new(b: &'a [u8], magic: &[u8; 8], what: &'static str) -> io::Result<Src<'a>> {
        if b.len() < 8 || &b[..8] != magic {
            return Err(err(format!("{what}: wrong magic")));
        }
        Ok(Src { b, off: 8, what })
    }
    fn u32s<const N: usize>(&mut self) -> io::Result<[u32; N]> {
        if self.off + 4 * N > self.b.len() {
            return Err(err(format!("{}: truncated header", self.what)));
        }
        let mut out = [0u32; N];
        for (i, x) in out.iter_mut().enumerate() {
            *x = u32at(self.b, self.off + 4 * i);
        }
        self.off += 4 * N;
        Ok(out)
    }
    /// A padded POD array, copied out: the file is a `Vec<u8>` with no alignment guarantee, so
    /// windows into it would be unsound -- and these arrays are the small ones, held in RAM.
    fn pod<T: Pod>(&mut self, n: usize) -> io::Result<Vec<T>> {
        self.off = self.off.next_multiple_of(ALIGN);
        let bytes = n
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| err(format!("{}: array size overflows", self.what)))?;
        if self.off + bytes > self.b.len() {
            return Err(err(format!("{}: truncated array of {n}", self.what)));
        }
        let mut v = vec![T::default(); n];
        arr::pod_bytes_mut(&mut v).copy_from_slice(&self.b[self.off..self.off + bytes]);
        self.off += bytes;
        Ok(v)
    }
    /// The file must end exactly where the geometry says it does, trailing padding included.
    fn end(self, len: usize, what: &str) -> io::Result<()> {
        if self.off.next_multiple_of(ALIGN) != len {
            return Err(err(format!(
                "{what}: {len} bytes, the geometry accounts for {}",
                self.off.next_multiple_of(ALIGN)
            )));
        }
        Ok(())
    }
}

fn u32at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

fn f16at(b: &[u8], o: usize) -> f32 {
    f16::from_le_bytes(b[o..o + 2].try_into().unwrap()).to_f32()
}

// ---------------------------------------------------------------------------------------------
// scalar codec and layout reference (PAGES.md sections 2 and 4)
// ---------------------------------------------------------------------------------------------

/// Encode one vector against centroid `c`: fills `codes[i] in 0..16`, returns `(scale, shift)`.
pub fn encode_token(x: &[f32], c: &[f32], rot: &Rotation, codes: &mut [u8]) -> (f32, f32) {
    let mut resid = vec![0f32; rot.dim];
    let mut rotated = vec![0f32; rot.dim];
    encode_into(x, c, rot, &mut resid, &mut rotated, codes)
}

/// [`encode_token`] with the caller's scratch: `prepare` runs this 6.6 million times per
/// session, and two `Vec`s per call would be most of its allocator traffic.
fn encode_into(
    x: &[f32],
    c: &[f32],
    rot: &Rotation,
    resid: &mut [f32],
    rotated: &mut [f32],
    codes: &mut [u8],
) -> (f32, f32) {
    let d = rot.dim;
    debug_assert!(x.len() == d && c.len() == d && codes.len() == d);
    for i in 0..d {
        resid[i] = x[i] - c[i];
    }
    rot.apply(&resid[..d], rotated);
    // f64 for both reductions: the mean and the RMS are subtracted from and divide every
    // coordinate, so their error is the one error the codec cannot average away.
    let shift = (rotated[..d].iter().map(|&g| g as f64).sum::<f64>() / d as f64) as f32;
    let ss: f64 = rotated[..d]
        .iter()
        .map(|&g| {
            let z = (g - shift) as f64;
            z * z
        })
        .sum();
    let scale = (ss / d as f64).sqrt() as f32;
    if scale > 0.0 {
        let inv = 1.0 / scale;
        for i in 0..d {
            codes[i] = tq4::quantise((rotated[i] - shift) * inv);
        }
    } else {
        // An all-equal rotated residual: `u = 0` and the shift alone reconstructs it exactly.
        let zero = tq4::quantise(0.0);
        codes[..d].fill(zero);
    }
    (scale, shift)
}

/// `x_hat = R^T (scale * CB[code] + shift) + c`.
pub fn decode_token(
    codes: &[u8],
    scale: f32,
    shift: f32,
    c: &[f32],
    rot: &Rotation,
    out: &mut [f32],
) {
    let mut g = vec![0f32; rot.dim];
    decode_into(codes, scale, shift, c, rot, &mut g, out);
}

fn decode_into(
    codes: &[u8],
    scale: f32,
    shift: f32,
    c: &[f32],
    rot: &Rotation,
    g: &mut [f32],
    out: &mut [f32],
) {
    let d = rot.dim;
    debug_assert!(codes.len() == d && c.len() == d && out.len() == d);
    for i in 0..d {
        g[i] = scale * tq4::CODEBOOK4[(codes[i] & 0x0f) as usize] + shift;
    }
    rot.apply_t(&g[..d], out);
    for i in 0..d {
        out[i] += c[i];
    }
}

/// `(byte, nibble shift)` of `code[tok][i]` in the K half of a page.
pub fn k_index(d: usize, t: usize, tok: usize, i: usize) -> (usize, u32) {
    debug_assert!(i < d && tok < t);
    let q = d / 4;
    let (m, b) = (i / 4, i % 4);
    let (half, blk) = (m / (q / 2), m % (q / 2));
    (blk * (4 * t) + tok * 4 + b, 4 * half as u32)
}

/// `(byte, nibble shift)` of `code[tok][i]` in the V half of a page (offset within the half).
pub fn v_index(d: usize, t: usize, tok: usize, i: usize) -> (usize, u32) {
    debug_assert!(i < d && tok < t);
    let tq_n = t / 4;
    let (cb, ci) = (i / 16, i % 16);
    let (tq, tb) = (tok / 4, tok % 4);
    let (pair, half) = (tq / 2, tq % 2);
    ((cb * (tq_n / 2) + pair) * 64 + ci * 4 + tb, 4 * half as u32)
}

/// The code of coordinate `i` of token `tok` on a page (keys: K half, else V half).
pub fn code_at(page: &[u8], keys: bool, d: usize, t: usize, tok: usize, i: usize) -> u8 {
    let (byte, shift) = if keys {
        k_index(d, t, tok, i)
    } else {
        v_index(d, t, tok, i)
    };
    let base = if keys { 0 } else { PAGE / 2 };
    (page[base + byte] >> shift) & 0x0f
}
