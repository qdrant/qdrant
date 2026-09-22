use super::*;
use crate::builder::index::HeadIndex;
fn f16r(x: f32) -> f32 {
    f16::from_f32(x).to_f32()
}
fn invert(perm: &[u32], n: usize) -> io::Result<Vec<u32>> {
    if perm.len() != n {
        return Err(err(format!(
            "pages: permutation of {} ids for {n} tokens",
            perm.len()
        )));
    }
    let mut inv = vec![u32::MAX; n];
    for (i, &p) in perm.iter().enumerate() {
        if p as usize >= n || inv[p as usize] != u32::MAX {
            return Err(err("pages: not a permutation"));
        }
        inv[p as usize] = i as u32;
    }
    Ok(inv)
}
fn mean_rows(x: &[f32], dim: usize, n: usize) -> Vec<f32> {
    let mut acc = vec![0f64; dim];
    for row in x.chunks_exact(dim) {
        for (a, &v) in acc.iter_mut().zip(row) {
            *a += v as f64;
        }
    }
    acc.into_iter().map(|a| (a / n as f64) as f32).collect()
}
impl PagesHead {
    pub fn build_with(
        keys: &[f32],
        values: &[f32],
        dim: usize,
        rot: &Rotation,
        graph: &HeadIndex,
        perm: &[u32],
        levels: PageLevels,
    ) -> io::Result<PagesHead> {
        let n = perm.len();
        if n == 0 {
            return Err(err("pages: an empty head"));
        }
        if rot.dim != dim {
            return Err(err(format!("pages: rotation of {} for dim {dim}", rot.dim)));
        }
        if keys.len() != n * dim || values.len() != n * dim {
            return Err(err(format!(
                "pages: {} keys and {} values for {n} tokens of {dim} dimensions",
                keys.len(),
                values.len()
            )));
        }
        if graph.n as usize != n || graph.dim != dim {
            return Err(err(format!(
                "pages: the graph has {} nodes of {} dimensions, the head {n} of {dim}",
                graph.n, graph.dim
            )));
        }
        let (t, pages) = geometry(n, dim)?;
        let inverse = invert(perm, n)?;

        // ---- centroids: one per head, the mean of the originals (PAGES.md section 2) --------
        // f64 accumulation over 100k tokens: the f32 sum of that many same-signed terms loses
        // the low bits of every later one, and the centroid is subtracted from every vector
        // before quantisation, so its error is a systematic bias on the whole head.
        let cent_k = mean_rows(keys, dim, n);
        let cent_v = mean_rows(values, dim, n);

        // ---- the codes, in physical order ---------------------------------------------------
        let mut data = vec![0u8; pages * PAGE];
        let slots = pages * t;
        let mut scale_k = vec![0f32; slots];
        let mut shift_k = vec![0f32; slots];
        let mut scale_v = vec![0f32; slots];
        let mut shift_v = vec![0f32; slots];
        let mut logical = vec![u32::MAX; slots];
        let mut codes = vec![0u8; dim];
        let mut resid = vec![0f32; dim];
        let mut rotated = vec![0f32; dim];
        for (phys, &log) in perm.iter().enumerate() {
            let log = log as usize;
            let (p, slot) = (phys / t, phys % t);
            let page = &mut data[p * PAGE..(p + 1) * PAGE];
            for (keys_half, (src, cent)) in
                [(keys, &cent_k), (values, &cent_v)].into_iter().enumerate()
            {
                let x = &src[log * dim..(log + 1) * dim];
                let (s, sh) = encode_into(x, cent, rot, &mut resid, &mut rotated, &mut codes);
                let base = keys_half * (PAGE / 2);
                for (i, &c) in codes.iter().enumerate() {
                    let (byte, shift) = if keys_half == 0 {
                        k_index(dim, t, slot, i)
                    } else {
                        v_index(dim, t, slot, i)
                    };
                    page[base + byte] |= c << shift;
                }
                // f16 here, not at write time: the head in RAM must be the head on disk.
                if keys_half == 0 {
                    scale_k[phys] = f16r(s);
                    shift_k[phys] = f16r(sh);
                } else {
                    scale_v[phys] = f16r(s);
                    shift_v[phys] = f16r(sh);
                }
            }
            logical[phys] = log as u32;
        }

        // ---- group summaries: means of the DECODED vectors (PAGES.md section 3) -------------
        // Decoded, not original: the tail replaces its members with this mean at serving time,
        // so the mean has to live in the same approximation as the tokens it stands for.
        let n_groups = n.div_ceil(TAIL_BLOCK);
        let mut mean_k = vec![0f32; n_groups * dim];
        let mut mean_v = vec![0f32; n_groups * dim];
        let mut count = vec![0u32; n_groups];
        let mut decoded = vec![0f32; dim];
        for g in 0..n_groups {
            let (lo, hi) = (g * TAIL_BLOCK, ((g + 1) * TAIL_BLOCK).min(n));
            let mut sum_k = vec![0f64; dim];
            let mut sum_v = vec![0f64; dim];
            for log in lo..hi {
                let phys = inverse[log] as usize;
                let (p, slot) = (phys / t, phys % t);
                let page = &data[p * PAGE..(p + 1) * PAGE];
                for (keys_half, (sum, cent, scale, shift)) in [
                    (&mut sum_k, &cent_k, &scale_k, &shift_k),
                    (&mut sum_v, &cent_v, &scale_v, &shift_v),
                ]
                .into_iter()
                .enumerate()
                {
                    for (i, c) in codes.iter_mut().enumerate() {
                        *c = code_at(page, keys_half == 0, dim, t, slot, i);
                    }
                    decode_into(
                        &codes,
                        scale[phys],
                        shift[phys],
                        cent,
                        rot,
                        &mut rotated,
                        &mut decoded,
                    );
                    for (a, &x) in sum.iter_mut().zip(&decoded) {
                        *a += x as f64;
                    }
                }
            }
            let c = (hi - lo) as f64;
            count[g] = (hi - lo) as u32;
            for j in 0..dim {
                mean_k[g * dim + j] = f16r((sum_k[j] / c) as f32);
                mean_v[g * dim + j] = f16r((sum_v[j] / c) as f32);
            }
        }

        let mut rt_one = vec![0f32; dim];
        rot.apply_t(&vec![1.0f32; dim], &mut rt_one);

        Ok(PagesHead {
            n,
            dim,
            t,
            pages,
            data: Arr::from_vec(data),
            scale_k,
            shift_k,
            scale_v,
            shift_v,
            logical,
            graph: page_graph(graph, perm, &inverse, t, pages, levels)?,
            inverse,
            cent_k,
            cent_v,
            rt_one,
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
struct Hist {
    count: Vec<u32>,
    touched: Vec<u32>,
}
impl Hist {
    fn new(pages: usize) -> Hist {
        Hist {
            count: vec![0; pages],
            touched: Vec::with_capacity(M0 * 4),
        }
    }

    fn clear(&mut self) {
        for &p in &self.touched {
            self.count[p as usize] = 0;
        }
        self.touched.clear();
    }

    fn add(&mut self, page: u32) {
        let c = &mut self.count[page as usize];
        if *c == 0 {
            self.touched.push(page);
        }
        *c += 1;
    }

    fn best(&self, keep: usize) -> Vec<u32> {
        let mut cand: Vec<(u32, u32)> = self
            .touched
            .iter()
            .map(|&p| (self.count[p as usize], p))
            .collect();
        cand.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        cand.truncate(keep);
        cand.into_iter().map(|(_, p)| p).collect()
    }
}
fn page_graph(
    graph: &HeadIndex,
    perm: &[u32],
    inverse: &[u32],
    t: usize,
    pages: usize,
    rule: PageLevels,
) -> io::Result<PageGraph> {
    let parts = graph
        .stored_parts()
        .ok_or_else(|| err("pages: the token graph is not frozen"))?;
    let n = perm.len();

    let mut levels = vec![0u8; pages];
    for (phys, &log) in perm.iter().enumerate() {
        let l = parts.levels[log as usize];
        let page = &mut levels[phys / t];
        if l > *page {
            *page = l;
        }
    }
    if rule == PageLevels::Shift {
        for l in levels.iter_mut() {
            *l = l.saturating_sub(1);
        }
    }
    let n_levels = levels.iter().copied().max().unwrap_or(0) as usize + 1;
    let entry = inverse[parts.entry as usize] / t as u32;
    let entry_level = levels[entry as usize] as usize;

    let page_of = |token: i32| -> Option<u32> {
        (token >= 0 && (token as usize) < n).then(|| inverse[token as usize] / t as u32)
    };
    let mut hist = Hist::new(pages);
    let stride = parts.params.m.max(1);

    let build_level = |level: usize, keep: usize, hist: &mut Hist| -> PageCsr {
        let mut offs = Vec::with_capacity(pages + 1);
        let mut links: Vec<u32> = Vec::with_capacity(pages * keep / 2);
        offs.push(0u32);
        // Which token level lends this page level its links (`PageLevels::token_level`).
        let src = if rule == PageLevels::Shift && level > 0 {
            level + 1
        } else {
            level
        };
        for p in 0..pages {
            hist.clear();
            if levels[p] as usize >= level {
                let hi = ((p + 1) * t).min(n);
                for phys in p * t..hi {
                    let log = perm[phys] as usize;
                    if (parts.levels[log] as usize) < src {
                        continue;
                    }
                    let row: &[i32] = if src == 0 {
                        graph.links0(log as u32)
                    } else {
                        match parts.up.get(src - 1) {
                            Some(&(ulinks, ucnt)) => {
                                let r = parts.up_pos[log];
                                if r < 0 || r as usize >= ucnt.len() {
                                    continue;
                                }
                                let r = r as usize;
                                let c = (ucnt[r] as usize).min(stride);
                                match ulinks.get(r * stride..r * stride + c) {
                                    Some(s) => s,
                                    None => continue,
                                }
                            }
                            None => continue,
                        }
                    };
                    for &target in row {
                        if let Some(tp) = page_of(target) {
                            if tp as usize != p {
                                hist.add(tp);
                            }
                        }
                    }
                }
            }
            links.extend(hist.best(keep));
            offs.push(links.len() as u32);
        }
        // u16 halves the largest array of the graph and every real head is far under 65536
        // pages (a 100k-token head at d=256 is 6409); u32 is the escape hatch, not the default.
        let links = if pages <= u16::MAX as usize + 1 {
            PageLinks::U16(Arr::from_vec(links.into_iter().map(|x| x as u16).collect()))
        } else {
            PageLinks::U32(Arr::from_vec(links))
        };
        PageCsr {
            offs: Arr::from_vec(offs),
            links,
        }
    };

    let l0 = build_level(0, M0, &mut hist);
    let up = (1..n_levels)
        .map(|l| build_level(l, M, &mut hist))
        .collect();
    Ok(PageGraph {
        pages: pages as u32,
        levels: Arr::from_vec(levels),
        entry,
        entry_level,
        l0,
        up,
        rule,
    })
}
