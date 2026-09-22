use std::io;

use super::err;
use super::index::{self, HeadIndex, Scorer};
use super::kernel::{self, Codec, KeyQuery};
use super::tq4::{Rotation, TqHead};
pub(crate) fn fit(
    head: &TqHead,
    graph: &HeadIndex,
    rot: &Rotation,
    queries: &[Vec<f32>],
    per_page: usize,
) -> io::Result<Vec<u32>> {
    if per_page == 0 {
        return Err(err("a page that holds no tokens"));
    }
    if queries.is_empty() {
        return Err(err(
            "no uploaded training queries for this head; supply learned permutations",
        ));
    }
    let n = head.n_tokens as usize;
    let recs = crate::builder::nodes::NodeRecords::from_head(head);
    let mut rows = Vec::new();
    let mut cols = vec![Vec::new(); n];
    for q in queries {
        let kq = KeyQuery::new(q, rot, Codec::Tq4);
        let md = kernel::mean_dots(q, &head.cents());
        struct Collect<'a> {
            recs: &'a crate::builder::nodes::NodeRecords,
            md: &'a [f32],
            q: &'a KeyQuery,
        }
        impl Scorer for Collect<'_> {
            fn score_many(&mut self, ids: &[u32], out: &mut [f32]) {
                self.recs.score(self.md, self.q, ids, out);
            }
        }
        let mut scorer = Collect {
            recs: &recs,
            md: &md,
            q: &kq,
        };
        let visited = index::with_scratch(n, |st, epoch| {
            graph.search(&mut scorer, 512, 512, st, epoch, &mut Vec::new());
            st.iter()
                .enumerate()
                .filter_map(|(i, &v)| ((v >> 1) == epoch).then_some(i as u32))
                .collect::<Vec<_>>()
        });
        for &id in &visited {
            cols[id as usize].push(rows.len());
        }
        rows.push(visited);
    }
    let mut hot: Vec<_> = (0..n).collect();
    hot.sort_unstable_by_key(|&i| (std::cmp::Reverse(cols[i].len()), i));
    let mut placed = vec![false; n];
    let mut order = Vec::with_capacity(n);
    let mut counts = vec![0u8; n];
    let pp = per_page;
    for seed in hot {
        if placed[seed] {
            continue;
        }
        order.push(seed as u32);
        placed[seed] = true;
        counts.fill(0);
        let qs = &cols[seed];
        let take = qs.len().min(8);
        for j in 0..take {
            for &id in &rows[qs[j * qs.len() / take]] {
                if !placed[id as usize] {
                    counts[id as usize] += 1;
                }
            }
        }
        let mut candidates: Vec<_> = counts
            .iter()
            .enumerate()
            .filter_map(|(i, &c)| (c > 0).then_some(i))
            .collect();
        let take = candidates.len().min(pp - 1);
        if take > 0 {
            let cmp = |&a: &usize, &b: &usize| counts[b].cmp(&counts[a]).then(a.cmp(&b));
            if take < candidates.len() {
                candidates.select_nth_unstable_by(take, cmp);
            }
            candidates[..take].sort_unstable_by(cmp);
            for &i in &candidates[..take] {
                order.push(i as u32);
                placed[i] = true;
            }
        }
    }
    inverse(&order, n)?;
    Ok(order)
}
fn inverse(perm: &[u32], n: usize) -> io::Result<Vec<u32>> {
    if perm.len() != n {
        return Err(err("permutation length"));
    }
    let mut inv = vec![u32::MAX; n];
    for (i, &p) in perm.iter().enumerate() {
        if p as usize >= n || inv[p as usize] != u32::MAX {
            return Err(err("not a permutation"));
        }
        inv[p as usize] = i as u32;
    }
    Ok(inv)
}
