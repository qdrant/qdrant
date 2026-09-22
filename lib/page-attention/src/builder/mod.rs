//! Offline generation builder, extracted from kv-search's tested preparation path.
//! Build-only token graph and block codec never participate in serving.
#![allow(dead_code)]

mod edges;
pub(crate) mod index;
mod kernel;
mod layout;
mod nodes;
mod search;
mod tq4;

use std::path::Path;
use std::{fs, io};

use crate::pages::{PageLevels, PagesHead};
use crate::tq4::Rotation;

fn err(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn bf16(path: &Path, dim: usize) -> io::Result<(Vec<u8>, Vec<f32>)> {
    let raw = fs::read(path)?;
    if raw.is_empty() || raw.len() % (2 * dim) != 0 {
        return Err(err(format!(
            "{}: expected nonempty BF16 rows of dimension {dim}",
            path.display()
        )));
    }
    let values: Vec<_> = raw
        .chunks_exact(2)
        .map(|b| search::bf16_to_f32(u16::from_le_bytes([b[0], b[1]])))
        .collect();
    if values.iter().any(|v| !v.is_finite()) {
        return Err(err(format!("{}: non-finite values", path.display())));
    }
    Ok((raw, values))
}

/// Input contains keys.bf16, values.bf16, queries.bf16 (row-major LE BF16)
/// and positions.u32 (one causal token position per training query).
/// Output must be a new directory. The caller publishes all heads atomically.
pub fn prepare_head(
    input: &Path,
    output: &Path,
    session: &str,
    layer: u32,
    head: u32,
    dim: usize,
) -> io::Result<()> {
    if !matches!(dim, 128 | 256) || session.is_empty() {
        return Err(err(
            "expected dimension 128 or 256 and a nonempty session id",
        ));
    }
    if output.exists() {
        return Err(err("output already exists"));
    }
    let (raw_keys, keys) = bf16(&input.join("keys.bf16"), dim)?;
    let (_, values) = bf16(&input.join("values.bf16"), dim)?;
    let (raw_queries, queries) = bf16(&input.join("queries.bf16"), dim)?;
    let n = keys.len() / dim;
    if keys.len() != values.len() || n <= dim || n > u32::MAX as usize {
        return Err(err(
            "K/V shapes differ or token count is outside the attention protocol limits",
        ));
    }
    let positions = fs::read(input.join("positions.u32"))?;
    if positions.len() != queries.len() / dim * 4 {
        return Err(err("expected one u32 position per training query"));
    }
    let positions: Vec<_> = positions
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .collect();
    if positions.iter().any(|&p| p as usize >= n) {
        return Err(err("training position outside the context"));
    }
    let rot = Rotation::for_session(session, dim);
    eprintln!(
        "l{layer:04}h{head:04}: {n} tokens, {} training queries: graph",
        positions.len()
    );
    let centred = edges::centred_f32(&raw_keys, dim);
    let mut graph = index::HeadIndex::build(&centred, dim, index::IndexParams::default(), 1);
    let training: Vec<_> = raw_queries.chunks_exact(dim * 2).zip(positions).collect();
    edges::miss_edges(&mut graph, &centred, &raw_keys, dim, &training, 4);
    graph.freeze();
    drop(centred);
    eprintln!("l{layer:04}h{head:04}: learned layout");
    let codes = tq4::TqHead::encode_spec(
        &keys,
        &values,
        dim,
        32,
        &rot,
        kernel::Codec::Tq4,
        tq4::CentroidSpec::default(),
    );
    // Match the prototype's saved layout-training sample (paired::save_training).
    // Edge repair above uses every query; page packing uses at most 256 per head.
    let query_count = queries.len() / dim;
    let count = query_count.min(256);
    let training: Vec<_> = (0..count)
        .map(|i| {
            let start = (i * query_count / count) * dim;
            queries[start..start + dim].to_vec()
        })
        .collect();
    let permutation = layout::fit(&codes, &graph, &rot, &training, 4096 / dim)?;
    drop(codes);
    let pages = PagesHead::build_with(
        &keys,
        &values,
        dim,
        &rot,
        &graph,
        &permutation,
        PageLevels::Shift,
    )?;
    fs::create_dir(output)?;
    pages.write(output, layer, head)?;
    // Validate the persisted representation before the orchestrator publishes it.
    PagesHead::load(output, layer, head, &rot)?;
    eprintln!("l{layer:04}h{head:04}: complete ({} pages)", pages.pages);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_bf16_build_is_repeatable_loadable_and_rejects_bad_training() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input");
        fs::create_dir(&input).unwrap();
        let (n, dim) = (513, 128);
        let mut rng = crate::index::SplitMix64::new(19);
        let mut rows = |count| -> Vec<u8> {
            (0..count)
                .flat_map(|_| search::f32_to_bf16(rng.next_f64() as f32 - 0.5).to_le_bytes())
                .collect()
        };
        fs::write(input.join("keys.bf16"), rows(n * dim)).unwrap();
        fs::write(input.join("values.bf16"), rows(n * dim)).unwrap();
        fs::write(input.join("queries.bf16"), rows(16 * dim)).unwrap();
        let positions: Vec<u8> = (0..16u32).flat_map(|p| (p * 32).to_le_bytes()).collect();
        fs::write(input.join("positions.u32"), positions).unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        prepare_head(&input, &first, "repeatable", 0, 0, dim).unwrap();
        prepare_head(&input, &second, "repeatable", 0, 0, dim).unwrap();
        for ext in ["meta", "pages", "side", "graph", "summary", "inverse"] {
            let name = format!("l0000h0000.{ext}");
            assert_eq!(
                fs::read(first.join(&name)).unwrap(),
                fs::read(second.join(name)).unwrap()
            );
        }
        let rot = Rotation::for_session("repeatable", dim);
        let head = PagesHead::load(&first, 0, 0, &rot).unwrap();
        assert_eq!(head.n, n);
        for log in 0..n {
            assert_eq!(head.logical[head.inverse[log] as usize] as usize, log);
        }
        assert!(prepare_head(&input, &first, "repeatable", 0, 0, dim).is_err());
        let bad = temp.path().join("bad");
        fs::write(input.join("positions.u32"), vec![255; 16 * 4]).unwrap();
        assert!(prepare_head(&input, &bad, "repeatable", 0, 0, dim).is_err());
        assert!(!bad.exists());
        fs::write(input.join("queries.bf16"), []).unwrap();
        assert!(prepare_head(&input, &bad, "repeatable", 0, 0, dim).is_err());
    }
}
