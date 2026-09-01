# callgraph

Interactive call-graph reports for one function of the Qdrant workspace,
viewable in a browser: pan/zoom graph, per-node docs and source snippets,
exact call sites, GitHub/editor links.

```bash
tools/callgraph/callgraph.py read_bytes_async                        # by name
tools/callgraph/callgraph.py universal_io::traits::read_bytes_async  # :: segments disambiguate
tools/callgraph/callgraph.py lib/common/common/src/universal_io/traits/read.rs:113
```

Prints a `file://…/target/callgraph/<fn>.html` link when done. Requires
`rust-analyzer` and graphviz `dot` on `PATH`; no Python dependencies.

## How it works

- rust-analyzer's call hierarchy over LSP provides resolved (not textual)
  caller/callee edges; both directions are collected in one run.
- Trait declarations and their impls are bridged via goto-implementation /
  goto-declaration, so a walk doesn't dead-end when a call dispatches through
  a trait (dashed edges in the graph).
- Test code is excluded for real: rust-analyzer runs with `cfg(test)` off,
  and `tests/`, `benches/`, `examples/` targets are filtered by path.
- Layout by graphviz at generation time; the report itself is one
  self-contained HTML file with no external resources.

## Options

- `--depth N` — call hops from the root (default 4)
- `--max-nodes N` — per-view node cap (default 250)
- `--out PATH` — output HTML path

## Performance note

Each run cold-starts rust-analyzer, which re-indexes the workspace (~3 min).
If runs become frequent, the upgrade path is a `--serve` mode that keeps one
rust-analyzer instance alive between reports.
