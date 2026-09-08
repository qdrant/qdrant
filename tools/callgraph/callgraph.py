#!/usr/bin/env python3
"""Interactive call-graph reports for the Qdrant workspace.

Drives rust-analyzer's call hierarchy to build caller/callee graphs for one
function, bridges trait declarations <-> implementations (so dispatch through
a trait doesn't dead-end the walk), lays the graphs out with graphviz, and
writes a self-contained interactive HTML report: pan/zoom graph, per-node
source snippets and docs, exact call sites, GitHub/editor links.

The root may also be a struct, enum, trait, type alias, const or static. Its
"callers" are the items whose source mentions it: the enclosing function, or
the enclosing type definition / impl block's self type when the mention is not
inside a function. Its "callees" are its methods (trait body + impl blocks).

Usage:
    tools/callgraph/callgraph.py <item-name>
    tools/callgraph/callgraph.py <module::path::item>
    tools/callgraph/callgraph.py <path/to/file.rs>:<line>

Test code is excluded: rust-analyzer runs with cfg(test) disabled, and the
tests/, benches/, examples/ cargo targets are filtered by path.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TOOL_DIR))
EXCLUDE_PATH_PARTS = ("/tests/", "/benches/", "/examples/", "/target/", "/edge/publish/")
SNIPPET_MAX_LINES = 80
ITEM_RE = r"\b(fn|struct|enum|trait|type|const|static)\s+(%s)\b"
# LSP SymbolKind values as rust-analyzer emits them
FN_KINDS = (6, 12)  # Method, Function
TYPE_KINDS = (23, 10, 11, 26, 14)  # Struct, Enum, Interface (trait), TypeParameter (type alias), Constant (const/static)
IMPL_KIND = 19  # Object: impl blocks
PALETTE = [
    "#dbeafe", "#dcfce7", "#fef3c7", "#fce7f3", "#e0e7ff",
    "#ccfbf1", "#fee2e2", "#f3e8ff", "#ede9d5", "#f1f5f9",
]


def log(msg):
    print(msg, file=sys.stderr)


class Lsp:
    def __init__(self):
        self.proc = subprocess.Popen(
            ["rust-analyzer"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self.next_id = 0
        self.quiescent = False

    def send(self, msg):
        data = json.dumps(msg).encode()
        self.proc.stdin.write(b"Content-Length: %d\r\n\r\n%s" % (len(data), data))
        self.proc.stdin.flush()

    def read_msg(self):
        length = None
        while True:
            line = self.proc.stdout.readline()
            if not line:
                sys.exit("rust-analyzer exited unexpectedly")
            if line.startswith(b"Content-Length:"):
                length = int(line.split(b":")[1])
            if line == b"\r\n":
                break
        return json.loads(self.proc.stdout.read(length))

    def handle(self, msg):
        """React to server-initiated traffic; return True if it was consumed."""
        if msg.get("method") == "experimental/serverStatus":
            self.quiescent = msg["params"].get("quiescent", False)
            return True
        if "id" in msg and "method" in msg:  # server request: give an empty answer
            if msg["method"] == "workspace/configuration":
                result = [None] * len(msg["params"]["items"])
            else:
                result = None
            self.send({"jsonrpc": "2.0", "id": msg["id"], "result": result})
            return True
        return "method" in msg  # other notifications

    def request(self, method, params, default=None):
        self.next_id += 1
        rid = self.next_id
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        while True:
            msg = self.read_msg()
            if self.handle(msg):
                continue
            if msg.get("id") == rid:
                if "error" in msg:
                    return default
                return msg["result"] if msg["result"] is not None else default

    def notify(self, method, params):
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def start(self):
        self.request(
            "initialize",
            {
                "processId": os.getpid(),
                "rootUri": "file://" + ROOT,
                "capabilities": {
                    "textDocument": {
                        "hover": {"contentFormat": ["markdown"]},
                        "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
                    },
                    "experimental": {"serverStatusNotification": True},
                },
                # analyze without cfg(test): test modules become inactive code,
                # so they can never appear in the call hierarchy
                "initializationOptions": {"cfg": {"setTest": False}},
                "clientInfo": {"name": "qdrant-callgraph"},
            },
        )
        self.notify("initialized", {})

    def wait_quiescent(self, timeout=900):
        log("waiting for rust-analyzer to index the workspace (~3 min cold)...")
        deadline = time.time() + timeout
        while not self.quiescent:
            if time.time() > deadline:
                sys.exit("timed out waiting for rust-analyzer indexing")
            self.handle(self.read_msg())
        log("indexed.")


def find_item(target):
    """Locate an item definition by name, optionally qualified with :: module hints."""
    *hints, name = target.split("::")
    pattern = ITEM_RE % re.escape(name)
    out = subprocess.run(
        ["grep", "-rn", "--include=*.rs", "-E", pattern, "lib", "src"],
        cwd=ROOT, capture_output=True, text=True,
    ).stdout.splitlines()
    hits = []
    for line in out:
        path, lineno, text = line.split(":", 2)
        if any(p in "/" + path + "/" for p in EXCLUDE_PATH_PARTS):
            continue
        score = sum(1 for h in hints if h in path.removesuffix(".rs").split("/"))
        hits.append((score, path, int(lineno), text))
    if not hits:
        sys.exit(f"no definition of `{name}` found")
    best = max(score for score, *_ in hits)
    hits = [h for h in hits if h[0] == best]
    if len(hits) > 1:
        log(f"`{target}` is ambiguous, use FILE:LINE:")
        for _, path, lineno, text in hits:
            log(f"  {path}:{lineno}  {text.strip()}")
        sys.exit(1)
    _, path, lineno, text = hits[0]
    return os.path.join(ROOT, path), lineno - 1, re.search(pattern, text).start(2)


def position_in_file(spec):
    path, lineno = spec.rsplit(":", 1)
    path = os.path.abspath(path)
    lineno = int(lineno) - 1
    line = open(path).read().splitlines()[lineno]
    m = re.search(ITEM_RE % r"\w+", line)
    if not m:
        sys.exit(f"no item definition on line {lineno + 1} of {path}")
    return path, lineno, m.start(2)


def keep(path):
    # cfg(test) code is already invisible (cfg.setTest=false); what remains to
    # exclude are the directory-defined cargo targets and generated code.
    return path.startswith(ROOT) and not any(p in path for p in EXCLUDE_PATH_PARTS)


def is_fn(item):
    return item["kind"] in FN_KINDS


def contains(rng, pos):
    start, end = rng["start"], rng["end"]
    return (start["line"], start["character"]) <= (pos["line"], pos["character"]) <= (end["line"], end["character"])


def as_item(uri, sym):
    """Shape a DocumentSymbol like a CallHierarchyItem so type nodes are uniform with fn nodes."""
    return {
        "name": sym["name"],
        "kind": sym["kind"],
        "uri": uri,
        "range": sym["range"],
        "selectionRange": sym["selectionRange"],
        "detail": sym.get("detail", ""),
    }


def norm_locations(resp):
    """Normalize a goto-style response (Location | Location[] | LocationLink[])."""
    if resp is None:
        return []
    if isinstance(resp, dict):
        resp = [resp]
    out = []
    for loc in resp:
        if "targetUri" in loc:
            out.append((loc["targetUri"], loc["targetSelectionRange"]["start"]))
        else:
            out.append((loc["uri"], loc["range"]["start"]))
    return out


class Collector:
    def __init__(self, lsp):
        self.lsp = lsp
        self.nodes = {}  # id -> {item, ...}
        self.ids = {}  # (uri, line, name) -> id
        self.file_cache = {}
        self.symbol_cache = {}  # uri -> hierarchical DocumentSymbol[]

    def node_id(self, item):
        key = (item["uri"], item["selectionRange"]["start"]["line"], item["name"])
        if key not in self.ids:
            self.ids[key] = f"n{len(self.ids)}"
            self.nodes[self.ids[key]] = {"item": item}
        return self.ids[key]

    def prepare(self, uri, pos):
        items = self.lsp.request(
            "textDocument/prepareCallHierarchy",
            {"textDocument": {"uri": uri}, "position": pos},
            default=[],
        )
        return items[0] if items else None

    def lines(self, path):
        if path not in self.file_cache:
            try:
                self.file_cache[path] = open(path).read().splitlines()
            except OSError:
                self.file_cache[path] = []
        return self.file_cache[path]

    def symbols(self, uri):
        if uri not in self.symbol_cache:
            self.symbol_cache[uri] = self.lsp.request(
                "textDocument/documentSymbol", {"textDocument": {"uri": uri}}, default=[]
            )
        return self.symbol_cache[uri]

    def symbol_chain(self, uri, pos):
        """Document symbols enclosing `pos`, outermost first."""
        chain, syms = [], self.symbols(uri)
        while True:
            hit = next((s for s in syms if contains(s["range"], pos)), None)
            if not hit:
                return chain
            chain.append(hit)
            syms = hit.get("children") or []

    def type_at(self, uri, pos):
        chain = self.symbol_chain(uri, pos)
        if chain and chain[-1]["kind"] in TYPE_KINDS:
            return as_item(uri, chain[-1])
        return None

    def item_at(self, uri, pos):
        return self.prepare(uri, pos) or self.type_at(uri, pos)

    def owner(self, uri, pos):
        """The item a mention at `pos` belongs to: the enclosing fn or type
        definition, or the self type of the enclosing impl block. None for
        mentions outside any item (`use` lines)."""
        for sym in reversed(self.symbol_chain(uri, pos)):
            start = sym["selectionRange"]["start"]
            if sym["kind"] in FN_KINDS:
                return self.prepare(uri, start)
            if sym["kind"] in TYPE_KINDS:
                return as_item(uri, sym)
            if sym["kind"] == IMPL_KIND:  # selectionRange of an impl block is its self type
                for def_uri, def_pos in norm_locations(self.lsp.request(
                    "textDocument/definition", {"textDocument": {"uri": uri}, "position": start}
                )):
                    return self.type_at(def_uri, def_pos)
                return None
        return None

    def referrers(self, item):
        """Items whose source mentions `item` -> [(peer_item, [(path, line) mention sites])]."""
        uri, pos = item["uri"], item["selectionRange"]["start"]
        peers = {}
        for ref in self.lsp.request(
            "textDocument/references",
            {"textDocument": {"uri": uri}, "position": pos, "context": {"includeDeclaration": False}},
            default=[],
        ):
            path = ref["uri"].removeprefix("file://")
            if not keep(path):
                continue
            peer = self.owner(ref["uri"], ref["range"]["start"])
            if peer is None:
                continue
            peers.setdefault(self.node_id(peer), (peer, []))[1].append((path, ref["range"]["start"]["line"]))
        return list(peers.values())

    def methods_of(self, item):
        """Functions declared in the item's own body (trait methods) and in its impl blocks."""
        uri, pos = item["uri"], item["selectionRange"]["start"]
        impls = norm_locations(self.lsp.request(
            "textDocument/implementation", {"textDocument": {"uri": uri}, "position": pos}
        ))
        methods = []
        for block_uri, block_pos in [(uri, pos), *impls]:
            if not keep(block_uri.removeprefix("file://")):
                continue
            chain = self.symbol_chain(block_uri, block_pos)
            for child in (chain[-1].get("children") or []) if chain else []:
                if child["kind"] in FN_KINDS:
                    peer = self.prepare(block_uri, child["selectionRange"]["start"])
                    if peer:
                        methods.append(peer)
        return methods

    def neighbors(self, item, callers):
        """One walk step from `item`: [(peer_item, edge_kind, [(path, line) sites])]."""
        if not is_fn(item):
            if callers:
                return [(peer, "ref", sites) for peer, sites in self.referrers(item)]
            return [(m, "member", []) for m in self.methods_of(item)]
        method = "callHierarchy/incomingCalls" if callers else "callHierarchy/outgoingCalls"
        out = []
        for call in self.lsp.request(method, {"item": item}, default=[]):
            peer = call["from" if callers else "to"]
            if peer["kind"] not in FN_KINDS:
                continue
            path = (peer if callers else item)["uri"].removeprefix("file://")  # fromRanges are the caller's
            out.append((peer, "call", [(path, r["start"]["line"]) for r in call.get("fromRanges", [])]))
        return out

    def goto(self, item, method):
        """Resolve a goto-style request into call-hierarchy items (self excluded)."""
        uri, pos = item["uri"], item["selectionRange"]["start"]
        peers = []
        for loc_uri, loc_pos in norm_locations(
            self.lsp.request(method, {"textDocument": {"uri": uri}, "position": pos})
        ):
            if loc_uri == uri and loc_pos["line"] == pos["line"]:
                continue
            peer = self.prepare(loc_uri, loc_pos)
            if peer:
                peers.append(peer)
        return peers

    def bridge(self, item, callers, is_root):
        """Trait decl <-> impl links relevant to the walk direction.

        Note: goto-implementation answers from impl sites too (listing all
        sibling impls), so it must only be asked on genuine declarations —
        sibling impls are runtime alternatives, not part of this call graph.
        Returns [(decl_item, impl_item, item_to_enqueue)].
        """
        decls = self.goto(item, "textDocument/declaration")
        links = []
        if callers:
            # calls dispatched through the trait reach this impl: walk the decl up
            for decl in decls:
                links.append((decl, item, decl))
            if is_root and not decls:
                # the root is a decl: direct calls to any impl are usages too
                for impl in self.goto(item, "textDocument/implementation"):
                    links.append((item, impl, impl))
        elif not decls:
            # a call lands on a decl: the runtime target is one of its impls
            for impl in self.goto(item, "textDocument/implementation"):
                links.append((item, impl, impl))
        return links

    def collect_view(self, root_item, direction, depth, max_nodes):
        callers = direction == "callers"
        root_id = self.node_id(root_item)
        edges = {}  # (caller_id, callee_id, kind) -> [(path, line) call/mention sites]
        in_view = {root_id}
        truncated = set()
        expanded = set()
        queue = [(root_item, 0)]
        while queue:
            item, d = queue.pop(0)
            iid = self.node_id(item)
            if iid in expanded:
                continue
            expanded.add(iid)
            if d >= depth or len(in_view) >= max_nodes:
                truncated.add(iid)
                continue
            for peer, kind, sites in self.neighbors(item, callers):
                if not keep(peer["uri"].removeprefix("file://")):
                    continue
                pid = self.node_id(peer)
                if pid == iid and kind == "ref":  # a type named in its own impl headers
                    continue
                edge = (pid, iid, kind) if callers else (iid, pid, kind)
                edges.setdefault(edge, []).extend(sites)
                in_view.add(pid)
                queue.append((peer, d + 1))
            if not is_fn(item):
                continue
            for decl, impl, enqueue in self.bridge(item, callers, iid == root_id):
                if not (keep(decl["uri"].removeprefix("file://")) and keep(impl["uri"].removeprefix("file://"))):
                    continue
                edges.setdefault((self.node_id(decl), self.node_id(impl), "impl"), [])
                in_view.update((self.node_id(decl), self.node_id(impl)))
                # a bridge hop is free: it is the same logical function
                queue.append((enqueue, d))
        log(f"  {direction}: {len(in_view)} nodes, {len(edges)} edges")
        return {"edges": edges, "in_view": in_view, "truncated": truncated}

    def enrich(self, node_id):
        node = self.nodes[node_id]
        item = node["item"]
        path = item["uri"].removeprefix("file://")
        rel = os.path.relpath(path, ROOT)
        start = item["range"]["start"]["line"]
        end = item["range"]["end"]["line"]
        lines = self.lines(path)[start : end + 1]
        clipped = len(lines) > SNIPPET_MAX_LINES
        hover = self.lsp.request(
            "textDocument/hover",
            {"textDocument": {"uri": item["uri"]}, "position": item["selectionRange"]["start"]},
            default={},
        )
        contents = hover.get("contents", "")
        if isinstance(contents, dict):
            contents = contents.get("value", "")
        elif isinstance(contents, list):
            contents = "\n\n".join(c if isinstance(c, str) else c.get("value", "") for c in contents)
        node.update(
            name=item["name"],
            path=rel,
            line=item["selectionRange"]["start"]["line"] + 1,
            crate=crate_of(rel),
            isType=not is_fn(item),
            detail=item.get("detail", ""),
            hover=contents,
            snippet={"start": start + 1, "lines": lines[:SNIPPET_MAX_LINES], "clipped": clipped},
        )

    def call_sites(self, sites):
        out = []
        for path, line in sorted(set(sites)):
            lines = self.lines(path)
            lo, hi = max(0, line - 2), min(len(lines), line + 3)
            out.append({
                "path": os.path.relpath(path, ROOT),
                "line": line + 1,
                "context_start": lo + 1,
                "lines": lines[lo:hi],
            })
        return out


def crate_of(rel):
    if "/src/" in rel:
        return rel.split("/src/")[0].split("/")[-1]
    return "qdrant"


def short_path(rel):
    tail = rel.split("/src/")[-1]
    parts = tail.split("/")
    return "/".join(parts[-2:]) if len(parts) > 1 else tail


def render_dot(nodes, view, root_id, crate_colors):
    lines = [
        "digraph callgraph {",
        '  rankdir=LR; splines=true; ranksep=0.7; nodesep=0.25; pad=0.3;',
        '  node [shape=box, fontname="Helvetica", fontsize=11, margin="0.15,0.08", color="#00000033"];',
        '  edge [color="#64748b", arrowsize=0.7];',
    ]
    for nid in sorted(view["in_view"], key=lambda n: int(n[1:])):
        node = nodes[nid]
        name = node["name"].replace("\\", "\\\\").replace('"', '\\"')
        where = short_path(node["path"]).replace("\\", "\\\\").replace('"', '\\"')
        style = "filled" if node["isType"] else "rounded,filled"  # square corners = type, rounded = fn
        extra = ', penwidth=2.2, color="#b45309"' if nid == root_id else ""
        lines.append(
            f'  {nid} [id="{nid}", label="{name}\\n{where}", style="{style}",'
            f' fillcolor="{crate_colors[node["crate"]]}"{extra}];'
        )
    for i, (a, b, kind) in enumerate(sorted(view["edges"])):
        attrs = f'id="E{i}_{kind}"'
        if kind == "impl":
            attrs += ', style=dashed, color="#94a3b8", arrowhead=empty'
        elif kind == "ref":
            attrs += ', color="#0d9488"'
        elif kind == "member":
            attrs += ', style=dotted, color="#94a3b8"'
        lines.append(f"  {a} -> {b} [{attrs}];")
    lines.append("}")
    return "\n".join(lines)


def layout_svg(dot_source):
    svg = subprocess.run(
        ["dot", "-Tsvg"], input=dot_source.encode(), capture_output=True, check=True
    ).stdout.decode()
    return svg[svg.index("<svg") :]


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="item name (fn/struct/enum/trait/...), module::path::item, or file.rs:line")
    parser.add_argument("--depth", type=int, default=4, help="call hops from the root (default 4)")
    parser.add_argument("--max-nodes", type=int, default=250, help="node cap per view (default 250)")
    parser.add_argument("--out", help="output HTML path (default target/callgraph/<fn>.html)")
    args = parser.parse_args()

    if re.search(r"\.rs:\d+$", args.target):
        path, line, col = position_in_file(args.target)
    else:
        path, line, col = find_item(args.target)

    lsp = Lsp()
    lsp.start()
    uri = "file://" + path
    lsp.notify(
        "textDocument/didOpen",
        {"textDocument": {"uri": uri, "languageId": "rust", "version": 0, "text": open(path).read()}},
    )
    lsp.wait_quiescent()

    collector = Collector(lsp)
    root = None
    for _ in range(5):  # rust-analyzer can need a beat right after quiescence
        root = collector.item_at(uri, {"line": line, "character": col})
        if root:
            break
        time.sleep(1)
    if not root:
        sys.exit("rust-analyzer found no function or type definition at that position")
    root_id = collector.node_id(root)

    log("collecting graphs...")
    views = {
        "callers": collector.collect_view(root, "callers", args.depth, args.max_nodes),
        "callees": collector.collect_view(root, "callees", args.depth, args.max_nodes),
    }

    log("enriching nodes (docs, snippets, call sites)...")
    all_ids = views["callers"]["in_view"] | views["callees"]["in_view"]
    for nid in all_ids:
        collector.enrich(nid)

    crates = sorted({collector.nodes[n]["crate"] for n in all_ids})
    crate_colors = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(crates)}

    out_views = {}
    for name, view in views.items():
        edges_json = []
        sites_json = {}
        for (a, b, kind), sites in sorted(view["edges"].items()):
            edges_json.append([a, b, kind])
            if sites:
                sites_json[f"{a}>{b}"] = collector.call_sites(sites)
        out_views[name] = {
            "svg": layout_svg(render_dot(collector.nodes, view, root_id, crate_colors)),
            "edges": edges_json,
            "sites": sites_json,
            "truncated": sorted(view["truncated"] & view["in_view"]),
            "nodeCount": len(view["in_view"]),
        }

    remote = git("remote", "get-url", "origin")
    m = re.search(r"github\.com[:/](.+?)(?:\.git)?$", remote)
    data = {
        "meta": {
            "root": root["name"],
            "rootId": root_id,
            "target": f"{collector.nodes[root_id]['path']}:{collector.nodes[root_id]['line']}",
            "commit": git("rev-parse", "HEAD"),
            "commitShort": git("rev-parse", "--short", "HEAD"),
            "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
            "github": f"https://github.com/{m.group(1)}" if m else "",
            "repoRoot": ROOT,
            "depth": args.depth,
            "maxNodes": args.max_nodes,
            "date": time.strftime("%Y-%m-%d %H:%M"),
            "crateColors": crate_colors,
        },
        "nodes": {
            nid: {k: v for k, v in collector.nodes[nid].items() if k != "item"}
            for nid in all_ids
        },
        "views": out_views,
    }

    out_path = args.out or os.path.join(ROOT, "target", "callgraph", f"{root['name']}.html")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    template = open(os.path.join(TOOL_DIR, "template.html")).read()
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    open(out_path, "w").write(template.replace("__DATA__", payload, 1))
    log(f"callers: {out_views['callers']['nodeCount']} nodes, "
        f"callees: {out_views['callees']['nodeCount']} nodes")
    print(f"file://{out_path}")
    lsp.proc.kill()


if __name__ == "__main__":
    main()
