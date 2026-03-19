#!/usr/bin/env -S uv run --script
"""
Amalgamation
(noun)
/əˌmælɡəˈmeɪʃən/

1. The process of combining multiple crates into a single one. Used to prevent
   namespace pollution when publishing to crates.io.
2. The result of amalgamating.
"""
# /// script
# dependencies = [ "tomlkit" ]
# ///

import functools
import os.path
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from collections.abc import Iterable
from pathlib import Path

import tomlkit

VERSION = "0.6.0"

# Assume this script is in <root>/lib/edge/publish/.
REPO_ROOT = Path(__file__).parent.parent.parent.parent

AMALGAMATION = Path(__file__).parent / "qdrant-edge"

PACKAGES_TO_INCLUDE = [
    "common",
    "edge",
    "gridstore",
    "posting_list",
    "quantization",
    "segment",
    "shard",
    "sparse",
    "wal",
]

EXCLUDED_DEPENDENCIES = {
    # build dependencies
    "prost-build",
    "tonic-build",
}


def main() -> None:
    root_manifest = tomlkit.loads(
        Path(REPO_ROOT / "Cargo.toml").read_text(encoding="utf-8")
    )
    packages = all_file_dependencies(REPO_ROOT / "lib/edge")

    shutil.rmtree(AMALGAMATION, ignore_errors=True)

    # Copy Rust sources.
    for pkg, (path, manifest) in packages.items():
        shutil.copytree(path / "src", AMALGAMATION / "src" / pkg)
        os.rename(
            AMALGAMATION / "src" / pkg / "lib.rs",
            AMALGAMATION / "src" / pkg / "mod.rs",
        )

    # Copy C sources and build scripts.
    shutil.copytree(
        REPO_ROOT / "lib/quantization/cpp", AMALGAMATION / "cpp/quantization"
    )
    shutil.copy2(
        REPO_ROOT / "lib/common/common/build.rs", AMALGAMATION / "build_common.rs"
    )
    shutil.copy2(REPO_ROOT / "lib/segment/build.rs", AMALGAMATION / "build_segment.rs")
    shutil.copy2(
        REPO_ROOT / "lib/quantization/build.rs", AMALGAMATION / "build_quantization.rs"
    )
    (AMALGAMATION / "build.rs").write_text(
        textwrap.dedent(
            """
            mod build_common;
            mod build_quantization;
            mod build_segment;
            fn main() {
                build_common::main();
                build_quantization::main();
                build_segment::main();
            }
            """
        ).lstrip(),
        encoding="utf-8",
    )

    # Copy resources.
    shutil.copytree(REPO_ROOT / "lib/segment/tokenizer", AMALGAMATION / "tokenizer")
    shutil.copy2(Path(__file__).parent / "README.md", AMALGAMATION / "README.md")

    # Write Cargo.toml.
    manifest = {
        "package": {
            "name": "qdrant-edge",
            "version": VERSION,
            "authors": ["Qdrant Team <info@qdrant.tech>"],
            "license": "Apache-2.0",
            "edition": "2024",
            "description": "A lightweight, in-process vector search engine designed for embedded devices, autonomous systems, and mobile agents.",
            "readme": "README.md",
            "homepage": "https://qdrant.tech/edge/",
            "repository": "https://github.com/qdrant/qdrant",
            "keywords": ["vector-search", "retrieval", "search", "qdrant", "embedded"],
            "categories": ["database-implementations"],
            "publish": True,
        },
        **gather_dependencies(
            root_manifest, [manifest for _, manifest in packages.values()]
        ),
    }
    (AMALGAMATION / "Cargo.toml").write_text(tomlkit.dumps(manifest), encoding="utf-8")

    # Write src/lib.rs.
    # It just re-exports everything.
    (AMALGAMATION / "src/lib.rs").write_text(
        textwrap.dedent(
            """
            #![allow(unexpected_cfgs)]
            #![allow(dead_code, unused_imports)]
            // #![warn(unnameable_types)] // TODO: re-enable when cleaning up the API
            pub use edge::*;
            """
        ).lstrip()
        + "".join(sorted(f"mod {pkg};\n" for pkg in packages.keys())),
        encoding="utf-8",
    )

    # Regex-based fixups.
    substitute(
        AMALGAMATION / "build_common.rs",
        # Make it callable from the main build.rs.
        (r"^fn main\(\)", "pub fn main()"),
        # Won't compile because we don't package benchmarks.
        (r".*cargo:rustc-link-arg-benches.*", ""),
    )
    substitute(
        AMALGAMATION / "build_segment.rs",
        # Make it callable from the main build.rs.
        (r"^fn main\(\)", "pub fn main()"),
        # Fix paths to C sources.
        (r'"src/', r'"src/segment/'),
        # Resolve C library name conflict.
        (
            r'builder\.compile\("simd_utils"\);',
            'builder.compile("simd_utils_segment");',
        ),
    )
    substitute(
        AMALGAMATION / "build_quantization.rs",
        # Make it callable from the main build.rs.
        (r"^fn main\(\)", "pub fn main()"),
        # Fix paths to C sources.
        (r'"cpp/', r'"cpp/quantization/'),
        # Resolve C library name conflict.
        (
            r'builder\.compile\("simd_utils"\);',
            'builder.compile("simd_utils_quantization");',
        ),
    )
    substitute(
        AMALGAMATION / "src/segment/common/anonymize.rs",
        # Remove code that doesn't compile.
        (r"^pub use macros::Anonymize;$\n", ""),
    )
    substitute(
        AMALGAMATION.glob("src/**/*.rs"),
        # Cleanup public API.
        (r"^#\[macro_export]$\n", ""),
    )

    # Fix doctests
    substitute(
        AMALGAMATION / "src/common/typelevel.rs",
        (r"^//! ```\n//! use ", "//! ```ignore\n//! use "),
    )
    substitute(
        AMALGAMATION.glob("src/edge/**/*.rs"),
        (r"^(/// .*)\bedge::", r"\1qdrant_edge::"),
    )

    # Remove unused code.
    shutil.rmtree(AMALGAMATION / "src/segment/index/hnsw_index/gpu")

    # Ast-grep-based fixups.
    RULES_TEMPLATE = (Path(__file__).parent / "ast-grep-rules.yaml").read_text(
        encoding="utf-8"
    )
    for package, (_, manifest) in packages.items():
        deps = (
            "|".join(
                dep
                for dep in manifest["dependencies"].keys()
                if dep in PACKAGES_TO_INCLUDE
            )
            or "some-nonexistent-package"
        )
        version = manifest["package"]["version"]
        rules = (
            RULES_TEMPLATE.replace("%PACKAGE%", package)
            .replace("%DEPS%", deps)
            .replace("%VERSION%", version)
        )
        # ast-grep prints stats but it doesn't print the directory it's working on.
        print(
            end=(package + ": ").ljust(max(len(pkg) for pkg in packages.keys()) + 2),
            file=sys.stderr,
            flush=True,
        )

        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", suffix=".yaml") as f:
            f.write(rules)
            f.flush()

            subprocess.run(
                [
                    shutil.which("ast-grep") or "ast-grep",
                    "scan",
                    "--update-all",
                    f"--rule={f.name}",
                    AMALGAMATION / "src" / package,
                ],
                check=True,
            )


def gather_dependencies(
    root_manifest: tomlkit.TOMLDocument, crates: list[tomlkit.TOMLDocument]
) -> dict[str, dict]:
    """Collect and merge dependencies from the provided manifests."""
    dependencies: dict[str, dict] = {}
    workspace_deps = {
        name: {"version": spec} if isinstance(spec, str) else spec
        for name, spec in root_manifest["workspace"]["dependencies"].items()
    }

    excluded = set()

    def add_specs(path: tuple[str, ...], specs: dict) -> None:
        dest = None
        for name, spec in specs.items():
            spec = {"version": spec} if isinstance(spec, str) else spec
            if name in EXCLUDED_DEPENDENCIES:
                excluded.add(name)
                continue
            if "path" in spec or spec.get("optional") is True:
                continue
            if spec.get("workspace") is True:
                # Merge two specs. Sloppy as it won't merge features.
                spec = {
                    **workspace_deps[name],
                    **{k: v for k, v in spec.items() if k != "workspace"},
                }
            dest = dest or functools.reduce(
                lambda d, k: d.setdefault(k, {}), path, dependencies
            )
            table = tomlkit.inline_table()
            table.update(dest.get(name, {}) | spec)
            dest[name] = table

    SECTIONS = ("dependencies", "build-dependencies", "dev-dependencies")
    for manifest in crates:
        for section in SECTIONS:
            add_specs((section,), manifest.get(section, {}))
        for target_name, target_manifest in manifest.get("target", {}).items():
            for section in SECTIONS:
                add_specs(
                    ("target", target_name, section), target_manifest.get(section, {})
                )
    return dependencies


def substitute(paths: Path | Iterable[Path], *replacements: tuple[str, str]) -> None:
    """Like `sed -i` but worse. Complains if some pattern is not found."""
    seen = [False] * len(replacements)
    regexes = [re.compile(pattern, flags=re.MULTILINE) for pattern, _ in replacements]
    if isinstance(paths, Path):
        paths = (paths,)
    for path in paths:
        text = path.read_text(encoding="utf-8")
        changed = False
        for i in range(len(replacements)):
            new_text = regexes[i].sub(replacements[i][1], text)
            if new_text != text:
                text = new_text
                changed = True
                seen[i] = True
        if changed:
            path.write_text(text, encoding="utf-8")
    for seen, (pattern, _) in zip(seen, replacements):
        assert seen, f"Pattern {pattern!r} not found"


def all_file_dependencies(root: Path) -> dict[str, tuple[Path, tomlkit.TOMLDocument]]:
    """Recursively collect Cargo.toml files for all dependencies.
    Returns mapping package_name -> (path_to_package, manifest).
    """
    seen: set[Path] = {root}
    stack = [root]
    result: dict[str, tuple[Path, tomlkit.TOMLDocument]] = {}

    while stack:
        path = stack.pop()
        manifest = tomlkit.loads((path / "Cargo.toml").read_text(encoding="utf-8"))
        name = manifest["package"]["name"]
        if name in PACKAGES_TO_INCLUDE:
            result[manifest["package"]["name"]] = (path, manifest)
        for spec in manifest["dependencies"].values():
            if isinstance(spec, dict) and isinstance(spec.get("path"), str):
                dep = Path(os.path.normpath(path / spec["path"]))
                if dep not in seen:
                    seen.add(dep)
                    stack.append(dep)

    return dict(sorted(result.items()))


if __name__ == "__main__":
    main()
