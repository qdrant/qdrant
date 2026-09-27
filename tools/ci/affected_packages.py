#!/usr/bin/env python3
"""Select the workspace packages whose tests can be affected by a set of changed files.

Prints nextest package arguments: `--workspace`, a list of `-p <name>`, or nothing when
no Rust tests are affected.

    tools/ci/affected_packages.py <base-rev>       # files changed between base-rev and HEAD
    tools/ci/affected_packages.py --files a b ...  # explicit file list

A changed file selects the package owning it plus every workspace package depending on
it, directly or transitively, through any dependency kind. Files that belong to no
package fall back to the full workspace unless they are known to be irrelevant to Rust
tests.
"""

import json
import subprocess
import sys

# Paths that never affect Rust tests
IGNORED = (
    "docs/",
    "tests/",
    "openapi/",
    "pkg/",
    "tools/",
    ".github/",
    "README.md",
    "LICENSE",
    "Dockerfile",
    "shell.nix",
    "clippy.toml",
    "rustfmt.toml",
    ".gitignore",
    ".dockerignore",
)

# Paths outside `lib/*` read by the root `qdrant` package
ROOT_OWNED = ("src/", "config/")

# Reads of another package's files that are not a cargo dependency: path prefix -> readers
EXTRA_READERS = {
    "lib/api/src/grpc/proto/": ["uio-grpc-client"],
}

# Paths that change how everything builds or runs; must stay ahead of `IGNORED`
FULL_RUN = (
    "tools/ci/",
    ".github/workflows/rust.yml",
    ".github/actions/",
)


def workspace_packages():
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"]
        )
    )
    root = metadata["workspace_root"].rstrip("/") + "/"
    packages = {}
    for package in metadata["packages"]:
        manifest_dir = package["manifest_path"][len(root) :].rsplit("Cargo.toml", 1)[0]
        dependencies = {dep["name"] for dep in package["dependencies"] if "path" in dep}
        packages[package["name"]] = (manifest_dir, dependencies)
    return packages


def owners(path, packages):
    """Packages directly affected by `path`, or `None` if the whole workspace is."""
    if path.startswith(FULL_RUN):
        return None
    extra = [
        reader for prefix, readers in EXTRA_READERS.items() if path.startswith(prefix) for reader in readers
    ]
    if path.startswith(ROOT_OWNED):
        return {"qdrant", *extra}

    owner = max(
        (name for name, (manifest_dir, _) in packages.items() if manifest_dir and path.startswith(manifest_dir)),
        key=lambda name: len(packages[name][0]),
        default=None,
    )
    if owner is not None:
        return {owner, *extra}
    if path.startswith(IGNORED):
        return set()
    return None


def affected(paths, packages):
    selected = set()
    for path in paths:
        direct = owners(path, packages)
        if direct is None:
            return None
        selected |= direct

    dependents = {name: set() for name in packages}
    for name, (_, dependencies) in packages.items():
        for dependency in dependencies & dependents.keys():
            dependents[dependency].add(name)

    queue = list(selected)
    while queue:
        for dependent in dependents[queue.pop()] - selected:
            selected.add(dependent)
            queue.append(dependent)
    return selected


def main():
    if sys.argv[1:2] == ["--files"]:
        paths = sys.argv[2:]
    else:
        paths = subprocess.check_output(
            ["git", "diff", "--name-only", sys.argv[1], "HEAD"], text=True
        ).split()

    selected = affected(paths, workspace_packages())
    if selected is None:
        print("--workspace")
    else:
        print(" ".join(f"-p {name}" for name in sorted(selected)))


if __name__ == "__main__":
    main()
