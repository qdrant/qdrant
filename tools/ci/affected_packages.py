#!/usr/bin/env python3
"""Select the workspace packages whose tests can be affected by a set of changed files.

Prints nextest package arguments: `--workspace` or a list of `-p <name>`.

    tools/ci/affected_packages.py <base-rev>       # files changed between base-rev and HEAD
    tools/ci/affected_packages.py --files a b ...  # explicit file list

Everything is derived from `cargo metadata`. A package owns its directory; the root
package, whose directory is the whole repository, owns only the directories of its
target sources. A changed file selects its owner plus every workspace package depending
on it, directly or transitively, through any dependency kind. A file with no owner
selects the whole workspace.
"""

import json
import os
import subprocess
import sys


def workspace_packages():
    """Directory prefixes owned by each package, and its workspace dependencies."""
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"]
        )
    )
    root = metadata["workspace_root"]

    def relative_dir(path):
        directory = os.path.relpath(os.path.dirname(path), root)
        return "" if directory == "." else directory + "/"

    packages = {}
    for package in metadata["packages"]:
        prefixes = {relative_dir(package["manifest_path"])}
        if prefixes == {""}:
            prefixes = {relative_dir(target["src_path"]) for target in package["targets"]}
        prefixes.discard("")
        dependencies = {dep["name"] for dep in package["dependencies"] if "path" in dep}
        packages[package["name"]] = (prefixes, dependencies)
    return packages


def owner(path, packages):
    matches = [
        (len(prefix), name)
        for name, (prefixes, _) in packages.items()
        for prefix in prefixes
        if path.startswith(prefix)
    ]
    return max(matches, default=(0, None))[1]


def affected(paths, packages):
    """Selected packages, or `None` for the whole workspace."""
    selected = set()
    for path in paths:
        name = owner(path, packages)
        if name is None:
            return None
        selected.add(name)

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
