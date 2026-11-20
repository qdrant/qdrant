#!/usr/bin/env bash
# Pre-publish gate, run by `make publish` before the binding is regenerated.
# The published archive carries a binding generated on this machine, so the
# inputs to that generation must be exactly the reviewed ones:
#
#  1. the working tree is clean (the binding is generated from it);
#  2. UNIFFI_BINDGEN_DART is built from the generator commit CI pins
#     (UNIFFI_DART_REF in .github/workflows/edge-dart.yml), from a clean tree;
#  3. the engine pins match the release (tool/check_pins.sh);
#  4. that release was built from the same Rust sources as HEAD, so the
#     engine carries the fixes the binding and CHANGELOG describe.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
repo_root="$(git rev-parse --show-toplevel)"
fail() { echo "error: $*" >&2; exit 1; }

# 1. Clean tree.
[ -z "$(git -C "$repo_root" status --porcelain)" ] \
  || fail "the working tree has uncommitted changes; publish from a clean checkout"

# 2. Generator provenance.
bindgen="${UNIFFI_BINDGEN_DART:-}"
[ -n "$bindgen" ] && [ -x "$bindgen" ] \
  || fail "set UNIFFI_BINDGEN_DART to the uniffi_bindgen_dart binary"
want_ref="$(sed -nE 's/^ *UNIFFI_DART_REF: *([0-9a-f]{40}).*/\1/p' "$repo_root/.github/workflows/edge-dart.yml")"
[ -n "$want_ref" ] || fail "cannot read UNIFFI_DART_REF from edge-dart.yml"
gen_repo="$(git -C "$(dirname "$bindgen")" rev-parse --show-toplevel 2>/dev/null)" \
  || fail "$bindgen is not inside a uniffi-dart checkout"
got_ref="$(git -C "$gen_repo" rev-parse HEAD)"
[ "$got_ref" = "$want_ref" ] \
  || fail "the generator checkout is at $got_ref, CI pins $want_ref"
[ -z "$(git -C "$gen_repo" status --porcelain -- src Cargo.toml Cargo.lock)" ] \
  || fail "the generator checkout $gen_repo has uncommitted changes"
[ -z "$(find "$gen_repo/src" -newer "$bindgen" -print -quit)" ] \
  || fail "$bindgen is older than the generator sources; rebuild it"

# 3. Pins.
./tool/check_pins.sh

# 4. The release's sources.
hook=hook/build.dart
tag="$(sed -nE "s/^const _releaseTag = '([^']+)';/\1/p" "$hook")"
remote="$(sed -nE "s#^ *'(https://github.com/[^']+)/releases/download/\\\$_releaseTag';#\1#p" "$hook").git"
tag_sha="$(git ls-remote --tags "$remote" "refs/tags/$tag^{}" | cut -f1)"
[ -n "$tag_sha" ] || tag_sha="$(git ls-remote --tags "$remote" "refs/tags/$tag" | cut -f1)"
[ -n "$tag_sha" ] || fail "cannot resolve tag $tag in $remote"
git -C "$repo_root" cat-file -e "$tag_sha^{commit}" 2>/dev/null \
  || git -C "$repo_root" fetch --quiet --no-tags "$remote" "$tag_sha" \
  || fail "cannot fetch $tag_sha from $remote"
if ! git -C "$repo_root" diff --quiet "$tag_sha" HEAD -- Cargo.toml Cargo.lock lib ':(exclude)lib/edge/dart'; then
  echo "error: $tag ($tag_sha) was built from different Rust sources than HEAD:" >&2
  git -C "$repo_root" diff --stat "$tag_sha" HEAD -- Cargo.toml Cargo.lock lib ':(exclude)lib/edge/dart' >&2
  exit 1
fi
echo "publish inputs check out: generator $want_ref, $tag built from HEAD's sources"
