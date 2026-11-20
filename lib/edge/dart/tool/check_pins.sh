#!/usr/bin/env bash
# Checks that hook/build.dart's _sha256 pins match the checksums.txt of the
# release named by _releaseTag, for every archive in either. Run before
# publishing: a version whose pins do not match its release cannot be built by
# anyone who downloads the engine.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
hook=hook/build.dart

tag="$(sed -nE "s/^const _releaseTag = '([^']+)';/\1/p" "$hook")"
base="$(sed -nE "s#^ *'(https://github.com/[^']+)/releases/download/\\\$_releaseTag';#\1#p" "$hook")"
[ -n "$tag" ] && [ -n "$base" ] || { echo "error: cannot parse _releaseTag/_releaseBase from $hook" >&2; exit 1; }

url="$base/releases/download/$tag/checksums.txt"
released="$(curl -fsSL "$url" | awk '{sub(/^qdrant-edge-ffi-/, "", $2); sub(/\.tar\.gz$/, "", $2); print $2, $1}' | LC_ALL=C sort)" \
  || { echo "error: cannot fetch $url (is $tag released?)" >&2; exit 1; }

# The pin table is `'<os_arch>':` on one line and `'<sha>',` on the next.
pinned="$(awk "/^const _sha256/,/^};/" "$hook" \
  | grep -oE "'[a-z0-9_]+'" | tr -d "'" | paste - - | LC_ALL=C sort | tr '\t' ' ' || true)"

[ -n "$pinned" ] || { echo "error: cannot parse the _sha256 table in $hook" >&2; exit 1; }

if [ "$released" != "$pinned" ]; then
  echo "error: _sha256 in $hook does not match $url" >&2
  diff <(echo "$pinned") <(echo "$released") >&2 || true
  exit 1
fi
echo "pins match $tag ($(echo "$pinned" | wc -l | tr -d ' ') archives)"
