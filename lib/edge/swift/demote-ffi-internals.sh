#!/usr/bin/env bash
#
# Rewrite UniFFI's generated QdrantEdge.swift so its FFI plumbing types
# (FfiConverter*, Uniffi*, RustBuffer, ForeignBytes, InitializationResult,
# and the *_lift / *_lower free functions) are declared `internal` instead
# of `public`. The real domain types (EdgeShard, Point, Query, …) keep
# their `public` visibility.
#
# Safe because QdrantEdge.swift compiles into a single Swift module and
# the plumbing is only referenced from within that file.
#
# Usage: demote-ffi-internals.sh <path-to-QdrantEdge.swift>
# Idempotent: a marker on line 3 prevents double-processing.

set -euo pipefail

SWIFT_FILE="${1:-}"
if [ -z "$SWIFT_FILE" ] || [ ! -f "$SWIFT_FILE" ]; then
    echo "Usage: $0 <path-to-QdrantEdge.swift>" >&2
    exit 1
fi

MARKER="// Post-processed by demote-ffi-internals.sh: FFI plumbing demoted to internal."

if head -n 5 "$SWIFT_FILE" | grep -qF "$MARKER"; then
    echo "==> $(basename "$SWIFT_FILE") already processed; skipping."
    exit 0
fi

PUBLIC_BEFORE=$(grep -cE "^public " "$SWIFT_FILE" || true)

# `public static func` at column 0 is left untouched: UniFFI emits genuine
# class members (EdgeShard.load, UpdateOperation.upsertPoints) without
# indentation.
perl -i -pe '
    s/^public (struct|enum|protocol|func|class|final|typealias|var|let) (FfiConverter|Uniffi|uniffi|RustBuffer|ForeignBytes|InitializationResult)/internal $1 $2/;
    s/^public (func) ([A-Za-z_][A-Za-z0-9_]*_(lift|lower))\b/internal $1 $2/;
' "$SWIFT_FILE"

PUBLIC_AFTER=$(grep -cE "^public " "$SWIFT_FILE" || true)
DEMOTED=$((PUBLIC_BEFORE - PUBLIC_AFTER))

perl -i -pe '
    if ($. == 3 && !$done) {
        print "'"$MARKER"'\n";
        $done = 1;
    }
' "$SWIFT_FILE"

echo "==> Demoted $DEMOTED public declarations to internal ($PUBLIC_BEFORE → $PUBLIC_AFTER)."

if command -v swift-format >/dev/null 2>&1; then
    echo "==> Running swift-format..."
    swift-format --in-place "$SWIFT_FILE"
else
    echo "==> swift-format not found; skipping format pass."
fi
