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

# Fail closed: if nothing was demoted, the UniFFI symbol vocabulary likely changed
# (e.g. a version bump renamed FfiConverter*/RustBuffer/Uniffi* prefixes). Shipping with
# zero demotions would leave the FFI plumbing `public` in the consumer-facing module.
if [ "$DEMOTED" -eq 0 ]; then
    echo "ERROR: demote-ffi-internals.sh demoted 0 declarations." >&2
    echo "       The UniFFI-generated symbol vocabulary may have changed — update the" >&2
    echo "       demote patterns above so internal plumbing stays out of the public API." >&2
    exit 1
fi

# Positive post-condition: assert NO known-plumbing-shaped declaration is still
# `public`. The count-based guard above only fires on a TOTAL vocabulary change
# (0 demotions); it misses a PARTIAL leak where uniffi-bindgen adds or renames
# ONE plumbing family alongside the recognized ones — the known prefixes still
# demote plenty, so DEMOTED > 0 and the build passes while the new family leaks
# into the public surface. Re-scan for any surviving public plumbing so that
# partial case fails closed too.
REMAINING=$(grep -cE "^public (struct|enum|protocol|func|class|final|typealias|var|let) (FfiConverter|Uniffi|uniffi|RustBuffer|ForeignBytes|InitializationResult)" "$SWIFT_FILE" || true)
REMAINING_LIFTLOWER=$(grep -cE "^public func [A-Za-z_][A-Za-z0-9_]*_(lift|lower)\b" "$SWIFT_FILE" || true)
if [ "$REMAINING" -ne 0 ] || [ "$REMAINING_LIFTLOWER" -ne 0 ]; then
    echo "ERROR: $((REMAINING + REMAINING_LIFTLOWER)) FFI-plumbing declaration(s) remain \`public\` after demotion." >&2
    grep -nE "^public (struct|enum|protocol|func|class|final|typealias|var|let) (FfiConverter|Uniffi|uniffi|RustBuffer|ForeignBytes|InitializationResult)|^public func [A-Za-z_][A-Za-z0-9_]*_(lift|lower)\b" "$SWIFT_FILE" >&2 || true
    exit 1
fi

if command -v swift-format >/dev/null 2>&1; then
    echo "==> Running swift-format..."
    # Cosmetic only — never fail the build on it. A non-Apple `swift-format` may
    # be first in PATH (e.g. Chromium's depot_tools variant) and exit non-zero;
    # the generated bindings are already correct without formatting.
    swift-format --in-place "$SWIFT_FILE" || \
        echo "==> swift-format failed (non-fatal); leaving bindings unformatted." >&2
else
    echo "==> swift-format not found; skipping format pass."
fi
