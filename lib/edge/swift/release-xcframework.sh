#!/usr/bin/env bash
#
# Prepare the Qdrant Edge XCFramework for SwiftPM remote distribution:
# build it, zip it, compute the SwiftPM checksum, and patch Package.swift's
# `releaseURL` / `releaseChecksum` constants so the package is installable from
# a Git tag via `url:` + `checksum:`.
#
# This does NOT publish anything. It produces a local zip + checksum and updates
# Package.swift. The actual release is a manual step:
#   1. Run this script.
#   2. Create a GitHub release tagged `edge-v<VERSION>` and upload
#      out/QdrantEdge.xcframework.zip as an asset (URL must match `releaseURL`).
#   3. Commit the patched Package.swift and tag the repo.
#
# Usage:
#   ./release-xcframework.sh [--debug] [--all-platforms]
#     (flags are forwarded to build-xcframework.sh)
#
# Verify locally without a network: this script prints the checksum and writes
# it into Package.swift; `QDRANT_EDGE_RELEASE` mode then uses it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EDGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT_DIR="$SCRIPT_DIR/out"
XCFRAMEWORK_DIR="$OUT_DIR/QdrantEdge.xcframework"
ZIP_PATH="$OUT_DIR/QdrantEdge.xcframework.zip"
PACKAGE_SWIFT="$SCRIPT_DIR/Package.swift"

# Single source of truth for the SDK version (see lib/edge/VERSION).
VERSION="$(tr -d '[:space:]' < "$EDGE_DIR/VERSION")"
RELEASE_URL="https://github.com/qdrant/qdrant/releases/download/edge-v${VERSION}/QdrantEdge.xcframework.zip"

for tool in swift zip; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "ERROR: required tool '$tool' not found in PATH." >&2
        exit 1
    }
done

# ── 1. Build the XCFramework ───────────────────────────────────────────────────

echo "==> Building XCFramework (version $VERSION)..."
bash "$SCRIPT_DIR/build-xcframework.sh" "$@"

if [ ! -d "$XCFRAMEWORK_DIR" ]; then
    echo "ERROR: expected $XCFRAMEWORK_DIR after build, not found." >&2
    exit 1
fi

# ── 2. Zip it ──────────────────────────────────────────────────────────────────

echo "==> Zipping XCFramework..."
rm -f "$ZIP_PATH"
# Zip from OUT_DIR so the archive contains `QdrantEdge.xcframework/...` at its
# root (SwiftPM requires the .xcframework at the top level of the zip).
( cd "$OUT_DIR" && zip -r -q "$ZIP_PATH" "QdrantEdge.xcframework" )

# ── 3. Compute the SwiftPM checksum ────────────────────────────────────────────

# `swift package compute-checksum` is the canonical way; SwiftPM verifies the
# downloaded zip against exactly this value.
echo "==> Computing checksum..."
CHECKSUM="$(cd "$SCRIPT_DIR" && swift package compute-checksum "$ZIP_PATH")"
# compute-checksum can exit 0 with empty output in edge cases; an empty checksum
# would patch a useless value and silently disable release mode. Guard it.
[ -n "$CHECKSUM" ] || {
    echo "ERROR: 'swift package compute-checksum' produced an empty checksum." >&2
    exit 1
}

# ── 4. Patch Package.swift ─────────────────────────────────────────────────────

echo "==> Patching Package.swift (url + checksum)..."
# Update the two committed constants in place.
perl -0pi -e "s{let releaseURL = \"[^\"]*\"}{let releaseURL = \"$RELEASE_URL\"}" "$PACKAGE_SWIFT"
perl -0pi -e "s{let releaseChecksum = \"[^\"]*\"}{let releaseChecksum = \"$CHECKSUM\"}" "$PACKAGE_SWIFT"

# perl -pi exits 0 even when the regex never matches (e.g. the constants were
# renamed). Verify the replacements actually landed so a no-op patch can't
# silently ship stale values.
grep -qF "let releaseURL = \"$RELEASE_URL\"" "$PACKAGE_SWIFT" || {
    echo "ERROR: failed to patch releaseURL in Package.swift (constant renamed?)." >&2
    exit 1
}
grep -qF "let releaseChecksum = \"$CHECKSUM\"" "$PACKAGE_SWIFT" || {
    echo "ERROR: failed to patch releaseChecksum in Package.swift (constant renamed?)." >&2
    exit 1
}

# ── Done ───────────────────────────────────────────────────────────────────────

cat <<EOF

Done.
  Zip:      $ZIP_PATH
  Checksum: $CHECKSUM
  URL:      $RELEASE_URL

Package.swift updated. To publish:
  1. Create GitHub release 'edge-v${VERSION}', upload the zip above as an asset.
  2. Verify the asset URL matches the 'releaseURL' above.
  3. Commit Package.swift and tag the repo.

To verify the remote target resolves locally (after uploading), run:
  QDRANT_EDGE_RELEASE=1 swift package resolve
EOF
