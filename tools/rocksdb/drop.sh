#!/bin/sh
# This script removes rocksdb-related code from the code base

set -euo pipefail
SCRIPT_DIR=$(dirname "$0")

git checkout -- lib

for rule in "$SCRIPT_DIR"/rules/*.yml; do
	ast-grep scan --rule "$rule" lib --update-all
done

# ast-grep adds leftover empty lines. Remove them from the patch.
patch=$(mktemp)
trap 'rm -f "$patch"' EXIT
git -c diff.noprefix=true diff -- lib | grep -v '^+ *$' > "$patch"
git checkout -- lib
git apply -p0 --recount "$patch"

# Delete modules that are no longer referenced after removing `mod something;`.
git -c diff.noprefix=true diff -- lib | awk '
	/^diff --git / { file = $3  }
	/^-(pub )?mod .*;/ {
		mod = gensub(/^-?(pub )?mod (.*);$/, "\\2", "g");
		dir = gensub(/\/[^/]+$/, "", "g", file);
		print dir"/"mod".rs"
	}' | xargs rm

cargo +nightly fmt

git diff --shortstat lib
