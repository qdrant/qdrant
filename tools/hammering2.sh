#!/usr/bin/env bash

msg() {
	printf "\x1b[1;33mHammering: $(git rev-parse --short HEAD): %s\x1b[0m\n" "$*"
}

msg "building"
cargo test -p collection --lib --no-run || { msg "build failed, skip"; exit 125; }
echo

msg "running"
for i in 1 2 3; do
	cargo test -p collection --lib collection_manager::collection_updater -- --nocapture
	RC=$?
	case $RC in
		0) msg "run $i/3: ok"; echo;;
		139) msg "run $i/3: segfault"; exit 1;;
		*) msg "run $i/3: unecpected fail (rc=$RC), skip"; exit 125;;
	esac
done

msg "good"
exit 0
