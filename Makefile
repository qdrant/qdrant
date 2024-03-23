fmt-check:
	cargo +nightly fmt --all -- --check

fmt-check:
	cargo +nightly fmt --all

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

clippy-fix:
	cargo clippy --workspace --all-targets --all-features --fix
