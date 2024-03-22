fmt:
	cargo +nightly fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings
