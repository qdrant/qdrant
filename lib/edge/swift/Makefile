SHELL := /bin/bash
.DEFAULT_GOAL := help

SCRIPT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
WORKSPACE_ROOT := $(abspath $(SCRIPT_DIR)/../../..)
XCFRAMEWORK := $(SCRIPT_DIR)out/QdrantEdge.xcframework

# ── Setup ────────────────────────────────────────────────────────────────────

.PHONY: setup
setup: setup-homebrew setup-targets ## Install all prerequisites

.PHONY: setup-homebrew
setup-homebrew: ## Install Homebrew dependencies (protobuf)
	@command -v brew >/dev/null 2>&1 || { echo "Homebrew not found. Install from https://brew.sh"; exit 1; }
	brew install protobuf

.PHONY: setup-rust
setup-rust: ## Install the nightly Rust toolchain (required — the workspace uses unstable std features)
	@set -e; \
	if ! command -v rustup >/dev/null 2>&1; then \
		echo "Installing rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
		echo 'NOTE: add  source "$$HOME/.cargo/env"  to your shell profile, or restart your shell, before running `make build`.'; \
	fi; \
	export PATH="$$HOME/.cargo/bin:$$PATH"; \
	rustup toolchain install nightly --profile minimal; \
	rustup component add rust-src --toolchain nightly; \
	echo "Rust toolchain OK: $$(rustc --version 2>/dev/null || echo 'restart shell to activate')"

.PHONY: setup-targets
setup-targets: setup-rust ## Install required Rust cross-compilation targets
	@# Make runs each target's recipe in its own shell, so the PATH export in
	@# setup-rust does NOT survive into this target. On a fresh machine (rustup
	@# just installed by setup-rust, shell not yet restarted) `rustup` is only on
	@# PATH via ~/.cargo/bin — export it here too, or `make setup` dies with
	@# "rustup: command not found" at this step.
	@export PATH="$$HOME/.cargo/bin:$$PATH"; \
	rustup target add --toolchain nightly \
		aarch64-apple-ios \
		aarch64-apple-ios-sim \
		x86_64-apple-ios \
		aarch64-apple-darwin \
		x86_64-apple-darwin
	@echo "All targets build on nightly (selected by rust-toolchain.toml). tvOS/visionOS are tier-3 (nightly + -Z build-std, via 'make build-all') and are NOT in the shipped package yet."

# ── Build ────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Build the XCFramework (iOS + macOS)
	@bash $(SCRIPT_DIR)build-xcframework.sh

.PHONY: build-all
build-all: ## Build the XCFramework (all platforms incl. tvOS/visionOS)
	@bash $(SCRIPT_DIR)build-xcframework.sh --all-platforms

.PHONY: build-debug
build-debug: ## Build the XCFramework (debug, iOS + macOS)
	@bash $(SCRIPT_DIR)build-xcframework.sh --debug

.PHONY: build-ci
build-ci: ## Build the XCFramework (fast PR-gate profile, iOS + macOS)
	@bash $(SCRIPT_DIR)build-xcframework.sh --ci

.PHONY: run-example
run-example: build ## Build the XCFramework, then run the Swift example
	# The example's Package.swift references the local out/ XCFramework via
	# path:, so it only resolves after `build` has produced it — hence the
	# `build` prerequisite above.
	@cd $(SCRIPT_DIR)example && swift run

.PHONY: test
test: build ## Build the XCFramework, then run the Swift package tests
	# Tests/QdrantEdgeTests/ imports only QdrantEdge (not the raw FFI module),
	# verifying that the public API surface is self-contained.
	@cd $(SCRIPT_DIR) && swift test

# ── Info ─────────────────────────────────────────────────────────────────────

.PHONY: size
size: ## Show XCFramework size breakdown
	@if [ ! -d "$(XCFRAMEWORK)" ]; then \
		echo "XCFramework not found. Run 'make build' first."; exit 1; \
	fi
	@echo "=== XCFramework total ==="
	@du -sh "$(XCFRAMEWORK)"
	@echo ""
	@echo "=== Per-platform static library sizes ==="
	@find "$(XCFRAMEWORK)" -name '*.a' -exec sh -c 'echo "$$(du -sh "$$1" | cut -f1)\t$$(basename $$(dirname $$(dirname "$$1")))"' _ {} \;
	@echo ""
	@echo "=== Architecture details ==="
	@find "$(XCFRAMEWORK)" -name '*.a' | while read lib; do \
		echo "--- $$(basename $$(dirname $$(dirname "$$lib"))) ---"; \
		lipo -info "$$lib"; \
	done

# ── Clean ────────────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove build outputs
	rm -rf $(SCRIPT_DIR)out

# ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
