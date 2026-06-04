SHELL := /bin/bash
.DEFAULT_GOAL := help

SCRIPT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
WORKSPACE_ROOT := $(abspath $(SCRIPT_DIR)/../../..)
XCFRAMEWORK := $(SCRIPT_DIR)out/QdrantEdge.xcframework

# ── Setup ────────────────────────────────────────────────────────────────────

.PHONY: setup
setup: setup-homebrew setup-rust setup-targets ## Install all prerequisites

.PHONY: setup-homebrew
setup-homebrew: ## Install Homebrew dependencies (protobuf)
	@command -v brew >/dev/null 2>&1 || { echo "Homebrew not found. Install from https://brew.sh"; exit 1; }
	brew install protobuf

.PHONY: setup-rust
setup-rust: ## Install Rust toolchain (stable + nightly for tier-3 targets)
	@if ! command -v rustup >/dev/null 2>&1; then \
		echo "Installing rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
		echo 'NOTE: add  source "$$HOME/.cargo/env"  to your shell profile, or restart your shell.'; \
	fi
	rustup toolchain install nightly --profile minimal
	rustup component add rust-src --toolchain nightly
	@echo "Rust toolchain OK: $$(rustc --version 2>/dev/null || echo 'restart shell to activate')"

.PHONY: setup-targets
setup-targets: ## Install required Rust cross-compilation targets
	rustup target add \
		aarch64-apple-ios \
		aarch64-apple-ios-sim \
		x86_64-apple-ios \
		aarch64-apple-darwin \
		x86_64-apple-darwin
	@echo "tvOS/visionOS targets are tier-3 and will be built with nightly -Z build-std"

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
