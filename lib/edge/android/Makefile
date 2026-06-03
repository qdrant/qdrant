SHELL := /bin/bash
.DEFAULT_GOAL := help

SCRIPT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
WORKSPACE_ROOT := $(abspath $(SCRIPT_DIR)/../../..)
AAR := $(SCRIPT_DIR)qdrant-edge/build/outputs/aar/qdrant-edge-release.aar

# ── Setup ────────────────────────────────────────────────────────────────────

.PHONY: setup
setup: setup-homebrew setup-rust setup-targets setup-ndk ## Install all prerequisites

.PHONY: setup-homebrew
setup-homebrew: ## Install Homebrew dependencies (protobuf)
	@command -v brew >/dev/null 2>&1 || { echo "Homebrew not found. Install from https://brew.sh"; exit 1; }
	brew install protobuf

.PHONY: setup-rust
setup-rust: ## Install Rust toolchain + cargo-ndk
	@if ! command -v rustup >/dev/null 2>&1; then \
		echo "Installing rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
		echo 'NOTE: add  source "$$HOME/.cargo/env"  to your shell profile, or restart your shell.'; \
	fi
	cargo install cargo-ndk --version ^3.5 --locked
	@echo "Rust toolchain OK: $$(rustc --version 2>/dev/null || echo 'restart shell to activate')"

.PHONY: setup-targets
setup-targets: ## Install required Rust cross-compilation targets for Android
	rustup target add \
		aarch64-linux-android \
		x86_64-linux-android
	@echo "Note: 32-bit targets (armv7/x86) are excluded due to upstream crate limitations"

.PHONY: setup-ndk
setup-ndk: ## Check Android NDK availability
	@if [ -n "$${ANDROID_NDK_HOME:-}" ]; then \
		echo "NDK found: $$ANDROID_NDK_HOME"; \
	elif [ -n "$${ANDROID_HOME:-}" ]; then \
		NDK_DIR=$$(ls -d "$$ANDROID_HOME/ndk/"* 2>/dev/null | sort -V | tail -1 || true); \
		if [ -n "$$NDK_DIR" ]; then \
			echo "NDK found: $$NDK_DIR"; \
		else \
			echo "No NDK found. Install via: sdkmanager --install 'ndk;27.0.12077973'"; \
			exit 1; \
		fi; \
	else \
		echo "ERROR: ANDROID_NDK_HOME or ANDROID_HOME must be set."; \
		echo "  Install Android SDK + NDK, then export ANDROID_HOME=/path/to/sdk"; \
		exit 1; \
	fi

# ── Build ────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Build native libs + Kotlin bindings (release-mobile profile)
	@bash $(SCRIPT_DIR)build-aar.sh

.PHONY: build-debug
build-debug: ## Build native libs + Kotlin bindings (debug)
	@bash $(SCRIPT_DIR)build-aar.sh --debug

.PHONY: aar
aar: build ## Build the AAR (release-mobile, requires Gradle / Android SDK)
	cd $(SCRIPT_DIR) && ./gradlew :qdrant-edge:assembleRelease
	@echo ""
	@echo "AAR built: $(AAR)"

.PHONY: aar-debug
aar-debug: build-debug ## Build the AAR (debug)
	cd $(SCRIPT_DIR) && ./gradlew :qdrant-edge:assembleDebug

# ── Info ─────────────────────────────────────────────────────────────────────

.PHONY: size
size: ## Show per-ABI .so size breakdown
	@echo "=== Per-ABI native library sizes ==="
	@for abi in arm64-v8a x86_64; do \
		so="$(SCRIPT_DIR)qdrant-edge-ffi/src/main/jniLibs/$$abi/libqdrant_edge_ffi.so"; \
		if [ -f "$$so" ]; then \
			printf "  %-16s %s\n" "$$abi:" "$$(du -sh "$$so" | cut -f1)"; \
		fi; \
	done
	@if [ -f "$(AAR)" ]; then \
		echo ""; \
		echo "=== AAR size ==="; \
		du -sh "$(AAR)"; \
	fi

# ── Clean ────────────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove build outputs
	rm -rf $(SCRIPT_DIR)qdrant-edge-ffi/src/main/jniLibs/*/libqdrant_edge_ffi.so
	rm -rf $(SCRIPT_DIR)qdrant-edge-ffi/src/main/kotlin
	rm -rf $(SCRIPT_DIR)qdrant-edge-ffi/build
	rm -rf $(SCRIPT_DIR)qdrant-edge/build
	rm -rf $(SCRIPT_DIR)example/build
	rm -rf $(SCRIPT_DIR).gradle $(SCRIPT_DIR)build

# ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
