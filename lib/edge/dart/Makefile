SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: build
build: ## Regenerate the Dart binding from the qdrant-edge-ffi crate
	./build.sh

.PHONY: test
test: ## Run the SDK tests (dart test runs the Native Assets build hook)
	dart pub get && dart test

.PHONY: example
example: ## Run the example
	cd example && dart pub get && dart run

.PHONY: publish
# The gate runs before `build`: it checks the inputs the binding is generated from.
publish: ## Check the publish inputs, regenerate the binding, then publish to pub.dev (needs pub.dev auth)
	./tool/check_publish.sh
	./build.sh
	dart pub publish

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
