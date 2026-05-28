SHELL = /usr/bin/env bash

# put tools at the root of the folder
PATH := $(CURDIR)/.tools/bin:$(PATH)
# We don't want find to scan inside a bunch of directories
DONT_FIND := -name vendor -prune -o -name .git -prune -o -name .cache -prune -o -name .tools -prune -o
# Generating proto code is automated.
PROTO_DEFS := $(shell find . $(DONT_FIND) -type f -name '*.proto' -print)
PROTO_GOS := $(patsubst %.proto,%.pb.go,$(PROTO_DEFS))
# Download the proper protoc version for Darwin (osx) and Linux.
# If you need windows for some reason it's at https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-win32.zip
UNAME_S := $(shell uname -s)
PROTO_PATH := https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/
# Pinned SHA256 hashes maintained by `make update-protoc-sha` (Renovate postUpgradeTasks hook).
PROTO_ZIP_SHA256_LINUX  := 6003de742ea3fcf703cfec1cd4a3380fd143081a2eb0e559065563496af27807
PROTO_ZIP_SHA256_DARWIN := 0decc6ce5beed07f8c20361ddeb5ac7666f09cf34572cca530e16814093f9c0c
ifeq ($(UNAME_S), Linux)
	PROTO_ZIP=protoc-3.6.1-linux-x86_64.zip
	PROTO_ZIP_SHA256=$(PROTO_ZIP_SHA256_LINUX)
endif
ifeq ($(UNAME_S), Darwin)
	PROTO_ZIP=protoc-3.6.1-osx-x86_64.zip
	PROTO_ZIP_SHA256=$(PROTO_ZIP_SHA256_DARWIN)
endif
GO_MODS=$(shell find . $(DONT_FIND) -type f -name 'go.mod' -print)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Display this help and any documented user-facing targets. Other undocumented targets may be present in the Makefile.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-50s %s\n", $$1, $$2 } /^##@/ { printf "\n%s\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: test
test: ## Runs Go tests
	go test -tags netgo -timeout 30m -race -count 1 ./...

.PHONY: test-benchmarks
test-benchmarks: ## Runs Go benchmarks with 1 iteration to make sure they still make sense
	go test -tags netgo -run=NONE -bench=. -benchtime=1x -timeout 30m ./...

.PHONY: lint
lint: .tools/bin/misspell .tools/bin/faillint .tools/bin/golangci-lint ## Runs misspell, golangci-lint, and faillint
	misspell -error README.md CONTRIBUTING.md LICENSE

	# Configured via .golangci.yml.
	golangci-lint run

	# Ensure no blocklisted package is imported.
	faillint -paths "github.com/bmizerany/assert=github.com/stretchr/testify/assert,\
		golang.org/x/net/context=context,\
		sync/atomic=go.uber.org/atomic,\
		github.com/go-kit/kit/log,\
		gopkg.in/yaml.v3=go.yaml.in/yaml/v3,\
		github.com/prometheus/client_golang/prometheus.{MustRegister}=github.com/prometheus/client_golang/prometheus/promauto,\
		github.com/prometheus/client_golang/prometheus.{NewCounter,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,NewHistogram,NewHistogramVec,NewSummary,NewSummaryVec}\
		=github.com/prometheus/client_golang/prometheus/promauto.With.{NewCounter,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,NewHistogram,NewHistogramVec,NewSummary,NewSummaryVec}"\
		./...

.PHONY: clean
clean: ## Removes the .tools/ directory
	@# go mod makes the modules read-only, so before deletion we need to make them deleteable
	@chmod -R u+rwX .tools 2> /dev/null || true
	rm -rf .tools/

.PHONY: mod-check
mod-check: ## Git diffs the go mod files
	@./.scripts/mod-check.sh $(GO_MODS)

.PHONY: clean-protos
clean-protos: ## Removes the proto files
	rm -rf $(PROTO_GOS)

.PHONY: protos
protos: .tools/bin/protoc .tools/bin/protoc-gen-gogoslick .tools/bin/protoc-gen-go $(PROTO_GOS) ## Creates proto files

%.pb.go:
	.tools/protoc/bin/protoc -I $(GOPATH):./vendor/github.com/gogo/protobuf:./vendor:./$(@D) --gogoslick_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

.PHONY: check-protos
check-protos: clean-protos protos ## Re-generates protos and git diffs them
	@git diff --exit-code -- $(PROTO_GOS)

.tools:
	mkdir -p .tools/

.tools/bin/misspell: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/client9/misspell/cmd/misspell@v0.3.4

.tools/bin/faillint: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/fatih/faillint@v1.15.0

GOLANGCI_LINT_VERSION := 2.9.0
.tools/bin/golangci-lint: .tools
	@set -e; \
	mkdir -p .tools/bin; \
	OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	ARCH=$$(go env GOARCH); \
	SLUG=golangci-lint-$(GOLANGCI_LINT_VERSION)-$$OS-$$ARCH; \
	BASE=https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION); \
	curl -sSfL "$$BASE/$$SLUG.tar.gz" -o ".tools/$$SLUG.tar.gz"; \
	curl -sSfL "$$BASE/golangci-lint-$(GOLANGCI_LINT_VERSION)-checksums.txt" -o ".tools/golangci-lint-checksums.txt"; \
	grep -E "[[:space:]]+$$SLUG\.tar\.gz$$" ".tools/golangci-lint-checksums.txt" > ".tools/$$SLUG.sha256"; \
	(cd .tools && sha256sum --check --strict "$$SLUG.sha256"); \
	tar -xzf ".tools/$$SLUG.tar.gz" -C .tools/bin --strip-components=1 "$$SLUG/golangci-lint"; \
	rm ".tools/$$SLUG.tar.gz" ".tools/golangci-lint-checksums.txt" ".tools/$$SLUG.sha256"

.tools/bin/protoc: .tools
ifeq ("$(wildcard .tools/protoc/bin/protoc)","")
	mkdir -p .tools/protoc
	cd .tools/protoc && curl -LO $(PROTO_PATH)$(PROTO_ZIP)
	# Verify the archive against the pinned SHA256 before unzip.
	cd .tools/protoc && echo "$(PROTO_ZIP_SHA256)  $(PROTO_ZIP)" | sha256sum --check --strict -
	unzip -n .tools/protoc/$(PROTO_ZIP) -d .tools/protoc/
endif

# Invoked by Renovate's postUpgradeTasks after it bumps the protoc version in
# this Makefile. Re-downloads both archives at the new version and rewrites the
# PROTO_ZIP_SHA256_LINUX and PROTO_ZIP_SHA256_DARWIN top-level constants.
.PHONY: update-protoc-sha
update-protoc-sha: ## Recompute pinned protoc archive SHA256 values from upstream.
	@set -e; \
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp" Makefile.new' EXIT; \
	base=$$(awk '/^PROTO_PATH := / {print $$3}' Makefile); \
	linux_zip=$$(awk -F= '/^[[:space:]]+PROTO_ZIP=protoc-.*-linux-x86_64\.zip$$/ {print $$2}' Makefile); \
	darwin_zip=$$(awk -F= '/^[[:space:]]+PROTO_ZIP=protoc-.*-osx-x86_64\.zip$$/  {print $$2}' Makefile); \
	[ -n "$$base" ] && [ -n "$$linux_zip" ] && [ -n "$$darwin_zip" ] \
	  || { echo "update-protoc-sha: failed to extract PROTO_PATH or PROTO_ZIP values from Makefile" >&2; exit 1; }; \
	curl -sSfL "$$base$$linux_zip"  -o "$$tmp/$$linux_zip"; \
	curl -sSfL "$$base$$darwin_zip" -o "$$tmp/$$darwin_zip"; \
	linux_sha=$$(sha256sum "$$tmp/$$linux_zip"  | awk '{print $$1}'); \
	darwin_sha=$$(sha256sum "$$tmp/$$darwin_zip" | awk '{print $$1}'); \
	awk -v ls="$$linux_sha" -v ds="$$darwin_sha" ' \
	  /^PROTO_ZIP_SHA256_LINUX[[:space:]]*:=/  { sub(/:=.*/, ":= " ls); print; next } \
	  /^PROTO_ZIP_SHA256_DARWIN[[:space:]]*:=/ { sub(/:=.*/, ":= " ds); print; next } \
	  { print } \
	' Makefile > Makefile.new && mv Makefile.new Makefile

.tools/bin/protoc-gen-gogoslick: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0

.tools/bin/protoc-gen-go: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/golang/protobuf/protoc-gen-go@v1.3.1
