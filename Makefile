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
PROTO_PATH := https://github.com/protocolbuffers/protobuf/releases/download/v29.3/
ifeq ($(UNAME_S), Linux)
	PROTO_ZIP=protoc-29.3-linux-x86_64.zip
endif
ifeq ($(UNAME_S), Darwin)
	PROTO_ZIP=protoc-v29.3-osx-x86_64.zip
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
		github.com/go-kit/kit/log, \
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

PROTO_DEFS_CSPROTO := ./ring/ring.proto ./ring/partition_ring_desc.proto ./kv/mock.proto ./kv/memberlist/kv.proto
.PHONY: protos-csproto
protos-csproto:
	@for name in $(PROTO_DEFS_CSPROTO); do \
        .tools/protoc/bin/protoc \
        -I . \
        --go_out=paths=source_relative:. \
        --fastmarshal_out=apiversion=v2,paths=source_relative:. \
        --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:. \
       $${name}; \
    done

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
	GOPATH=$(CURDIR)/.tools go install github.com/fatih/faillint@v1.13.0

.tools/bin/golangci-lint: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.60.1

.tools/bin/protoc: .tools
ifeq ("$(wildcard .tools/protoc/bin/protoc)","")
	mkdir -p .tools/protoc
	cd .tools/protoc && curl -LO $(PROTO_PATH)$(PROTO_ZIP)
	unzip -n .tools/protoc/$(PROTO_ZIP) -d .tools/protoc/
endif

.tools/bin/protoc-gen-gogoslick: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.2

.tools/bin/protoc-gen-go: .tools
	GOPATH=$(CURDIR)/.tools go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.2

.tools/bin/protoc-gen-go-grpc: .tools
	GOPATH=$(CURDIR)/.tools go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

.tools/bin/protoc-gen-fastmarshal: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/CrowdStrike/csproto/cmd/protoc-gen-fastmarshal@v0.33.0