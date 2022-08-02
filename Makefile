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
ifeq ($(UNAME_S), Linux)
	PROTO_ZIP=protoc-3.6.1-linux-x86_64.zip
endif
ifeq ($(UNAME_S), Darwin)
	PROTO_ZIP=protoc-3.6.1-osx-x86_64.zip
endif

.PHONY: test
test:
	go test -tags netgo -timeout 30m -race -count 1 ./...

.PHONY: lint
lint: .tools/bin/misspell .tools/bin/faillint .tools/bin/golangci-lint
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
clean:
	@# go mod makes the modules read-only, so before deletion we need to make them deleteable
	@chmod -R u+rwX .tools 2> /dev/null || true
	rm -rf .tools/

.PHONY: mod-check
mod-check:
	GO111MODULE=on go mod download
	GO111MODULE=on go mod verify
	GO111MODULE=on go mod tidy
	@git diff --exit-code -- go.sum go.mod

.PHONY: clean-protos
clean-protos:
	rm -rf $(PROTO_GOS)

.PHONY: protos
protos: .tools/bin/protoc .tools/bin/protoc-gen-gogoslick .tools/bin/protoc-gen-go $(PROTO_GOS)

%.pb.go:
	.tools/protoc/bin/protoc -I $(GOPATH):./vendor/github.com/gogo/protobuf:./vendor:./$(@D) --gogoslick_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

.PHONY: check-protos
check-protos: clean-protos protos
	@git diff --exit-code -- $(PROTO_GOS)

.tools:
	mkdir -p .tools/

.tools/bin/misspell: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/client9/misspell/cmd/misspell@v0.3.4

.tools/bin/faillint: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/fatih/faillint@v1.10.0

.tools/bin/golangci-lint: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.47.3

.tools/bin/protoc: .tools
ifeq ("$(wildcard .tools/protoc/bin/protoc)","")
	mkdir -p .tools/protoc
	cd .tools/protoc && curl -LO $(PROTO_PATH)$(PROTO_ZIP)
	unzip -n .tools/protoc/$(PROTO_ZIP) -d .tools/protoc/
endif

.tools/bin/protoc-gen-gogoslick: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0

.tools/bin/protoc-gen-go: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/golang/protobuf/protoc-gen-go@v1.3.1

.PHONY: drone
drone: .drone/drone.yml

.drone/drone.yml: .drone/drone.jsonnet
	# Drone's jsonnet formatting causes issues where arrays disappear
	drone jsonnet --source $< --target $@.tmp --stream --format=false
	drone sign --save grafana/dskit $@.tmp
	drone lint --trusted $@.tmp
	# When all passes move to correct destination
	mv $@.tmp $@
