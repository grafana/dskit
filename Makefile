# put tools at the root of the folder
PATH := $(CURDIR)/.tools/bin:$(PATH)

.PHONY: test
test:
	go test -tags netgo -timeout 30m -race -count 1 ./...

.PHONY: lint
lint: .tools/bin/misspell .tools/bin/faillint .tools/bin/golangci-lint
	misspell -error README.md LICENSE

	# Configured via .golangci.yml.
	golangci-lint run

	# Ensure no blocklisted package is imported.
	faillint -paths "github.com/bmizerany/assert=github.com/stretchr/testify/assert,\
		golang.org/x/net/context=context,\
		sync/atomic=go.uber.org/atomic,\
		github.com/prometheus/client_golang/prometheus.{MultiError}=github.com/prometheus/prometheus/tsdb/errors.{NewMulti}" ./...

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

.tools:
	mkdir -p .tools/

.tools/bin/misspell: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/client9/misspell/cmd/misspell@v0.3.4

.tools/bin/faillint: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/fatih/faillint@v1.5.0

.tools/bin/golangci-lint: .tools
	GOPATH=$(CURDIR)/.tools go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.41.1

.drone/drone.yml: .drone/drone.jsonnet .drone/backend-enterprise.libsonnet
	# Drones jsonnet formatting causes issues where arrays disappear
	drone jsonnet --source $< --target $@ --stream --format=false
	drone lint --trusted $@
