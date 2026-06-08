#!/usr/bin/env bash

set -euo pipefail

# Vet each Go module passed as an argument. Nested modules are separate
# from the root module, so the root `go build ./...` / `go test ./...` / lint
# never compile them; `go vet` type-checks both package and test files here,
# catching API breaks in them that would otherwise pass CI.
for path in "$@"; do
  dir=$(dirname "$path")
  echo "Vetting module ${dir}"
  ( cd "${dir}" && go vet -tags netgo ./... )
done
