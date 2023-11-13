#!/usr/bin/env bash

set -euo pipefail

# for each path passed as argument:
for path in "$@"; do
  echo "Checking $path"
  cd "$(dirname $path)"
  echo "go mod download"
  GO111MODULE=on go mod download
  echo "go mod verify"
  GO111MODULE=on go mod verify
  echo "go mod tidy"
  GO111MODULE=on go mod tidy
  git diff --exit-code -- go.sum go.mod
  echo "$path OK"
done

