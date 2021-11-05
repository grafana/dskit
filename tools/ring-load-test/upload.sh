#!/bin/bash

GOOS=linux GOARCH=amd64 go build -o ring-load-test-linux .

kubectl cp -n cortex-dedicated-11 ring-load-test-linux store-gateway-0:/tmp/ring-load-test-linux
kubectl cp -n cortex-dedicated-11 ring-load-test-linux store-gateway-1:/tmp/ring-load-test-linux
kubectl cp -n cortex-dedicated-11 ring-load-test-linux store-gateway-2:/tmp/ring-load-test-linux
kubectl cp -n cortex-dedicated-11 ring-load-test-linux store-gateway-3:/tmp/ring-load-test-linux
kubectl cp -n cortex-dedicated-11 ring-load-test-linux store-gateway-4:/tmp/ring-load-test-linux
