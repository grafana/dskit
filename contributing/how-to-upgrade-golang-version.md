# How to upgrade Golang version

1. `go mod edit -go=1.23`
1. `go mod vendor && go mod tidy`
1. Change active Go version and supported Go versions in `.github/workflows/test-build.yml`
