# Contributing

Welcome! We're excited that you're interested in contributing. Below are some
basic guidelines.

## Workflow

Dskit follows a standard GitHub pull request workflow. If you're unfamiliar
with this workflow, read the very helpful [Understanding the GitHub
flow][github-flow] guide from GitHub.

[github-flow]: https://guides.github.com/introduction/flow/

You are welcome to create draft PRs at any stage of readiness - this can be
helpful to ask for assistance or to develop an idea. But before a piece of work
is finished it should:

* Be organised into one or more commits, each of which has a commit message
  that describes all changes made in that commit ('why' more than 'what' - we
  can read the diffs to see the code that changed).

* Each commit should build towards the whole - don't leave in back-tracks and
  mistakes that you later corrected.

* Have unit tests for new functionality or tests that would have caught the bug
  being fixed.

* Have a pull request title that follows [Conventional Commits](https://www.conventionalcommits.org/) format (e.g., `feat: Add new feature`, `fix: Resolve bug`, `docs: Update README`). This title will become the commit message when your PR is squashed and merged.

## Testing und Linting the Code

To lint the code:

```bash
make lint
```

To run the unit tests suite:

```bash
make test
```

### Dependency management

We use [Go modules] to manage dependencies on external packages. This requires
a working Go environment with version 1.16 or greater and git installed.

[Go modules]: https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more

To add or update a new dependency, use the `go get` command:

```bash
# Pick the latest tagged release.
go get example.com/some/module/pkg

# Pick a specific version.
go get example.com/some/module/pkg@vX.Y.Z
```

Tidy up the `go.mod` and `go.sum` files:

```bash
go mod tidy
git add go.mod go.sum
git commit
```

You have to commit the changes to `go.mod` and `go.sum` before submitting the
pull request.

Dskit uses the `goimports` tool (`go get golang.org/x/tools/cmd/goimports` to
install) to format the Go files, and sort imports. We use goimports with
`-local github.com/grafana/dskit` parameter, to put Dskit internal imports into
a separate group. We try to keep imports sorted into three groups:
imports from standard library, imports of 3rd party packages and internal
imports. Goimports will fix the order, but will keep existing newlines
between imports in the groups. We try to avoid extra newlines like that.

## How to add a package from another repository preserving history

### Prerequisites

* Install git-filter-repo package

    ```
    # Ubuntu/Debian
    apt-get install git-filter-repo

    # MacOSX
    brew install git-filter-repo
    ```

### Import particular files

In this case we would like add two particular files from Cortex, while
preserving their commit history.

```
# create new checkout of cortex
CHECKOUT_PATH=$(mktemp -d $TEMPLATE)
git clone git@github.com:cortexproject/cortex.git "${CHECKOUT_PATH}"

# move into directory
cd "${CHECKOUT_PATH}"

# filter to backoff files
git filter-repo --path pkg/util/backoff.go --path pkg/util/backoff_test.go

# add your dskit fork as remote
git remote add dskit git@github.com:<github-handle>/dskit.git
git fetch dskit

# now rebase to avoid conflicts
git rebase dskit/main

# push remotely and create a PR
git push dskit HEAD:add-backoff-with-history
```
