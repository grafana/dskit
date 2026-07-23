#!/usr/bin/env bash
set -o pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(realpath "${SCRIPT_DIR}/../../../")

# Parse args.
INDEX=""
TOTAL=""

while [[ $# -gt 0 ]]
do
  case "$1" in
    --total)
      TOTAL="$2"
      shift # skip --total
      shift # skip total value
      ;;
    --index)
      INDEX="$2"
      shift # skip --index
      shift # skip index value
      ;;
    *)  break
      ;;
  esac
done

if [[ -z "$INDEX" ]]; then
    echo "No --index provided."
    exit 1
fi

if [[ -z "$TOTAL" ]]; then
    echo "No --total provided."
    exit 1
fi

cd "${REPO_DIR}" || exit 1

WORK_DIR=$(mktemp -d)
trap 'rm -f "${WORK_DIR}"/*; rmdir "${WORK_DIR}" 2>/dev/null || true' EXIT

# Map package directories to import paths for modules covered by the root go.mod.
if ! go list -f '{{.Dir}}	{{.ImportPath}}' ./... > "${WORK_DIR}/pkg_dirs.txt"; then
    echo "Failed to list packages."
    exit 1
fi

# Discover *_test.go files that belong to root-module packages.
# Sorted for stable shard assignment across CI jobs.
# Use paths relative to REPO_DIR so exclusions do not match parent dirs
# (e.g. a worktree living under .agents/worktrees/).
: > "${WORK_DIR}/all_test_files.txt"
while IFS= read -r rel_file; do
    [[ -z "$rel_file" ]] && continue
    # Strip leading ./
    rel_file="${rel_file#./}"
    test_file="${REPO_DIR}/${rel_file}"
    dir=$(dirname "$test_file")
    if ! awk -F '	' -v d="$dir" '$1 == d { found=1; exit } END { exit !found }' "${WORK_DIR}/pkg_dirs.txt"; then
        # Nested module or otherwise outside root go list (e.g. examples).
        continue
    fi
    echo "$test_file" >> "${WORK_DIR}/all_test_files.txt"
done < <(
    find . -type f -name '*_test.go' \
        ! -path './vendor/*' \
        ! -path './.git/*' \
        ! -path './.agents/*' \
        ! -path './.claude/*' \
        | sort
)

GROUP_FILES=$(awk -v TOTAL="$TOTAL" -v INDEX="$INDEX" '(NR - 1) % TOTAL == INDEX' "${WORK_DIR}/all_test_files.txt")

if [[ -z "$GROUP_FILES" ]]; then
    echo "No test files found for group $INDEX of $TOTAL."
    exit 0
fi

echo "This group will run tests from the following files:"
echo "$GROUP_FILES"
echo

# Build per-package test name lists and file lists.
: > "${WORK_DIR}/packages.txt"
while IFS= read -r test_file; do
    [[ -z "$test_file" ]] && continue
    dir=$(dirname "$test_file")
    pkg=$(awk -F '	' -v d="$dir" '$1 == d { print $2; exit }' "${WORK_DIR}/pkg_dirs.txt")

    # Match what plain `go test` runs: Test*, Fuzz* (seed corpus), Example*.
    # Exclude TestMain — it is a harness, not a runnable test name for -run.
    tests=$(grep -E '^func (Test|Fuzz|Example)[A-Za-z0-9_]*\(' "$test_file" \
        | sed -E 's/^func ((Test|Fuzz|Example)[A-Za-z0-9_]*)\(.*/\1/' \
        | grep -vx 'TestMain' \
        | sort -u)
    if [[ -z "$tests" ]]; then
        echo "Skipping $test_file (no Test*/Fuzz*/Example* functions)."
        continue
    fi

    pkg_key=$(printf '%s' "$pkg" | sha256sum | cut -c1-16)
    echo "$test_file" >> "${WORK_DIR}/files_${pkg_key}.txt"
    echo "$tests" >> "${WORK_DIR}/tests_${pkg_key}.txt"
    echo "$pkg" >> "${WORK_DIR}/packages.txt"
done <<< "$GROUP_FILES"

PACKAGES=$(sort -u "${WORK_DIR}/packages.txt")
if [[ -z "$PACKAGES" ]]; then
    echo "No Test*/Fuzz*/Example* functions found for group $INDEX of $TOTAL."
    exit 0
fi

EXIT_CODE=0
FAILED_PACKAGES=""
MAX_ATTEMPTS=2
GO_TEST_FLAGS=(-tags netgo -timeout 30m -race -count 1)

for pkg in $PACKAGES; do
    pkg_key=$(printf '%s' "$pkg" | sha256sum | cut -c1-16)
    tests=$(sort -u "${WORK_DIR}/tests_${pkg_key}.txt")
    RUN_REGEX=$(echo "$tests" | awk '
        BEGIN { first=1 }
        {
            gsub(/[][(){}.^$*+?|\\]/, "\\\\&")
            if (first) { printf "^(%s", $0; first=0 } else { printf "|%s", $0 }
        }
        END { if (!first) printf ")$\n" }
    ')

    echo "Package $pkg (files in this group):"
    sort -u "${WORK_DIR}/files_${pkg_key}.txt"
    echo "Running:"
    echo "$tests"
    echo

    PKG_EXIT_CODE=0
    for ATTEMPT in $(seq 1 $MAX_ATTEMPTS); do
        if [[ $ATTEMPT -gt 1 ]]; then
            echo "Retrying failed package: $pkg"
            echo
        fi

        output=$(mktemp)
        go test "${GO_TEST_FLAGS[@]}" "$pkg" -run "$RUN_REGEX" 2>&1 | tee "$output"
        PKG_EXIT_CODE=${PIPESTATUS[0]}

        # A broken -run regex exits 0 with [no tests to run]; treat that as failure
        # when we expected tests from this package.
        if [[ $PKG_EXIT_CODE -eq 0 ]] && grep -q '\[no tests to run\]' "$output"; then
            echo "Expected tests to run for $pkg but got [no tests to run]."
            PKG_EXIT_CODE=1
        fi
        rm -f "$output"

        if [[ $PKG_EXIT_CODE -eq 0 ]]; then
            break
        fi
    done

    if [[ $PKG_EXIT_CODE -ne 0 ]]; then
        EXIT_CODE=1
        FAILED_PACKAGES="${FAILED_PACKAGES} ${pkg}"
    fi
done

if [[ -n "$FAILED_PACKAGES" ]]; then
    echo
    echo "FAILED PACKAGES:${FAILED_PACKAGES}"
fi

exit $EXIT_CODE
