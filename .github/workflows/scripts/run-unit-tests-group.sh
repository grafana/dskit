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

# List all test packages.
ALL_TESTS=$(go list "${REPO_DIR}/..." | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL="$TOTAL" -v INDEX="$INDEX" '(NR - 1) % TOTAL == INDEX')

if [[ -z "$GROUP_TESTS" ]]; then
    echo "No packages found for group $INDEX of $TOTAL."
    exit 0
fi

echo "This group will run the following test packages:"
echo "$GROUP_TESTS"
echo

EXIT_CODE=0
FAILED_PACKAGES=""

# Run one package at a time so that a failure can be retried individually without re-running
# the entire group.
MAX_ATTEMPTS=2

for pkg in $GROUP_TESTS; do
    for ATTEMPT in $(seq 1 $MAX_ATTEMPTS); do
        if [[ $ATTEMPT -gt 1 ]]; then
            echo "Retrying failed package: $pkg"
            echo
        fi

        go test -tags netgo -timeout 30m -race -count 1 "$pkg"
        PKG_EXIT_CODE=$?

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
