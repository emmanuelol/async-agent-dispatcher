#!/bin/bash
set -euo pipefail

TARGET_DIR="${1:-.}"
echo "=== Scanning for blacklisted patterns in: $TARGET_DIR ==="

PATTERN="${BLACKLIST_REGEX:-}"

# Check for local gitignored blacklist file if env var is empty
if [ -z "$PATTERN" ] && [ -f "$TARGET_DIR/.blacklist_patterns" ]; then
  PATTERN=$(grep -v '^#' "$TARGET_DIR/.blacklist_patterns" | tr '\n' '|' | sed 's/|$//')
elif [ -z "$PATTERN" ] && [ -f ".blacklist_patterns" ]; then
  PATTERN=$(grep -v '^#' ".blacklist_patterns" | tr '\n' '|' | sed 's/|$//')
fi

# Fail closed if running in CI or REQUIRE_BLACKLIST=1 and no pattern configured
if [ -z "$PATTERN" ]; then
  if [ "${CI:-}" = "true" ] || [ "${REQUIRE_BLACKLIST:-0}" = "1" ]; then
    echo "ERROR: BLACKLIST_REGEX is not configured in CI environment! Failing closed."
    exit 1
  else
    echo "WARNING: No BLACKLIST_REGEX or .blacklist_patterns found. Skipping pattern scan in local development."
    PATTERN="^__DO_NOT_MATCH_ANYTHING_EMPTY_DEFAULT__$"
  fi
fi

MATCHES=$(grep -rnEi "$PATTERN" "$TARGET_DIR" --exclude-dir=.git --exclude-dir=.pytest_cache --exclude="*.txt" --exclude="check_blacklist.sh" --exclude=".blacklist_patterns" 2>/dev/null || true)

if [ -n "$MATCHES" ]; then
  echo "SECURITY VIOLATION: Blacklisted strings detected!"
  echo "$MATCHES"
  exit 1
fi

if git -C "$TARGET_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  DEV_LEAKS=$(git -C "$TARGET_DIR" ls-files scripts/dev 2>/dev/null || true)
  if [ -n "$DEV_LEAKS" ]; then
    echo "SECURITY VIOLATION: Internal dev tooling tracked in git!"
    echo "$DEV_LEAKS"
    exit 1
  fi
fi

echo "BLACK_LIST_CLEAN: No sensitive IP or credentials found."
