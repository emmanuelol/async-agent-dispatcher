#!/bin/bash
set -euo pipefail

TARGET_DIR="${1:-.}"
echo "=== Scanning for blacklisted patterns in: $TARGET_DIR ==="

PATTERN="TC-001|TC-002|Branch_ID|Client_Vault|Active_Branches|Drop_Zone|Archive_ID|Master_Sales_Data|\+521472|@paseo\.com|emmanuelol\.org|Silao|OPENCLAW_TOKEN|GEMINI_API_KEY|ODOO_PASSWORD|facturacion\.emmanuelol\.org|kiosk\.emmanuelol\.org"

MATCHES=$(grep -rnEi "$PATTERN" "$TARGET_DIR" --exclude-dir=.git --exclude-dir=.pytest_cache --exclude="*.txt" --exclude="check_blacklist.sh" 2>/dev/null || true)

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
