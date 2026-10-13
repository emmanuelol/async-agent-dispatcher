#!/bin/bash
set -euo pipefail

IMAGE="${IMAGE:-async-agent-dispatcher:latest}"
TRIVY_SEVERITY="${TRIVY_SEVERITY:-HIGH,CRITICAL}"
TRIVY_EXIT_CODE="${TRIVY_EXIT_CODE:-1}"
TRIVY_IGNORE_UNFIXED="${TRIVY_IGNORE_UNFIXED:-true}"
TRIVY_EXTRA_ARGS=()
if [ "$TRIVY_IGNORE_UNFIXED" = "true" ]; then
  TRIVY_EXTRA_ARGS+=(--ignore-unfixed)
fi

echo "=== Scanning image for prohibited files ==="
LEAKS=$(docker run --rm --entrypoint "" "$IMAGE" sh -c '
  find / -name "*.env" -o -name "sessions.json" 2>/dev/null | grep -v "/proc/" || true
  find /app -name "*.pem" -o -name "*.key" 2>/dev/null || true
')

if [ -n "$LEAKS" ]; then
  echo "ERROR: Found prohibited files in image:"
  echo "$LEAKS"
  exit 1
fi
echo "Prohibited files check passed."

echo "=== Checking image size ==="
RAW_SIZE=$(docker images "$IMAGE" --format "{{.Size}}")
echo "Image size: $RAW_SIZE"

if command -v trivy &>/dev/null; then
  echo "=== Running local Trivy scan ==="
  trivy image --severity "$TRIVY_SEVERITY" --exit-code "$TRIVY_EXIT_CODE" "${TRIVY_EXTRA_ARGS[@]}" "$IMAGE"
elif [ -e /var/run/docker.sock ]; then
  echo "=== Running containerized Trivy scan ==="
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
    aquasec/trivy image --severity "$TRIVY_SEVERITY" --exit-code "$TRIVY_EXIT_CODE" "${TRIVY_EXTRA_ARGS[@]}" "$IMAGE"
else
  echo "=== Trivy unavailable; skipping vulnerability scan ==="
fi
