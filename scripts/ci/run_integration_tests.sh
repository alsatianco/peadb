#!/usr/bin/env bash
# Run all integration files, or the explicit files supplied as arguments.
# The Python supervisor owns each test's process group, deadline and scratch data.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/run_integration_tests.py" "$@"
