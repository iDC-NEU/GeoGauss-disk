#!/usr/bin/env bash
# 兼容旧名：等同于 import_smoke_data.sh both
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/import_smoke_data.sh" both
