#!/usr/bin/env bash
# 已更名为 prep_tpcc_cluster.sh（仅预处理，不跑双端 TPCC）。
# 保留本文件以免旧命令失效。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "NOTE: run_tpcc_cluster.sh 已改名为 prep_tpcc_cluster.sh（不跑 TPCC）" >&2
exec bash "${SCRIPT_DIR}/prep_tpcc_cluster.sh" "$@"
