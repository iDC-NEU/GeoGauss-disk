#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 若父 shell 已 source env.sh，LD_LIBRARY_PATH 会在 bash 启动时加载 libreadline/libtinfo，
# 脚本结束时可能 segfault（exit 139）。用干净环境重新 exec 自身。
if [ -n "${LD_LIBRARY_PATH:-}" ] && [ -z "${EPOCH_DEPLOY_SANITIZED:-}" ]; then
    export EPOCH_DEPLOY_SANITIZED=1
    exec env -u LD_LIBRARY_PATH \
        EPOCH_DEPLOY_SANITIZED=1 \
        EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT:-}" \
        EPOCH_PROFILE="${EPOCH_PROFILE:-}" \
        EPOCH_PHYS_LOGICAL_GAP_LIMIT="${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}" \
        EPOCH_PHYSICAL_INTERVAL_MS="${EPOCH_PHYSICAL_INTERVAL_MS:-}" \
        EPOCH_NODE0_IP="${EPOCH_NODE0_IP:-}" \
        EPOCH_NODE1_IP="${EPOCH_NODE1_IP:-}" \
        EPOCH_NODE2_IP="${EPOCH_NODE2_IP:-}" \
        EPOCH_REMOTE_USER="${EPOCH_REMOTE_USER:-}" \
        EPOCH_REMOTE_BENCH_ROOT="${EPOCH_REMOTE_BENCH_ROOT:-}" \
        EPOCH_NODE2_REMOTE_USER="${EPOCH_NODE2_REMOTE_USER:-}" \
        EPOCH_NODE2_BENCH_ROOT="${EPOCH_NODE2_BENCH_ROOT:-}" \
        PATH="/usr/bin:/bin:/usr/local/bin" \
        bash "${BASH_SOURCE[0]}" "$@"
fi

# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

# deploy 仅用 rsync/ssh/cp
export PATH="/usr/bin:/bin:/usr/local/bin"
unset LD_LIBRARY_PATH

NODE_ID="${1:-}"
if [ -z "${NODE_ID}" ]; then
    echo "usage: $(basename "$0") REMOTE_NODE_ID" >&2
    echo "  1 -> ${EPOCH_NODE1_IP} (${EPOCH_REMOTE_USER})" >&2
    echo "  2 -> ${EPOCH_NODE2_IP} (${EPOCH_NODE2_REMOTE_USER})" >&2
    exit 2
fi
validate_node_id "${NODE_ID}"
if [ "${NODE_ID}" = "0" ]; then
    echo "node0 is local; deploy_to_remote is for node1/node2 only" >&2
    exit 2
fi

REMOTE="$(epoch_ssh_target "${NODE_ID}")"
REMOTE_ROOT="$(epoch_bench_root_for_node "${NODE_ID}")"
LOCAL_ROOT="${EPOCH_BENCH_ROOT}"
RUNTIME_LIBS="${LOCAL_ROOT}/epoch_deploy/runtime_libs"

stage_runtime_libs() {
    mkdir -p "${RUNTIME_LIBS}"
    local src
    for src in \
        /home/hcj2003/GeoGauss/dest/lib/libzmq.so* \
        /lib64/libzmq.so* \
        /lib64/libpgm-5.2.so* \
        /lib64/libncurses.so.5* \
        /lib64/libtinfo.so.5* \
        /lib64/libreadline.so.6* \
        /lib64/libhistory.so.6*; do
        for f in ${src}; do
            [ -e "${f}" ] || continue
            cp -a "${f}" "${RUNTIME_LIBS}/"
        done
    done
    if [ -z "$(ls -A "${RUNTIME_LIBS}" 2>/dev/null)" ]; then
        echo "WARN: no runtime libs staged under ${RUNTIME_LIBS}" >&2
    else
        echo "staged runtime libs: $(ls "${RUNTIME_LIBS}" | tr '\n' ' ')"
    fi
}

echo "=== deploy epoch build to node${NODE_ID} (${REMOTE}) ==="
echo "remote root: ${REMOTE_ROOT}"

stage_runtime_libs

ssh "${REMOTE}" "mkdir -p ${REMOTE_ROOT}/openGauss-server/mppdb_temp_install \
    ${REMOTE_ROOT}/openGauss-server/scripts/epoch_benchmark \
    ${REMOTE_ROOT}/data ${REMOTE_ROOT}/benchmarksql-5.0-epoch ${REMOTE_ROOT}/epoch_deploy/runtime_libs"

echo "syncing mppdb_temp_install (this may take a while) ..."
rsync -avz --delete \
    "${LOCAL_ROOT}/openGauss-server/mppdb_temp_install/" \
    "${REMOTE}:${REMOTE_ROOT}/openGauss-server/mppdb_temp_install/"

echo "syncing scripts/epoch_benchmark ..."
rsync -avz \
    "${SCRIPT_DIR}/" \
    "${REMOTE}:${REMOTE_ROOT}/openGauss-server/scripts/epoch_benchmark/"

echo "syncing epoch_deploy/runtime_libs (libzmq, libncurses.so.5, ...) ..."
rsync -avz \
    "${RUNTIME_LIBS}/" \
    "${REMOTE}:${REMOTE_ROOT}/epoch_deploy/runtime_libs/"

cat <<EOF

部署包已同步到 node${NODE_ID}。首次部署请先配置 SSH 免密：
  ssh-copy-id ${REMOTE}

然后在远程执行（node2 需 export EPOCH_LOCAL_NODE_ID=${NODE_ID}）：

  export EPOCH_BENCH_ROOT=${REMOTE_ROOT}
  export EPOCH_NODE0_IP=${EPOCH_NODE0_IP}
  export EPOCH_NODE1_IP=${EPOCH_NODE1_IP}
  export EPOCH_NODE2_IP=${EPOCH_NODE2_IP}
  export EPOCH_LOCAL_NODE_ID=${NODE_ID}
  bash ${REMOTE_ROOT}/openGauss-server/scripts/epoch_benchmark/configure_node.sh ${NODE_ID}
  bash ${REMOTE_ROOT}/openGauss-server/scripts/epoch_benchmark/start_node.sh ${NODE_ID}

三端 TPCC props 参考（benchmarksql-5.0-epoch/run）：
  node0: runWarehouseOffset=0,   runWarehouses=${EPOCH_RUN_WAREHOUSES_PER_NODE}, terminals=${EPOCH_TPCC_TERMINALS_NODE0}
  node1: runWarehouseOffset=${EPOCH_RUN_WAREHOUSES_PER_NODE}, runWarehouses=${EPOCH_RUN_WAREHOUSES_PER_NODE}, terminals=${EPOCH_TPCC_TERMINALS_NODE1}
  node2: runWarehouseOffset=$((EPOCH_RUN_WAREHOUSES_PER_NODE * 2)), runWarehouses=${EPOCH_RUN_WAREHOUSES_PER_NODE}, terminals=${EPOCH_TPCC_TERMINALS_NODE2}
  warehouses=${EPOCH_LOAD_WAREHOUSES}

EOF
