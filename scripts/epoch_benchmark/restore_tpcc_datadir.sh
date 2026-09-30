#!/usr/bin/env bash
# 从 *_bak 整目录恢复 live（默认 --full-datadir）。
# 必须在停库状态下执行；prep_tpcc_cluster.sh 会自动调用。
#
# 背景：仅 rsync base/<oid>+tablespace 在 crash 后会残留脏 pg_xlog/pg_control，
# 导致启动 redo PANIC。整目录 bak→live 是当前稳妥做法。
# tpcc300：不再维护在同一 data dir；若需 300w，另 gs_initdb 新目录再 load。
#
# 可选（少用）：仅恢复当前库三路径
#   bash scripts/epoch_benchmark/restore_tpcc_datadir.sh --db-files-only
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

usage() {
    cat <<EOF
usage: $(basename "$0") [--no-stop-start] [--db NAME] [--full-datadir|--db-files-only]

  EPOCH_TPCC_DB=${EPOCH_TPCC_DB}（可用 --db 覆盖；整目录模式下主要作日志标记）

  默认：
    整目录 rsync *_bak/ → live/（含 pg_xlog、global/pg_control）

  --full-datadir    同默认（显式写出）
  --db-files-only   仅恢复 base/<oid> + 两个 tablespace/<oid>（旧行为；crash 后勿用）
  --no-stop-start   仅 rsync（调用方已停库）
  --db NAME         覆盖 EPOCH_TPCC_DB（test | tpcc300；仅 --db-files-only 时影响 OID）
EOF
}

DO_STOP_START=1
# 默认整目录；--db-files-only 切回三路径
FULL_DATADIR=1
while [ $# -gt 0 ]; do
    case "$1" in
        --no-stop-start)
            DO_STOP_START=0
            shift
            ;;
        --full-datadir)
            FULL_DATADIR=1
            shift
            ;;
        --db-files-only)
            FULL_DATADIR=0
            shift
            ;;
        --db)
            export EPOCH_TPCC_DB="${2:?--db needs NAME}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 2
            ;;
    esac
done

epoch_live_bak_paths() {
    local node_id="$1"
    case "${node_id}" in
        1)
            echo "${EPOCH_DATA_ROOT}/epoch-node0" "${EPOCH_DATA_ROOT}/epoch-node0_bak"
            ;;
        2)
            echo "${EPOCH_DATA_ROOT}/epoch-node2" "${EPOCH_DATA_ROOT}/epoch-node2_bak"
            ;;
        3)
            echo "${EPOCH_DATA_ROOT}/epoch-node1" "${EPOCH_DATA_ROOT}/epoch-node1_bak"
            ;;
        *)
            echo "ERROR: no restore mapping for node id=${node_id}" >&2
            return 1
            ;;
    esac
}

epoch_ensure_bak_exists() {
    local node_id="$1"
    local bak="$2"
    if [ -d "${bak}" ]; then
        return 0
    fi
    if epoch_is_local_node "${node_id}"; then
        echo "ERROR: missing backup dir ${bak} for $(epoch_node_display_name "${node_id}")" >&2
        return 1
    fi
    local rb remote remote_bak
    rb="$(epoch_bench_root_for_node "${node_id}")"
    remote="$(epoch_ssh_target "${node_id}")"
    remote_bak="${rb}/data/$(basename "${bak}")"
    if ! ssh -o BatchMode=yes "${remote}" "[ -d '${remote_bak}' ]"; then
        echo "ERROR: missing backup dir ${remote_bak} on $(epoch_node_display_name "${node_id}")" >&2
        return 1
    fi
}

# 整目录恢复（默认）
epoch_restore_full_datadir() {
    local node_id="$1"
    local live bak
    read -r live bak <<<"$(epoch_live_bak_paths "${node_id}")"
    epoch_ensure_bak_exists "${node_id}" "${bak}"

    echo "=== [full-datadir] restore $(epoch_node_display_name "${node_id}") (id=${node_id}): ${bak}/ -> ${live}/ ==="
    if epoch_is_local_node "${node_id}"; then
        mkdir -p "${live}"
        rsync -a --delete --progress "${bak}/" "${live}/"
    else
        local rb remote live_name bak_name
        rb="$(epoch_bench_root_for_node "${node_id}")"
        remote="$(epoch_ssh_target "${node_id}")"
        live_name="$(basename "${live}")"
        bak_name="$(basename "${bak}")"
        ssh -o BatchMode=yes "${remote}" \
            "mkdir -p ${rb}/data/${live_name} && \
             rsync -a --delete --progress ${rb}/data/${bak_name}/ ${rb}/data/${live_name}/"
    fi
}

# 旧：仅恢复当前库三处路径（crash 后不安全）
epoch_restore_db_files() {
    local node_id="$1"
    local live bak oid ver
    read -r live bak <<<"$(epoch_live_bak_paths "${node_id}")"
    oid="$(epoch_tpcc_db_oid_for_node "${node_id}")"
    ver="$(epoch_tpcc_tblspc_ver_for_node "${node_id}")"
    epoch_ensure_bak_exists "${node_id}" "${bak}"

    local rels=(
        "base/${oid}"
        "pg_location/tablespace2/${ver}/${oid}"
        "pg_location/tablespace3/${ver}/${oid}"
    )

    echo "=== [${EPOCH_TPCC_DB} db-files-only] restore $(epoch_node_display_name "${node_id}") (id=${node_id}) oid=${oid} ==="
    if epoch_is_local_node "${node_id}"; then
        local r
        for r in "${rels[@]}"; do
            if [ ! -d "${bak}/${r}" ]; then
                echo "ERROR: missing ${bak}/${r}" >&2
                return 1
            fi
            mkdir -p "${live}/${r}"
            echo "  rsync ${bak}/${r}/ -> ${live}/${r}/"
            rsync -a --delete --progress "${bak}/${r}/" "${live}/${r}/"
        done
    else
        local rb remote live_name bak_name
        rb="$(epoch_bench_root_for_node "${node_id}")"
        remote="$(epoch_ssh_target "${node_id}")"
        live_name="$(basename "${live}")"
        bak_name="$(basename "${bak}")"
        ssh -o BatchMode=yes "${remote}" bash -s <<EOF
set -euo pipefail
LIVE="${rb}/data/${live_name}"
BAK="${rb}/data/${bak_name}"
for r in base/${oid} \
         pg_location/tablespace2/${ver}/${oid} \
         pg_location/tablespace3/${ver}/${oid}; do
  if [ ! -d "\${BAK}/\${r}" ]; then
    echo "ERROR: missing \${BAK}/\${r}" >&2
    exit 1
  fi
  mkdir -p "\${LIVE}/\${r}"
  echo "  rsync \${BAK}/\${r}/ -> \${LIVE}/\${r}/"
  rsync -a --delete --progress "\${BAK}/\${r}/" "\${LIVE}/\${r}/"
done
EOF
    fi
}

epoch_restore_one() {
    local node_id="$1"
    if [ "${FULL_DATADIR}" = "1" ]; then
        epoch_restore_full_datadir "${node_id}"
        return
    fi
    case "${EPOCH_TPCC_DB}" in
        test|tpcc300)
            epoch_restore_db_files "${node_id}"
            ;;
        *)
            echo "ERROR: unsupported EPOCH_TPCC_DB='${EPOCH_TPCC_DB}' (use test|tpcc300)" >&2
            return 1
            ;;
    esac
}

if [ "${DO_STOP_START}" = "1" ]; then
    echo "=== stop nodes before restore (db=${EPOCH_TPCC_DB}, full_datadir=${FULL_DATADIR}, nodes=$(epoch_profile_node_ids)) ==="
    for n in $(epoch_profile_node_ids); do
        bash "${SCRIPT_DIR}/stop_node.sh" "${n}" || true
    done
    sleep 2
fi

for n in $(epoch_profile_node_ids); do
    epoch_restore_one "${n}"
done

if [ "${DO_STOP_START}" = "1" ]; then
    echo "=== start nodes after restore ==="
    for n in $(epoch_profile_node_ids); do
        bash "${SCRIPT_DIR}/start_node.sh" "${n}"
    done
    sleep 5
fi

echo "DONE: restore mode=${EPOCH_TPCC_DB} full_datadir=${FULL_DATADIR} on nodes ($(epoch_profile_node_ids))"
