#!/usr/bin/env bash
# 每轮测试前的完整预处理：停库 -> 从 _bak 整目录恢复 -> 统一参数 -> 修表空间链接 -> 重设密码 -> 并行启动
# 用法: bash prep_cluster.sh            # 全流程
#       bash prep_cluster.sh --restore-only   # 只恢复并启动，不改参数
# ASAN 构建：openGauss 存在已知 ODR 冲突，需关闭该项才能启动
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_odr_violation=0:abort_on_error=1:disable_coredump=0}"
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"
# 生效节点集：EPOCH_SKIP_NODE=2 时只跑 node1 + node3（见 env.sh）
EFF_NODES="$(epoch_effective_total_nodes)"
EFF_HOSTS="$(epoch_effective_peer_hosts)"

# 某 IP 在生效 peer_hosts 里的下标 == 该节点的 epoch_node_id（0 起）
node_index_of_ip() {
    local ip="$1" idx=0 h
    for h in $(echo "${EFF_HOSTS}" | tr ',' ' '); do
        if [ "$h" = "$ip" ]; then
            echo "${idx}"
            return 0
        fi
        idx=$((idx + 1))
    done
    echo "-1"
}

RESTORE_ONLY=0
[ "${1:-}" = "--restore-only" ] && RESTORE_ONLY=1

N2="hcj2003@${EPOCH_NODE2_IP}"
N3="hcj2003@${EPOCH_NODE3_IP}"
ROOT2="/data/geogauss"
ROOT3="/mnt/data/geogauss"
GH1="${EPOCH_BENCH_ROOT}/openGauss-server/mppdb_temp_install"
GH2="${ROOT2}/openGauss-server/mppdb_temp_install"
GH3="${ROOT3}/openGauss-server/mppdb_temp_install"
D1="/data/geogauss/data/epoch-node0"
D2="${ROOT2}/data/epoch-node2"
D3="${ROOT3}/data/epoch-node3"

log() { echo "[$(date +%H:%M:%S)] $*"; }

stop_all() {
    log "stop all nodes"
    timeout 60 su -s /bin/bash hcj2003 -c "export ASAN_OPTIONS=detect_odr_violation=0:abort_on_error=1 GAUSSHOME=${GH1} LD_LIBRARY_PATH=${GH1}/lib PATH=${GH1}/bin:/usr/bin:/bin; gs_ctl stop -D ${D1} -m immediate" >/dev/null 2>&1
    ssh -o BatchMode=yes "${N2}" "export ASAN_OPTIONS=detect_odr_violation=0:abort_on_error=1 GAUSSHOME=${GH2} LD_LIBRARY_PATH=${GH2}/lib PATH=${GH2}/bin:/usr/bin:/bin; timeout 60 gs_ctl stop -D ${D2} -m immediate" >/dev/null 2>&1
    ssh -o BatchMode=yes "${N3}" "export ASAN_OPTIONS=detect_odr_violation=0:abort_on_error=1 GAUSSHOME=${GH3} LD_LIBRARY_PATH=${GH3}/lib PATH=${GH3}/bin:/usr/bin:/bin; timeout 60 gs_ctl stop -D ${D3} -m immediate" >/dev/null 2>&1
    sleep 2
    pkill -9 -f "gaussdb.*epoch-node0" 2>/dev/null
    ssh -o BatchMode=yes "${N2}" 'pkill -9 -f "gaussdb.*epoch-node2"' 2>/dev/null
    ssh -o BatchMode=yes "${N3}" 'pkill -9 -f "gaussdb.*epoch-node3"' 2>/dev/null
    sleep 1
}

clean_ipc() {
    log "clean ipc"
    for id in $(ipcs -m | awk 'NR>3 && $3=="hcj2003"{print $2}'); do ipcrm -m "$id" 2>/dev/null; done
    for id in $(ipcs -s | awk 'NR>3 && $3=="hcj2003"{print $2}'); do ipcrm -s "$id" 2>/dev/null; done
    for ip in "${EPOCH_NODE2_IP}" "${EPOCH_NODE3_IP}"; do
        ssh -o BatchMode=yes "hcj2003@${ip}" bash -s <<'EOS' 2>/dev/null
for id in $(ipcs -m | awk 'NR>3 && $3=="hcj2003"{print $2}'); do ipcrm -m "$id" 2>/dev/null; done
for id in $(ipcs -s | awk 'NR>3 && $3=="hcj2003"{print $2}'); do ipcrm -s "$id" 2>/dev/null; done
EOS
    done
}

restore_datadir() {
    log "restore datadir (parallel)"
    ( rsync -a --delete /data/geogauss/data/epoch-node0_bak/ "${D1}/" && echo "  n1 restored" ) &
    ( ssh -o BatchMode=yes "${N2}" "rsync -a --delete ${ROOT2}/data/epoch-node2_bak/ ${D2}/" && echo "  n2 restored" ) &
    ( ssh -o BatchMode=yes "${N3}" "rsync -a --delete ${ROOT3}/data/epoch-node3_bak/ ${D3}/" && echo "  n3 restored" ) &
    wait
}

remote_params() {
    local target="$1" ddir="$2"
    local tip="${target#*@}"
    local tnid
    tnid="$(node_index_of_ip "${tip}")"
    if [ "${tnid}" -lt 0 ]; then
        # 该节点不参与本轮（EPOCH_SKIP_NODE），不改它的配置
        return 0
    fi
    ssh -o BatchMode=yes "${target}" \
        "NODE_ID='${tnid}' EFF_NODES='${EFF_NODES}' EFF_HOSTS='${EFF_HOSTS}' DN_ENABLE='${DN_ENABLE}' DN_CATCHUP='${DN_CATCHUP}' DN_PROXY='${DN_PROXY}' DN_SLOW='${DN_SLOW}' DN_RTT='${DN_RTT}' DN_UPBPS='${DN_UPBPS}' DN_DOWNBPS='${DN_DOWNBPS}' DN_DBR_PCT='${DN_DBR_PCT}' DN_DBR_MODE='${DN_DBR_MODE}' bash -s -- '${ddir}'" <<'EOS'
D=$1
sed -i -E \
 -e "s/^#?listen_addresses *=.*/listen_addresses = '*'/" \
 -e 's/^shared_buffers *=.*/shared_buffers = 3GB/' \
 -e 's/^max_process_memory *=.*/max_process_memory = 20GB/' \
 -e 's/^effective_cache_size *=.*/effective_cache_size = 4GB/' \
 -e 's/^max_connections *=.*/max_connections = 400/' \
 -e "s/^epoch_node_id *=.*/epoch_node_id = ${NODE_ID}/" \
 -e "s/^epoch_total_nodes *=.*/epoch_total_nodes = ${EFF_NODES}/" \
 -e "s/^epoch_peer_hosts *=.*/epoch_peer_hosts = '${EFF_HOSTS}'/" \
 -e "s/^enable_epoch_dyn_net *=.*/enable_epoch_dyn_net = ${DN_ENABLE}/" \
 -e "s/^epoch_dyn_net_catchup *=.*/epoch_dyn_net_catchup = ${DN_CATCHUP}/" \
 -e "s/^epoch_dyn_net_proxy *=.*/epoch_dyn_net_proxy = ${DN_PROXY}/" \
 -e "s/^epoch_dyn_net_slow_nodes *=.*/epoch_dyn_net_slow_nodes = '${DN_SLOW}'/" \
 -e "s/^epoch_dyn_net_rtt_ms *=.*/epoch_dyn_net_rtt_ms = ${DN_RTT}/" \
 -e "s/^epoch_dbr_threshold_pct *=.*/epoch_dbr_threshold_pct = ${DN_DBR_PCT}/" \
 -e "s/^epoch_dyn_net_upload_bps *=.*/epoch_dyn_net_upload_bps = ${DN_UPBPS}/" \
 -e "s/^epoch_dyn_net_download_bps *=.*/epoch_dyn_net_download_bps = ${DN_DOWNBPS}/" \
 -e "s/^epoch_pack_threads *=.*/epoch_pack_threads = 1/" \
 -e "s/^epoch_merge_threads *=.*/epoch_merge_threads = 3/" \
 -e "s/^epoch_commit_threads *=.*/epoch_commit_threads = 9/" \
 -e "s/^epoch_abort_csn_per_epoch *=.*/epoch_abort_csn_per_epoch = 1024/" \
 "$D/postgresql.conf"
grep -q '^epoch_dyn_net_upload_bps' "$D/postgresql.conf" || echo "epoch_dyn_net_upload_bps = ${DN_UPBPS}" >> "$D/postgresql.conf"
grep -q '^epoch_dyn_net_download_bps' "$D/postgresql.conf" || echo "epoch_dyn_net_download_bps = ${DN_DOWNBPS}" >> "$D/postgresql.conf"
grep -q '^epoch_dbr_threshold_pct' "$D/postgresql.conf" || echo "epoch_dbr_threshold_pct = ${DN_DBR_PCT}" >> "$D/postgresql.conf"
# abort 集合每片只需容纳 24 笔事务，旧配置 65536 会让共享内存涨到 ~5GB
grep -q '^epoch_abort_csn_per_epoch' "$D/postgresql.conf" || echo "epoch_abort_csn_per_epoch = 1024" >> "$D/postgresql.conf"
# 写回只由 epoch apply 承担：backend 跳过 WAL 与堆写，否则两边都会写同一批行（主键冲突）
for ip in 172.16.1.11 172.16.1.12 172.16.1.13; do
  grep -q "^host all all ${ip}/32 trust" "$D/pg_hba.conf" || echo "host all all ${ip}/32 trust" >> "$D/pg_hba.conf"
done
chmod 600 "$D/postgresql.conf" "$D/pg_hba.conf"
EOS
}

apply_params() {
    log "apply params"
    # 正式实验一律关闭应用层虚拟网络（Dn），限速/延迟全部由 tc（tbf+netem）真实提供
    local DN_ENABLE="off"
    local DN_CATCHUP="${EPOCH_DYN_NET_CATCHUP:-off}"
    local DN_PROXY="${EPOCH_DYN_NET_PROXY:-off}"
    local DN_SLOW="${EPOCH_DYN_NET_SLOW_NODES:-}"
    local DN_RTT="${EPOCH_NETWORK_RTT_MS:-100}"
    local DN_DBR_PCT="${EPOCH_DBR_THRESHOLD_PCT:-95}"
    # 上行带宽上限（bit/s）：拥塞判定用“网卡实际速率 / 该上限”，0=不限速
    local DN_UPBPS="${EPOCH_DYN_NET_UPLOAD_BPS:-0}"
    local DN_DOWNBPS="${EPOCH_DYN_NET_DOWNLOAD_BPS:-0}"
    local sedexpr=(
        -e 's/^shared_buffers *=.*/shared_buffers = 3GB/'
        -e 's/^max_process_memory *=.*/max_process_memory = 20GB/'
        -e 's/^effective_cache_size *=.*/effective_cache_size = 4GB/'
        -e 's/^max_connections *=.*/max_connections = 400/'
        -e "s/^epoch_node_id *=.*/epoch_node_id = $(node_index_of_ip "${EPOCH_NODE1_IP}")/"
        -e "s/^epoch_total_nodes *=.*/epoch_total_nodes = ${EFF_NODES}/"
        -e "s/^epoch_peer_hosts *=.*/epoch_peer_hosts = '${EFF_HOSTS}'/"
        -e "s/^enable_epoch_dyn_net *=.*/enable_epoch_dyn_net = ${DN_ENABLE}/"
        -e "s/^epoch_dyn_net_catchup *=.*/epoch_dyn_net_catchup = ${DN_CATCHUP}/"
        -e "s/^epoch_dyn_net_proxy *=.*/epoch_dyn_net_proxy = ${DN_PROXY}/"
        -e "s/^epoch_dyn_net_slow_nodes *=.*/epoch_dyn_net_slow_nodes = '${DN_SLOW}'/"
        -e "s/^epoch_dyn_net_rtt_ms *=.*/epoch_dyn_net_rtt_ms = ${DN_RTT}/"
        -e "s/^epoch_dbr_threshold_pct *=.*/epoch_dbr_threshold_pct = ${DN_DBR_PCT}/"
        -e "s/^epoch_dyn_net_upload_bps *=.*/epoch_dyn_net_upload_bps = ${DN_UPBPS}/"
        -e "s/^epoch_dyn_net_download_bps *=.*/epoch_dyn_net_download_bps = ${DN_DOWNBPS}/"
        -e "s/^epoch_pack_threads *=.*/epoch_pack_threads = 1/"
        -e "s/^epoch_merge_threads *=.*/epoch_merge_threads = 3/"
        -e "s/^epoch_commit_threads *=.*/epoch_commit_threads = 9/"
        -e "s/^epoch_abort_csn_per_epoch *=.*/epoch_abort_csn_per_epoch = 1024/"
    )
    sed -i -E "${sedexpr[@]}" "${D1}/postgresql.conf"
    # 旧版脚本写入、当前代码未注册的 GUC：残留会让实例起不来
    sed -i '/^[[:space:]]*epoch_dbr_mode[[:space:]]*=/d' "${D1}/postgresql.conf"
    # 新参数：旧 conf 里没有这一行，sed 不会创建它
    grep -q '^epoch_dyn_net_upload_bps' "${D1}/postgresql.conf" || echo "epoch_dyn_net_upload_bps = ${DN_UPBPS}" >> "${D1}/postgresql.conf"
    grep -q '^epoch_dyn_net_download_bps' "${D1}/postgresql.conf" || echo "epoch_dyn_net_download_bps = ${DN_DOWNBPS}" >> "${D1}/postgresql.conf"
    grep -q '^epoch_dbr_threshold_pct' "${D1}/postgresql.conf" || echo "epoch_dbr_threshold_pct = ${DN_DBR_PCT}" >> "${D1}/postgresql.conf"
        grep -q '^epoch_abort_csn_per_epoch' "${D1}/postgresql.conf" || echo "epoch_abort_csn_per_epoch = 1024" >> "${D1}/postgresql.conf"
    for ip in 172.16.1.11 172.16.1.12 172.16.1.13; do
        grep -q "^host all all ${ip}/32 trust" "${D1}/pg_hba.conf" || echo "host all all ${ip}/32 trust" >> "${D1}/pg_hba.conf"
    done
    chmod 600 "${D1}/postgresql.conf" "${D1}/pg_hba.conf"
    remote_params "${N2}" "${D2}"
    remote_params "${N3}" "${D3}"
}

fix_tablespace() {
    log "fix tablespace links (node3)"
    ssh -o BatchMode=yes "${N3}" bash -s -- "${D3}" <<'EOS' 2>/dev/null
D=$1
cd "$D/pg_tblspc" || exit 0
for f in *; do
    [ -L "$f" ] || continue
    case "$(basename "$(readlink "$f")")" in
        tablespace2) ln -sfn "$D/pg_location/tablespace2" "$f" ;;
        tablespace3) ln -sfn "$D/pg_location/tablespace3" "$f" ;;
    esac
done
EOS
}

reset_password() {
    log "reset hcj2003 password (single-user)"
    timeout 90 su -s /bin/bash hcj2003 -c "export ASAN_OPTIONS=detect_odr_violation=0:abort_on_error=1 GAUSSHOME=${GH1} LD_LIBRARY_PATH=${GH1}/lib PATH=${GH1}/bin:/usr/bin:/bin; printf \"ALTER USER hcj2003 IDENTIFIED BY 'Test@123';\n\" | gaussdb --single -D ${D1} postgres" >/dev/null 2>&1
    ssh -o BatchMode=yes "${N2}" "export ASAN_OPTIONS=detect_odr_violation=0:abort_on_error=1 GAUSSHOME=${GH2} LD_LIBRARY_PATH=${GH2}/lib PATH=${GH2}/bin:/usr/bin:/bin; printf \"ALTER USER hcj2003 IDENTIFIED BY 'Test@123';\n\" | timeout 90 gaussdb --single -D ${D2} postgres" >/dev/null 2>&1
    ssh -o BatchMode=yes "${N3}" "export ASAN_OPTIONS=detect_odr_violation=0:abort_on_error=1 GAUSSHOME=${GH3} LD_LIBRARY_PATH=${GH3}/lib PATH=${GH3}/bin:/usr/bin:/bin; printf \"ALTER USER hcj2003 IDENTIFIED BY 'Test@123';\n\" | timeout 90 gaussdb --single -D ${D3} postgres" >/dev/null 2>&1
    rm -f "${D1}/postmaster.pid"
    ssh -o BatchMode=yes "${N2}" "rm -f ${D2}/postmaster.pid" 2>/dev/null
    ssh -o BatchMode=yes "${N3}" "rm -f ${D3}/postmaster.pid" 2>/dev/null
}

stop_all
clean_ipc
restore_datadir
if [ "${RESTORE_ONLY}" -eq 0 ]; then
    apply_params
    fix_tablespace
    reset_password
fi
clean_ipc
log "start cluster"
timeout 300 bash "${SCRIPT_DIR}/start_cluster.sh" $(epoch_active_node_ids)
log "prep done"
