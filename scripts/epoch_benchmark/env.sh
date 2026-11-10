#!/usr/bin/env bash
# HCJ ADDBY: epoch 多节点 BenchmarkSQL 测试环境变量

# 从 scripts/epoch_benchmark 向上三级推断 bench 根目录（node1/node2/node3 各自本机路径）
_epoch_env_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT:-$(cd "${_epoch_env_dir}/../../.." && pwd)}"
# 勿沿用 ~/.bashrc 里的 GAUSSHOME，epoch 必须用 2025-9 下与 node1 同步的二进制
export GAUSSHOME="${EPOCH_BENCH_ROOT}/openGauss-server/mppdb_temp_install"
export EPOCH_RUNTIME_LIBS="${EPOCH_BENCH_ROOT}/epoch_deploy/runtime_libs"
export BINARYLIBS="${BINARYLIBS:-/home/hcj2003/openGauss-third_party_binarylibs}"
export GCC_PATH="${GCC_PATH:-${BINARYLIBS}/buildtools/gcc10.3}"

epoch_prepend_ld_path() {
    local dir="$1"
    if [ -d "${dir}" ]; then
        if [ -z "${LD_LIBRARY_PATH:-}" ]; then
            LD_LIBRARY_PATH="${dir}"
        else
            case ":${LD_LIBRARY_PATH}:" in
                *:"${dir}":*) ;;
                *) LD_LIBRARY_PATH="${dir}:${LD_LIBRARY_PATH}" ;;
            esac
        fi
    fi
}

# 仅追加本机存在的目录，避免远程节点引用 hcj2003 路径；runtime_libs 优先于 /usr/local
LD_LIBRARY_PATH=""
epoch_prepend_ld_path "${GAUSSHOME}/lib"
epoch_prepend_ld_path "${EPOCH_RUNTIME_LIBS}"
if [ -f "${EPOCH_RUNTIME_LIBS}/libzmq.so.5" ] || [ -f "${EPOCH_RUNTIME_LIBS}/libzmq.so" ]; then
    :
elif [ -f "/lib64/libzmq.so.5" ]; then
    epoch_prepend_ld_path "/lib64"
elif [ -d "/home/hcj2003/GeoGauss/dest/lib" ] \
    && [ -f "/home/hcj2003/GeoGauss/dest/lib/libzmq.so.5" ]; then
    # 仅当无 runtime_libs/lib64 时回退；勿整目录加入 LD_LIBRARY_PATH（会污染 libstdc++）
    epoch_prepend_ld_path "/home/hcj2003/GeoGauss/dest/lib"
fi
if [ -d "${GCC_PATH}/lib64" ]; then
    epoch_prepend_ld_path "${GCC_PATH}/lib64"
    epoch_prepend_ld_path "${GCC_PATH}/isl/lib"
    epoch_prepend_ld_path "${GCC_PATH}/mpc/lib"
    epoch_prepend_ld_path "${GCC_PATH}/mpfr/lib"
    epoch_prepend_ld_path "${GCC_PATH}/gmp/lib"
fi
export LD_LIBRARY_PATH

PATH="${GAUSSHOME}/bin:/usr/bin:/bin"
if [ -d "${GCC_PATH}/gcc/bin" ]; then
    PATH="${GAUSSHOME}/bin:${GCC_PATH}/gcc/bin:/usr/bin:/bin"
fi
export PATH

export EPOCH_DATA_ROOT="${EPOCH_DATA_ROOT:-${EPOCH_BENCH_ROOT}/data}"
export EPOCH_BENCHMARKSQL="${EPOCH_BENCH_ROOT}/benchmarksql-5.0-epoch/run"

# 三节点拓扑（脚本 NODE_ID = 1..3，与 node1/node2/node3 一致）
#   id=1  node1  202.199.13.149   本机 hcj2003
#   id=2  node2  202.199.13.61    远程 zwx
#   id=3  node3  219.216.64.216   远程 hcj2003（当前默认不测）
export EPOCH_NODE1_IP="${EPOCH_NODE1_IP:-172.16.1.11}"
export EPOCH_NODE2_IP="${EPOCH_NODE2_IP:-172.16.1.12}"
export EPOCH_NODE3_IP="${EPOCH_NODE3_IP:-172.16.1.13}"
export EPOCH_TOTAL_NODES=3
export EPOCH_PEER_HOSTS="${EPOCH_NODE1_IP},${EPOCH_NODE2_IP},${EPOCH_NODE3_IP}"

# 跳过的脚本节点 id。0 = 三节点都参与。
# 当前：node1 + node2，跳过 node3（node3 IP 暂不可用）。
# 旧组合 node1 + node3：export EPOCH_SKIP_NODE=2
# 三节点都测：export EPOCH_SKIP_NODE=0
# 当前：三节点全参与
export EPOCH_SKIP_NODE=0

# 下面几行是文件里的生效值。prep / configure 会用这里的值，覆盖终端里已经 export 的同名变量。
# 对 wait / dbr / pa / het 都生效。RTT 和带宽由 apply_netem.sh 用 tc 写到网卡上。
# EPOCH_PROFILE：wait | dbr | pa | het
# EPOCH_DYN_NET_SLOW_NODES：被限速的节点，0 起。跳过 node3 时：0 = node1，1 = node2
# EPOCH_NETWORK_RTT_MS：往返延迟（毫秒）。100 表示两端各 50ms，ping 大约 100ms。
# EPOCH_DYN_NET_BW_MODE：unlimit | uplimit | downlimit | bilimit
# EPOCH_DYN_NET_BW_MBPS：慢节点带宽（Mbps）。unlimit 时不使用。
# dbr 会打开 epoch_dyn_net_catchup，pa 打开 epoch_dyn_net_proxy，het 两个都开。
export EPOCH_PROFILE="${EPOCH_PROFILE:-wait}"
export EPOCH_DYN_NET_SLOW_NODES="${EPOCH_DYN_NET_SLOW_NODES-0}"
export EPOCH_NETWORK_RTT_MS="${EPOCH_NETWORK_RTT_MS:-100}"
export EPOCH_DYN_NET_BW_MODE="${EPOCH_DYN_NET_BW_MODE:-uplimit}"
export EPOCH_DYN_NET_BW_MBPS="${EPOCH_DYN_NET_BW_MBPS:-10}"

epoch_node_is_skipped() {
    local node_id="$1"
    [ "${EPOCH_SKIP_NODE:-0}" != "0" ] && [ "${node_id}" = "${EPOCH_SKIP_NODE}" ]
}

epoch_effective_total_nodes() {
    if [ "${EPOCH_SKIP_NODE:-0}" != "0" ]; then
        echo $((EPOCH_TOTAL_NODES - 1))
    else
        echo "${EPOCH_TOTAL_NODES}"
    fi
}

epoch_active_node_ids() {
    local n
    for n in 1 2 3; do
        epoch_node_is_skipped "${n}" && continue
        printf '%s ' "${n}"
    done
    printf '\n'
}

epoch_effective_peer_hosts() {
    local n ip hosts=""
    for n in $(epoch_active_node_ids); do
        ip="$(epoch_node_ip "${n}")"
        if [ -z "${hosts}" ]; then
            hosts="${ip}"
        else
            hosts="${hosts},${ip}"
        fi
    done
    echo "${hosts}"
}

# 按 EPOCH_PROFILE 决定 prep/压测流程实际操作哪些节点（single-node-baseline 仅 node1）
epoch_profile_node_ids() {
    case "${EPOCH_PROFILE:-benchmark}" in
        single-node-baseline)
            echo "1"
            ;;
        *)
            epoch_active_node_ids
            ;;
    esac
}

epoch_remote_deploy_node_ids() {
    local n
    for n in $(epoch_active_node_ids); do
        [ "${n}" = "1" ] && continue
        printf '%s ' "${n}"
    done
    printf '\n'
}

# epoch_node_id GUC 为 0-based，与当前 peer_hosts 列表下标对齐
epoch_guc_node_id() {
    local node_id="$1"
    local n idx=0
    if epoch_node_is_skipped "${node_id}"; then
        echo "invalid node_id ${node_id} (EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE})" >&2
        return 1
    fi
    for n in $(epoch_active_node_ids); do
        if [ "${n}" = "${node_id}" ]; then
            echo "${idx}"
            return 0
        fi
        idx=$((idx + 1))
    done
    echo "invalid node_id ${node_id}" >&2
    return 1
}

export EPOCH_DB_PORT="${EPOCH_DB_PORT:-32000}"
export EPOCH_ZMQ_PORT_BASE="${EPOCH_ZMQ_PORT_BASE:-25600}"

# node1 / node3：hcj2003；node2：zwx（gs_initdb 超级用户随 OS 用户）
export EPOCH_SUPER_USER="${EPOCH_SUPER_USER:-hcj2003}"
export EPOCH_SUPER_PASSWORD="${EPOCH_SUPER_PASSWORD:-Gauss@123.}"
export EPOCH_NODE2_SUPER_USER="${EPOCH_NODE2_SUPER_USER:-hcj2003}"
export EPOCH_NODE2_SUPER_PASSWORD="${EPOCH_NODE2_SUPER_PASSWORD:-Gauss@123}"
# node2 的 sudo 密码，apply_netem.sh 在配置/启动测试时用它改 tc
export EPOCH_NODE2_SUDO_PASSWORD="${EPOCH_NODE2_SUDO_PASSWORD:-zwx}"
export EPOCH_TPCC_USER=bot
export EPOCH_TPCC_PASSWORD="${EPOCH_TPCC_PASSWORD:-Test@123}"
# 默认整目录 bak→live 恢复（见 restore_tpcc_datadir.sh）。tpcc300 建议另起 data dir。
export EPOCH_TPCC_DB="${EPOCH_TPCC_DB:-test}"

# 物理 OID（CREATE DATABASE 后固定；DROP 重建会变，需更新）
export EPOCH_TPCC_TEST_OID_NODE1="${EPOCH_TPCC_TEST_OID_NODE1:-24744}"
export EPOCH_TPCC_TEST_OID_NODE3="${EPOCH_TPCC_TEST_OID_NODE3:-49388}"
export EPOCH_TPCC300_OID_NODE1="${EPOCH_TPCC300_OID_NODE1:-24906}"
export EPOCH_TPCC300_OID_NODE3="${EPOCH_TPCC300_OID_NODE3:-49543}"
# openGauss tablespace 相对路径中的版本目录名（与 initdb nodename 相关；同节点上 test/tpcc300 共用）
export EPOCH_TPCC_TBLSPC_VER_NODE1="${EPOCH_TPCC_TBLSPC_VER_NODE1:-${EPOCH_TPCC300_TBLSPC_VER_NODE1:-PG_9.2_201611171_epoch_node0}}"
export EPOCH_TPCC_TBLSPC_VER_NODE3="${EPOCH_TPCC_TBLSPC_VER_NODE3:-${EPOCH_TPCC300_TBLSPC_VER_NODE3:-PG_9.2_201611171_epoch_node1}}"
# 兼容旧变量名
export EPOCH_TPCC300_TBLSPC_VER_NODE1="${EPOCH_TPCC_TBLSPC_VER_NODE1}"
export EPOCH_TPCC300_TBLSPC_VER_NODE3="${EPOCH_TPCC_TBLSPC_VER_NODE3}"

export EPOCH_LOAD_WAREHOUSES="${EPOCH_LOAD_WAREHOUSES:-300}"
export EPOCH_RUN_WAREHOUSES_PER_NODE="${EPOCH_RUN_WAREHOUSES_PER_NODE:-150}"
export EPOCH_TPCC_TERMINALS_NODE1="${EPOCH_TPCC_TERMINALS_NODE1:-33}"
export EPOCH_TPCC_TERMINALS_NODE2="${EPOCH_TPCC_TERMINALS_NODE2:-34}"
export EPOCH_TPCC_TERMINALS_NODE3="${EPOCH_TPCC_TERMINALS_NODE3:-33}"
# BenchmarkSQL props（相对 EPOCH_BENCHMARKSQL）；空则按 EPOCH_TPCC_DB 选默认
export EPOCH_TPCC_PROPS_NODE1="${EPOCH_TPCC_PROPS_NODE1:-}"
export EPOCH_TPCC_PROPS_NODE2="${EPOCH_TPCC_PROPS_NODE2:-}"
export EPOCH_TPCC_PROPS_NODE3="${EPOCH_TPCC_PROPS_NODE3:-}"
export EPOCH_HIST_ID_NODE_STRIDE="${EPOCH_HIST_ID_NODE_STRIDE:-$((2147483647 / $(epoch_effective_total_nodes)))}"
if ! [[ "${EPOCH_HIST_ID_NODE_STRIDE}" =~ ^[0-9]+$ ]] \
    || [ $(($(epoch_effective_total_nodes) * EPOCH_HIST_ID_NODE_STRIDE)) -gt 2147483647 ]; then
    export EPOCH_HIST_ID_NODE_STRIDE=$((2147483647 / $(epoch_effective_total_nodes)))
    echo "WARN: EPOCH_HIST_ID_NODE_STRIDE adjusted to ${EPOCH_HIST_ID_NODE_STRIDE} for $(epoch_effective_total_nodes) active nodes" >&2
fi

export EPOCH_REMOTE_USER="${EPOCH_REMOTE_USER:-hcj2003}"
export EPOCH_REMOTE_HOME="${EPOCH_REMOTE_HOME:-/home/hcj2003}"
export EPOCH_NODE3_BENCH_ROOT="${EPOCH_NODE3_BENCH_ROOT:-/mnt/data/geogauss}"
export EPOCH_NODE2_REMOTE_USER="${EPOCH_NODE2_REMOTE_USER:-hcj2003}"
export EPOCH_NODE2_REMOTE_HOME="${EPOCH_NODE2_REMOTE_HOME:-/home/hcj2003}"

export EPOCH_NODE2_BENCH_ROOT="${EPOCH_NODE2_BENCH_ROOT:-/data/geogauss}"
#export EPOCH_NODE2_REMOTE_USER="${EPOCH_NODE2_REMOTE_USER:-zwx}"
#export EPOCH_NODE2_REMOTE_HOME="${EPOCH_NODE2_REMOTE_HOME:-/home/zwx}"
#export EPOCH_NODE2_BENCH_ROOT="${EPOCH_NODE2_BENCH_ROOT:-${EPOCH_NODE2_REMOTE_HOME}/2025-9}"

export BUILD_JOBS=40
export OPENGAUSS_SRC="${EPOCH_BENCH_ROOT}/openGauss-server"

# 当前 EPOCH_TPCC_DB 对应的 OID
epoch_tpcc_db_oid_for_node() {
    local node_id="$1"
    case "${EPOCH_TPCC_DB}" in
        test)
            case "${node_id}" in
                1) echo "${EPOCH_TPCC_TEST_OID_NODE1}" ;;
                3) echo "${EPOCH_TPCC_TEST_OID_NODE3}" ;;
                *)
                    echo "ERROR: no test OID mapping for node id=${node_id}" >&2
                    return 1
                    ;;
            esac
            ;;
        tpcc300)
            case "${node_id}" in
                1) echo "${EPOCH_TPCC300_OID_NODE1}" ;;
                3) echo "${EPOCH_TPCC300_OID_NODE3}" ;;
                *)
                    echo "ERROR: no tpcc300 OID mapping for node id=${node_id}" >&2
                    return 1
                    ;;
            esac
            ;;
        *)
            echo "ERROR: unsupported EPOCH_TPCC_DB='${EPOCH_TPCC_DB}' (use test|tpcc300)" >&2
            return 1
            ;;
    esac
}

epoch_tpcc_tblspc_ver_for_node() {
    local node_id="$1"
    case "${node_id}" in
        1) echo "${EPOCH_TPCC_TBLSPC_VER_NODE1}" ;;
        3) echo "${EPOCH_TPCC_TBLSPC_VER_NODE3}" ;;
        *)
            echo "ERROR: no tablespace version for node id=${node_id}" >&2
            return 1
            ;;
    esac
}

# 兼容旧调用名
epoch_tpcc300_oid_for_node() {
    local saved="${EPOCH_TPCC_DB}"
    export EPOCH_TPCC_DB=tpcc300
    epoch_tpcc_db_oid_for_node "$@"
    local ec=$?
    export EPOCH_TPCC_DB="${saved}"
    return "${ec}"
}

epoch_tpcc300_tblspc_ver_for_node() {
    epoch_tpcc_tblspc_ver_for_node "$@"
}

epoch_tpcc_terminals_for_node() {
    local node_id="$1"
    case "${node_id}" in
        1) echo "${EPOCH_TPCC_TERMINALS_NODE1}" ;;
        2) echo "${EPOCH_TPCC_TERMINALS_NODE2}" ;;
        3) echo "${EPOCH_TPCC_TERMINALS_NODE3}" ;;
        *)
            echo "invalid node_id ${node_id} (expected 1..${EPOCH_TOTAL_NODES})" >&2
            return 1
            ;;
    esac
}

# 返回 props 文件名（相对 benchmarksql-5.0-epoch/run）
epoch_tpcc_props_for_node() {
    local node_id="$1"
    local override=""
    case "${node_id}" in
        1) override="${EPOCH_TPCC_PROPS_NODE1}" ;;
        2) override="${EPOCH_TPCC_PROPS_NODE2}" ;;
        3) override="${EPOCH_TPCC_PROPS_NODE3}" ;;
        *)
            echo "ERROR: no BenchmarkSQL props mapping for node id=${node_id}" >&2
            return 1
            ;;
    esac
    if [ -n "${override}" ]; then
        echo "${override}"
        return 0
    fi
    # node2 / node3 都是第二写端，共用 props.epoch.node2-*
    case "${EPOCH_TPCC_DB}" in
        tpcc300)
            case "${node_id}" in
                1) echo "props.epoch.node0-300w" ;;
                2|3) echo "props.epoch.node2-300w" ;;
            esac
            ;;
        test)
            case "${node_id}" in
                1) echo "props.epoch.node0-33" ;;
                2|3) echo "props.epoch.node2-33" ;;
            esac
            ;;
        *)
            echo "ERROR: unsupported EPOCH_TPCC_DB='${EPOCH_TPCC_DB}' for props (use test|tpcc300)" >&2
            return 1
            ;;
    esac
}

epoch_find_cmake_bin_dir() {
    local d candidate
    if command -v cmake >/dev/null 2>&1; then
        dirname "$(command -v cmake)"
        return 0
    fi
    candidate="${EPOCH_BENCH_ROOT}/epoch_deploy/buildtools/cmake/bin"
    if [ -x "${candidate}/cmake" ]; then
        echo "${candidate}"
        return 0
    fi
    for d in \
        "${BINARYLIBS}"/buildtools/cmake*/bin \
        /home/hcj2003/.cache/JetBrains/RemoteDev/dist/*/bin/cmake/linux/x64/bin; do
        for d in ${d}; do
            if [ -x "${d}/cmake" ]; then
                echo "${d}"
                return 0
            fi
        done
    done
    return 1
}

epoch_ensure_cmake_installed() {
    local src_dir dest_dir
    dest_dir="${EPOCH_BENCH_ROOT}/epoch_deploy/buildtools/cmake/bin"
    if [ -x "${dest_dir}/cmake" ]; then
        echo "${dest_dir}"
        return 0
    fi
    src_dir="$(epoch_find_cmake_bin_dir)" || return 1
    if [ "${src_dir}" = "${dest_dir}" ]; then
        echo "${dest_dir}"
        return 0
    fi
    mkdir -p "${dest_dir}"
    ln -sf "${src_dir}/cmake" "${dest_dir}/cmake"
    echo "${dest_dir}"
}

epoch_ensure_build_java_home() {
    local plat="${BINARYLIBS}/kernel/platform"
    local arch
    arch="$(uname -p)"
    if [ ! -d "${plat}/huaweijdk8" ] && [ -d "${plat}/openjdk8" ]; then
        ln -sf openjdk8 "${plat}/huaweijdk8" 2>/dev/null || true
    fi
    if [ -d "${plat}/huaweijdk8/${arch}/jdk" ]; then
        echo "${plat}/huaweijdk8/${arch}/jdk"
    elif [ -d "${plat}/openjdk8/${arch}/jdk" ]; then
        echo "${plat}/openjdk8/${arch}/jdk"
    elif [ -d "${plat}/openjdk8/jdk" ]; then
        echo "${plat}/openjdk8/jdk"
    else
        return 1
    fi
}

epoch_setup_build_env() {
    local gcc_ver="10.3"
    local gcc_root="${BINARYLIBS}/buildtools/gcc${gcc_ver}"
    local cmake_dir dep_zstd
    if [ ! -x "${gcc_root}/gcc/bin/gcc" ]; then
        echo "ERROR: missing ${gcc_root}/gcc/bin/gcc (check BINARYLIBS=${BINARYLIBS})" >&2
        return 1
    fi
    cmake_dir="$(epoch_ensure_cmake_installed)" || {
        echo "ERROR: cmake not found; install cmake or keep CLion bundled cmake under ~/.cache/JetBrains" >&2
        return 1
    }
    dep_zstd="${BINARYLIBS}/kernel/dependency/zstd/lib"
    export CC="${gcc_root}/gcc/bin/gcc"
    export CXX="${gcc_root}/gcc/bin/g++"
    JAVA_HOME="$(epoch_ensure_build_java_home)" || {
        echo "ERROR: JDK not found under ${BINARYLIBS}/kernel/platform" >&2
        return 1
    }
    export JAVA_HOME
    export EPOCH_CMAKE_BIN_DIR="${cmake_dir}"
    export PATH="${cmake_dir}:${gcc_root}/gcc/bin:${PATH}"
    epoch_prepend_ld_path "${dep_zstd}"
    epoch_prepend_ld_path "${gcc_root}/gcc/lib64"
    epoch_prepend_ld_path "${gcc_root}/isl/lib"
    epoch_prepend_ld_path "${gcc_root}/mpc/lib"
    epoch_prepend_ld_path "${gcc_root}/mpfr/lib"
    epoch_prepend_ld_path "${gcc_root}/gmp/lib"
    export LD_LIBRARY_PATH
    echo "build env: CC=${CC}, CMAKE=${cmake_dir}/cmake, JAVA_HOME=${JAVA_HOME}"
}

epoch_build() {
    epoch_setup_build_env || return 1
    cd "${OPENGAUSS_SRC}"
    sh build.sh "$@"
}

epoch_detect_local_node_id() {
    local ip
    for ip in $(hostname -I 2>/dev/null); do
        if [ "${ip}" = "${EPOCH_NODE1_IP}" ]; then
            echo 1
            return
        fi
        if [ "${ip}" = "${EPOCH_NODE2_IP}" ]; then
            echo 2
            return
        fi
        if [ "${ip}" = "${EPOCH_NODE3_IP}" ]; then
            echo 3
            return
        fi
    done
    echo ""
}

if [ -z "${EPOCH_LOCAL_NODE_ID:-}" ]; then
    _detected_nid="$(epoch_detect_local_node_id)"
    if [ -n "${_detected_nid}" ]; then
        export EPOCH_LOCAL_NODE_ID="${_detected_nid}"
    fi
fi

# 兼容旧版 0-based 数据目录：node1→epoch-node0, node3→epoch-node1
# 仅当 canonical 目录已 gs_initdb 时才用 epoch-nodeN；否则固定 legacy 路径。
# 不可在「调用方机器」上检查 legacy/postgresql.conf——集群脚本常在 node1 上
# 解析 node3 的 datadir，本机没有 epoch-node1 时会误判为 epoch-node3。
data_dir_for_node() {
    local node_id="$1"
    local canonical legacy=""
    canonical="${EPOCH_DATA_ROOT}/epoch-node${node_id}"
    case "${node_id}" in
        1) legacy="${EPOCH_DATA_ROOT}/epoch-node0" ;;
        3) legacy="${EPOCH_DATA_ROOT}/epoch-node1" ;;
    esac
    if [ -f "${canonical}/postgresql.conf" ]; then
        echo "${canonical}"
    elif [ -n "${legacy}" ]; then
        echo "${legacy}"
    else
        echo "${canonical}"
    fi
}

# 在目标节点机器上解析 datadir（远程 EPOCH_DATA_ROOT 与本地不一致时更可靠）
epoch_data_dir_for_node() {
    local node_id="$1"
    if epoch_is_local_node "${node_id}"; then
        data_dir_for_node "${node_id}"
        return
    fi
    local rb script
    rb="$(epoch_bench_root_for_node "${node_id}")"
    script="${rb}/openGauss-server/scripts/epoch_benchmark/env.sh"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$(epoch_ssh_target "${node_id}")" \
        "export EPOCH_BENCH_ROOT='${rb}'; export EPOCH_LOCAL_NODE_ID=${node_id}; \
        source '${script}'; data_dir_for_node ${node_id}"
}

epoch_node_ip() {
    local node_id="$1"
    case "${node_id}" in
        1) echo "${EPOCH_NODE1_IP}" ;;
        2) echo "${EPOCH_NODE2_IP}" ;;
        3) echo "${EPOCH_NODE3_IP}" ;;
        *)
            echo "invalid node_id ${node_id} (expected 1..${EPOCH_TOTAL_NODES})" >&2
            return 1
            ;;
    esac
}

epoch_remote_user() {
    local node_id="$1"
    case "${node_id}" in
        1|3) echo "${EPOCH_REMOTE_USER}" ;;
        2) echo "${EPOCH_NODE2_REMOTE_USER}" ;;
        *)
            echo "invalid node_id ${node_id}" >&2
            return 1
            ;;
    esac
}

epoch_bench_root_for_node() {
    local node_id="$1"
    case "${node_id}" in
        1) echo "${EPOCH_BENCH_ROOT}" ;;
        2) echo "${EPOCH_NODE2_BENCH_ROOT}" ;;
        3) echo "${EPOCH_NODE3_BENCH_ROOT}" ;;
        *)
            echo "invalid node_id ${node_id}" >&2
            return 1
            ;;
    esac
}

epoch_ssh_target() {
    local node_id="$1"
    echo "$(epoch_remote_user "${node_id}")@$(epoch_node_ip "${node_id}")"
}

epoch_remote_gauss_env() {
    local node_id="$1"
    local rb gh rt
    rb="$(epoch_bench_root_for_node "${node_id}")"
    gh="${rb}/openGauss-server/mppdb_temp_install"
    rt="${rb}/epoch_deploy/runtime_libs"
    echo "export GAUSSHOME=${gh}; export PATH=\${GAUSSHOME}/bin:/usr/bin:/bin; export LD_LIBRARY_PATH=\${GAUSSHOME}/lib:${rt}"
}

epoch_is_local_node() {
    local node_id="$1"
    if [ -n "${EPOCH_LOCAL_NODE_ID:-}" ] && [ "${EPOCH_LOCAL_NODE_ID}" = "${node_id}" ]; then
        return 0
    fi
    local target_ip ip
    target_ip="$(epoch_node_ip "${node_id}")"
    for ip in $(hostname -I 2>/dev/null); do
        if [ "${ip}" = "${target_ip}" ]; then
            return 0
        fi
    done
    return 1
}

epoch_remote_shell() {
    local n="$1"
    shift
    local rb env_prefix
    rb="$(epoch_bench_root_for_node "${n}")"
    env_prefix="$(epoch_remote_gauss_env "${n}")"
    if epoch_is_local_node "${n}"; then
        eval "${env_prefix}"
        bash -c "$*"
    else
        ssh -o BatchMode=yes -o ConnectTimeout=10 "$(epoch_ssh_target "${n}")" \
            "${env_prefix}; $*"
    fi
}

epoch_super_user_for_node() {
    local node_id="$1"
    if [ "${node_id}" = "2" ]; then
        echo "${EPOCH_NODE2_SUPER_USER}"
    else
        echo "${EPOCH_SUPER_USER}"
    fi
}

epoch_super_password_for_node() {
    local node_id="$1"
    if [ "${node_id}" = "2" ]; then
        echo "${EPOCH_NODE2_SUPER_PASSWORD}"
    else
        echo "${EPOCH_SUPER_PASSWORD}"
    fi
}

validate_node_id() {
    local node_id="$1"
    if ! [[ "${node_id}" =~ ^[0-9]+$ ]] || [ "${node_id}" -lt 1 ] || [ "${node_id}" -gt "${EPOCH_TOTAL_NODES}" ]; then
        echo "invalid NODE_ID=${node_id}, expected 1..${EPOCH_TOTAL_NODES}" >&2
        return 1
    fi
    if epoch_node_is_skipped "${node_id}"; then
        echo "NODE_ID=${node_id} (node${node_id}) is skipped (EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE})" >&2
        return 1
    fi
}

epoch_node_display_name() {
    local node_id="$1"
    echo "node${node_id}"
}

epoch_check_gaussdb() {
    local gaussdb="${GAUSSHOME}/bin/gaussdb"
    if [ ! -x "${gaussdb}" ]; then
        echo "ERROR: missing ${gaussdb}, run deploy_to_remote.sh from node1" >&2
        return 1
    fi
    if ! "${gaussdb}" -V >/dev/null 2>&1; then
        echo "ERROR: gaussdb cannot start (shared library missing). ldd output:" >&2
        ldd "${gaussdb}" 2>&1 | grep 'not found' >&2 || ldd "${gaussdb}" >&2
        echo "hint: on node1 run deploy_to_remote.sh 3 to sync epoch_deploy/runtime_libs" >&2
        echo "hint: or install libncurses5: sudo apt install libncurses5" >&2
        return 1
    fi
}

epoch_gsql() {
    gsql -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" "$@"
}

epoch_tpcc_gsql() {
    epoch_gsql "$@"
}

epoch_super_gsql() {
    gsql -d postgres -p "${EPOCH_DB_PORT}" "$@"
}
