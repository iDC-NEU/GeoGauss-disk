#include "postgres.h"

#include "epoch/e_perf.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_shmem.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "miscadmin.h"

#include <cstdio>
#include <cstring>
#include <ctime>

static uint64_t g_last_perf_report_physical = 0;

static EpochSharedState::EpochPerfCounters* EpochPerfCounters(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return nullptr;
    }
    return &state->perf;
}

static void EpochPerfZeroCounters(EpochSharedState::EpochPerfCounters* perf)
{
    if (perf == nullptr) {
        return;
    }
    for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
        perf->gate_wait_us[i].store(0, std::memory_order_relaxed);
        perf->gate_wait_count[i].store(0, std::memory_order_relaxed);
        perf->gate_spin_count[i].store(0, std::memory_order_relaxed);
    }
    perf->zmq_wire_txn_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_wire_txn_send_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_wire_row_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_wire_row_count.store(0, std::memory_order_relaxed);
    perf->zmq_epoch_end_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_epoch_end_send_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_hello_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_txn_req_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_wire_recv_count.store(0, std::memory_order_relaxed);
    perf->zmq_wire_recv_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_epoch_end_recv_count.store(0, std::memory_order_relaxed);
    perf->logical_advance_ok.store(0, std::memory_order_relaxed);
    perf->logical_advance_try.store(0, std::memory_order_relaxed);
    perf->stall_peer_not_connected.store(0, std::memory_order_relaxed);
    perf->stall_peer_lag.store(0, std::memory_order_relaxed);
    perf->stall_physical_not_ready.store(0, std::memory_order_relaxed);
    perf->stall_recv_incomplete.store(0, std::memory_order_relaxed);
    perf->stall_merge_incomplete.store(0, std::memory_order_relaxed);
    perf->stall_local_commit_pending.store(0, std::memory_order_relaxed);
    perf->stall_send_incomplete.store(0, std::memory_order_relaxed);
    perf->stall_reseal_pending.store(0, std::memory_order_relaxed);
    perf->stall_commit_path_incomplete.store(0, std::memory_order_relaxed);
    perf->stall_record_incomplete.store(0, std::memory_order_relaxed);
    perf->seal_block_physical_delay.store(0, std::memory_order_relaxed);
    perf->seal_block_peer_lag.store(0, std::memory_order_relaxed);
    perf->seal_block_exec_pending.store(0, std::memory_order_relaxed);
    perf->seal_block_change_set_pending.store(0, std::memory_order_relaxed);
    perf->seal_block_epoch_active.store(0, std::memory_order_relaxed);
    perf->seal_block_peer_txn_pending.store(0, std::memory_order_relaxed);
    perf->seal_ok_count.store(0, std::memory_order_relaxed);
    perf->worker_poll_count.store(0, std::memory_order_relaxed);
    perf->physical_advance_count.store(0, std::memory_order_relaxed);
}

bool EpochPerfIsEnabled(void)
{
    return g_enable_epoch_multimaster && g_epoch_perf_log;
}

void EpochPerfReset(void)
{
    EpochPerfZeroCounters(EpochPerfCounters());
    g_last_perf_report_physical = 0;
}

void EpochPerfAddGateWait(EpochPerfGateKind kind, uint64_t wait_us, uint64_t spin_count)
{
    if (!EpochPerfIsEnabled() || kind >= EPOCH_PERF_GATE_COUNT) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->gate_wait_us[kind].fetch_add(wait_us, std::memory_order_relaxed);
    perf->gate_wait_count[kind].fetch_add(1, std::memory_order_relaxed);
    perf->gate_spin_count[kind].fetch_add(spin_count, std::memory_order_relaxed);
}

void EpochPerfNoteLogicalAdvanceOk(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf != nullptr) {
        perf->logical_advance_ok.fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochPerfNoteLogicalAdvanceTry(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf != nullptr) {
        perf->logical_advance_try.fetch_add(1, std::memory_order_relaxed);
    }
}

static void EpochPerfBump(std::atomic<uint64_t>* counter)
{
    if (counter != nullptr) {
        counter->fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochPerfNoteLogicalStall(const char* reason)
{
    if (!EpochPerfIsEnabled() || reason == nullptr) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    if (strcmp(reason, "peer_not_connected") == 0) {
        EpochPerfBump(&perf->stall_peer_not_connected);
    } else if (strcmp(reason, "peer_lag") == 0) {
        EpochPerfBump(&perf->stall_peer_lag);
    } else if (strcmp(reason, "physical_not_ready") == 0) {
        EpochPerfBump(&perf->stall_physical_not_ready);
    } else if (strcmp(reason, "recv_incomplete") == 0) {
        EpochPerfBump(&perf->stall_recv_incomplete);
    } else if (strcmp(reason, "merge_incomplete") == 0) {
        EpochPerfBump(&perf->stall_merge_incomplete);
    } else if (strcmp(reason, "local_commit_pending") == 0) {
        EpochPerfBump(&perf->stall_local_commit_pending);
    } else if (strcmp(reason, "send_incomplete") == 0) {
        EpochPerfBump(&perf->stall_send_incomplete);
    } else if (strcmp(reason, "reseal_pending") == 0) {
        EpochPerfBump(&perf->stall_reseal_pending);
    } else if (strcmp(reason, "commit_path_incomplete") == 0) {
        EpochPerfBump(&perf->stall_commit_path_incomplete);
    } else if (strcmp(reason, "record_incomplete") == 0) {
        EpochPerfBump(&perf->stall_record_incomplete);
    }
}

void EpochPerfNoteSealBlock(const char* reason)
{
    if (!EpochPerfIsEnabled() || reason == nullptr) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    if (strcmp(reason, "physical_delay") == 0) {
        EpochPerfBump(&perf->seal_block_physical_delay);
    } else if (strcmp(reason, "peer_lag") == 0) {
        EpochPerfBump(&perf->seal_block_peer_lag);
    } else if (strcmp(reason, "exec_pending") == 0) {
        EpochPerfBump(&perf->seal_block_exec_pending);
    } else if (strcmp(reason, "change_set_pending") == 0) {
        EpochPerfBump(&perf->seal_block_change_set_pending);
    } else if (strcmp(reason, "epoch_active") == 0) {
        EpochPerfBump(&perf->seal_block_epoch_active);
    } else if (strcmp(reason, "peer_txn_pending") == 0) {
        EpochPerfBump(&perf->seal_block_peer_txn_pending);
    }
}

void EpochPerfNoteSealOk(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->seal_ok_count : nullptr);
}

void EpochPerfNoteWorkerPoll(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->worker_poll_count : nullptr);
}

void EpochPerfNotePhysicalAdvance(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->physical_advance_count : nullptr);
}

static uint64_t EpochPerfSumWireRowBytes(const E_WireTxn& txn)
{
    uint64_t sum = 0;
    for (const auto& row : txn.rows) {
        sum += static_cast<uint64_t>(row.tuple_data.size());
    }
    return sum;
}

void EpochPerfNoteWireTxnSend(const E_WireTxn& txn, uint64_t wire_bytes)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->zmq_wire_txn_send_count.fetch_add(1, std::memory_order_relaxed);
    perf->zmq_wire_txn_send_bytes.fetch_add(wire_bytes, std::memory_order_relaxed);
    perf->zmq_wire_row_bytes.fetch_add(EpochPerfSumWireRowBytes(txn), std::memory_order_relaxed);
    perf->zmq_wire_row_count.fetch_add(static_cast<uint64_t>(txn.rows.size()), std::memory_order_relaxed);
}

void EpochPerfNoteEpochEndSend(uint64_t wire_bytes)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->zmq_epoch_end_send_count.fetch_add(1, std::memory_order_relaxed);
    perf->zmq_epoch_end_send_bytes.fetch_add(wire_bytes, std::memory_order_relaxed);
}

void EpochPerfNoteWireRecv(const E_WireTxn& txn, uint64_t wire_bytes)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->zmq_wire_recv_bytes.fetch_add(wire_bytes, std::memory_order_relaxed);
    if (txn.start_epoch == 0 && txn.rows.empty() && txn.csn != EPOCH_WIRE_TXN_REQUEST_CSN) {
        perf->zmq_epoch_end_recv_count.fetch_add(1, std::memory_order_relaxed);
    } else {
        perf->zmq_wire_recv_count.fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochPerfNoteHelloSend(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->zmq_hello_send_count : nullptr);
}

void EpochPerfNoteTxnRequestSend(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->zmq_txn_req_send_count : nullptr);
}

static const char* EpochPerfGateName(EpochPerfGateKind kind)
{
    switch (kind) {
        case EPOCH_PERF_GATE_START:
            return "start_gate";
        case EPOCH_PERF_GATE_LOGICAL_COMMIT:
            return "logical_commit_wait";
        case EPOCH_PERF_GATE_REMOTE_EXEC:
            return "remote_exec_wait";
        case EPOCH_PERF_GATE_REMOTE_RECORD:
            return "remote_record_wait";
        case EPOCH_PERF_GATE_ASYNC_ADMIT:
            return "async_admit_wait";
        case EPOCH_PERF_GATE_ASYNC_PACK:
            return "async_pack_wait";
        default:
            return "unknown_gate";
    }
}

static void EpochPerfAppendLine(FILE* fp, const char* line)
{
    if (fp == nullptr || line == nullptr) {
        return;
    }
    fputs(line, fp);
    fputc('\n', fp);
}

void EpochPerfMaybeReport(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }

    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    if (g_last_perf_report_physical != 0 && physical <= g_last_perf_report_physical + 49) {
        return;
    }
    g_last_perf_report_physical = physical;

    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }

    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_perf")) {
        return;
    }

    FILE* fp = fopen(path, "a");
    if (fp == nullptr) {
        return;
    }

    time_t now = time(nullptr);
    struct tm tm_buf;
    (void)localtime_r(&now, &tm_buf);
    char ts[32];
    (void)strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t gap_pl = (physical >= logical) ? (physical - logical) : 0;
    uint32_t self = EpochGetNodeId();
    uint64_t total_gate_us = 0;
    for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
        total_gate_us += perf->gate_wait_us[i].load(std::memory_order_relaxed);
    }

    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "[%s] node=%u profile_gate_total_us=%lu physical=%lu logical=%lu gap=%lu commit_sync=%s",
        ts,
        self,
        total_gate_us,
        physical,
        logical,
        gap_pl,
        g_epoch_commit_sync ? "on" : "off");
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
        uint64_t wait_us = perf->gate_wait_us[i].load(std::memory_order_relaxed);
        uint64_t waits = perf->gate_wait_count[i].load(std::memory_order_relaxed);
        uint64_t spins = perf->gate_spin_count[i].load(std::memory_order_relaxed);
        if (wait_us == 0 && waits == 0) {
            continue;
        }
        rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "  GATE %s wait_us=%lu events=%lu spin_loops=%lu avg_us=%.1f",
            EpochPerfGateName(static_cast<EpochPerfGateKind>(i)),
            wait_us,
            waits,
            spins,
            waits > 0 ? (static_cast<double>(wait_us) / static_cast<double>(waits)) : 0.0);
        securec_check_ss(rc, "\0", "\0");
        EpochPerfAppendLine(fp, buf);
    }

    uint64_t wire_txn = perf->zmq_wire_txn_send_count.load(std::memory_order_relaxed);
    uint64_t wire_bytes = perf->zmq_wire_txn_send_bytes.load(std::memory_order_relaxed);
    uint64_t row_bytes = perf->zmq_wire_row_bytes.load(std::memory_order_relaxed);
    uint64_t row_count = perf->zmq_wire_row_count.load(std::memory_order_relaxed);
    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  ZMQ_SEND wire_txn=%lu wire_bytes=%lu row_bytes=%lu row_count=%lu avg_txn_bytes=%.1f avg_row_bytes=%.1f "
        "epoch_end=%lu epoch_end_bytes=%lu hello=%lu txn_req=%lu",
        wire_txn,
        wire_bytes,
        row_bytes,
        row_count,
        wire_txn > 0 ? (static_cast<double>(wire_bytes) / static_cast<double>(wire_txn)) : 0.0,
        row_count > 0 ? (static_cast<double>(row_bytes) / static_cast<double>(row_count)) : 0.0,
        perf->zmq_epoch_end_send_count.load(std::memory_order_relaxed),
        perf->zmq_epoch_end_send_bytes.load(std::memory_order_relaxed),
        perf->zmq_hello_send_count.load(std::memory_order_relaxed),
        perf->zmq_txn_req_send_count.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  ZMQ_RECV wire_txn=%lu wire_bytes=%lu epoch_end=%lu",
        perf->zmq_wire_recv_count.load(std::memory_order_relaxed),
        perf->zmq_wire_recv_bytes.load(std::memory_order_relaxed),
        perf->zmq_epoch_end_recv_count.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    uint64_t adv_try = perf->logical_advance_try.load(std::memory_order_relaxed);
    uint64_t adv_ok = perf->logical_advance_ok.load(std::memory_order_relaxed);
    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  LOGICAL advance_try=%lu advance_ok=%lu ok_rate=%.4f worker_poll=%lu physical_advance=%lu seal_ok=%lu",
        adv_try,
        adv_ok,
        adv_try > 0 ? (static_cast<double>(adv_ok) / static_cast<double>(adv_try)) : 0.0,
        perf->worker_poll_count.load(std::memory_order_relaxed),
        perf->physical_advance_count.load(std::memory_order_relaxed),
        perf->seal_ok_count.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  STALL peer_nc=%lu peer_lag=%lu phys=%lu recv=%lu merge=%lu local_cmt=%lu send=%lu reseal=%lu cmt_path=%lu record=%lu",
        perf->stall_peer_not_connected.load(std::memory_order_relaxed),
        perf->stall_peer_lag.load(std::memory_order_relaxed),
        perf->stall_physical_not_ready.load(std::memory_order_relaxed),
        perf->stall_recv_incomplete.load(std::memory_order_relaxed),
        perf->stall_merge_incomplete.load(std::memory_order_relaxed),
        perf->stall_local_commit_pending.load(std::memory_order_relaxed),
        perf->stall_send_incomplete.load(std::memory_order_relaxed),
        perf->stall_reseal_pending.load(std::memory_order_relaxed),
        perf->stall_commit_path_incomplete.load(std::memory_order_relaxed),
        perf->stall_record_incomplete.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  SEAL_BLOCK phys_delay=%lu peer_lag=%lu exec=%lu chg_set=%lu epoch_active=%lu peer_txn=%lu",
        perf->seal_block_physical_delay.load(std::memory_order_relaxed),
        perf->seal_block_peer_lag.load(std::memory_order_relaxed),
        perf->seal_block_exec_pending.load(std::memory_order_relaxed),
        perf->seal_block_change_set_pending.load(std::memory_order_relaxed),
        perf->seal_block_epoch_active.load(std::memory_order_relaxed),
        perf->seal_block_peer_txn_pending.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        uint64_t peer_pack = 0;
        uint64_t peer_should = 0;
        uint64_t peer_rcv = 0;
        for (uint64_t epoch = (logical > 8 ? logical - 8 : 1); epoch <= logical; ++epoch) {
            peer_pack += E_NodeManager::GetLocalPackTxnCounters(epoch, peer);
            peer_should += E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, peer);
            peer_rcv += E_NodeManager::GetStoreReceivedPackCounters(epoch, peer);
        }
        rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "  PEER peer=%u recent_pack=%lu should_rcv=%lu rcv=%lu peer_max_sealed=%lu",
            peer,
            peer_pack,
            peer_should,
            peer_rcv,
            EpochGetPeerMaxSealedEpoch(peer));
        securec_check_ss(rc, "\0", "\0");
        EpochPerfAppendLine(fp, buf);
    }

    EpochPerfAppendLine(fp, "");
    fclose(fp);
}
