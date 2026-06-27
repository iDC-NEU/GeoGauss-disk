//
// Global epoch barrier + FIXED/CATCHUP implementation.
//

#include "dn_epoch_manager.h"

#include <algorithm>
#include <climits>
#include <memory>

DnEpochManager::DnEpochManager(int node_id, const DnNetworkConfig *config, DnEpochObserver *observer)
    : m_nodeId(node_id), m_config(config), m_observer(observer), m_globalEpoch(1)
{
    int count = config != nullptr ? config->node_count : 3;
    m_executeIndexes.assign(count, 1);
}

void DnEpochManager::Configure(const DnNetworkConfig *config)
{
    m_config = config;
    if (config != nullptr && static_cast<int>(m_executeIndexes.size()) != config->node_count) {
        m_executeIndexes.assign(config->node_count, 1);
    }
}

void DnEpochManager::RecordObservedEpochFromAppend(int32_t sender_epoch, int from_node, int to_node)
{
    if (m_observer != nullptr) {
        m_observer->RecordObservedEpoch(sender_epoch, from_node, to_node);
    }
}

DnProposalStrategy DnEpochManager::ConsultProposalStrategy(int32_t current_local_proposal_epoch,
                                                             int32_t current_net_queue_size,
                                                             bool is_slow_node)
{
    DnProposalStrategy result;
    result.should_propose = true;
    result.action = DnStrategyAction::NORMAL;
    result.target_epoch = current_local_proposal_epoch;
    result.jump_delta = 0;

    int projected_max = m_observer != nullptr ? m_observer->GetMaxObservedEpoch() : current_local_proposal_epoch;
    int lag = projected_max - current_local_proposal_epoch;
    result.lag = lag;

    if (m_config == nullptr ||
        m_config->advancement != DnEpochAdvancement::CATCHUP || !is_slow_node) {
        return result;
    }

    bool lag_trigger = lag > m_config->lag_tolerance;
    bool queue_trigger = current_net_queue_size > m_config->queue_size_tolerance;
    if (!lag_trigger && !queue_trigger) {
        return result;
    }

    int target_jump = projected_max + m_config->catchup_factor_k;
    int delta = target_jump - current_local_proposal_epoch;
    if (delta > 0) {
        result.action = DnStrategyAction::JUMP;
        result.target_epoch = target_jump;
        result.jump_delta = delta;
    }
    return result;
}

bool DnEpochManager::IsBarrierReady(uint64_t target_epoch,
                                    const std::unordered_map<int32_t, DnPeerLogState> &logs)
{
    if (m_config == nullptr) {
        return false;
    }

    for (int i = 0; i < m_config->node_count; ++i) {
        auto it = logs.find(i);
        if (it == logs.end()) {
            return false;
        }
        const DnPeerLogState &log = it->second;
        int current_idx = m_executeIndexes[i];
        int commit_idx = log.commit_index.load(std::memory_order_acquire);
        bool channel_ready = false;

        for (int idx = current_idx; idx < static_cast<int>(log.entries.size()) && idx <= commit_idx; ++idx) {
            int e = log.entries[idx].epoch;
            if (e >= static_cast<int>(target_epoch)) {
                channel_ready = true;
                break;
            }
        }
        if (!channel_ready) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<DnEpochBatch> DnEpochManager::TryCollectBatch(
    const std::unordered_map<int32_t, DnPeerLogState> &logs)
{
    std::lock_guard<std::mutex> lock(m_executionMutex);
    uint64_t target_epoch = m_globalEpoch.load(std::memory_order_relaxed);

    if (!IsBarrierReady(target_epoch, logs)) {
        return nullptr;
    }

    uint64_t min_actual_epoch = UINT64_MAX;
    for (int i = 0; i < m_config->node_count; ++i) {
        auto it = logs.find(i);
        if (it == logs.end()) {
            continue;
        }
        const DnPeerLogState &log = it->second;
        int exec_idx = m_executeIndexes[i];
        int commit_idx = log.commit_index.load(std::memory_order_acquire);
        for (int idx = exec_idx; idx < static_cast<int>(log.entries.size()) && idx <= commit_idx; ++idx) {
            int e = log.entries[idx].epoch;
            if (e >= static_cast<int>(target_epoch)) {
                min_actual_epoch = std::min(min_actual_epoch, static_cast<uint64_t>(e));
                break;
            }
        }
    }

    if (min_actual_epoch != UINT64_MAX && min_actual_epoch > target_epoch) {
        bool has_data_in_gap = false;
        for (int i = 0; i < m_config->node_count; ++i) {
            auto it = logs.find(i);
            if (it == logs.end()) {
                continue;
            }
            const DnPeerLogState &log = it->second;
            int exec_idx = m_executeIndexes[i];
            int commit_idx = log.commit_index.load(std::memory_order_acquire);
            for (int idx = exec_idx; idx < static_cast<int>(log.entries.size()) && idx <= commit_idx; ++idx) {
                int e = log.entries[idx].epoch;
                if (e >= static_cast<int>(target_epoch) && e < static_cast<int>(min_actual_epoch)) {
                    has_data_in_gap = true;
                    break;
                }
            }
            if (has_data_in_gap) {
                break;
            }
        }
        if (!has_data_in_gap) {
            m_globalEpoch.store(min_actual_epoch, std::memory_order_relaxed);
            target_epoch = min_actual_epoch;
        }
    }

    auto batch = std::make_unique<DnEpochBatch>();
    batch->epoch_id = target_epoch;

    for (int i = 0; i < m_config->node_count; ++i) {
        auto it = logs.find(i);
        if (it == logs.end()) {
            continue;
        }
        const DnPeerLogState &log = it->second;
        int &exec_idx = m_executeIndexes[i];
        int commit_idx = log.commit_index.load(std::memory_order_acquire);

        while (exec_idx < static_cast<int>(log.entries.size()) && exec_idx <= commit_idx) {
            const DnRaftEntry &entry = log.entries[exec_idx];
            if (entry.epoch < static_cast<int>(target_epoch)) {
                exec_idx++;
                continue;
            }
            if (entry.epoch > static_cast<int>(target_epoch)) {
                break;
            }
            if (entry.type == DnEntryType::WRITE) {
                for (const auto &txn : entry.txns) {
                    batch->transactions.push_back(txn);
                }
            }
            exec_idx++;
            break;
        }
    }

    m_globalEpoch.fetch_add(1, std::memory_order_relaxed);
    return batch;
}

uint64_t DnEpochManager::GetGlobalEpoch() const
{
    return m_globalEpoch.load(std::memory_order_acquire);
}

const std::vector<int32_t> &DnEpochManager::GetExecuteIndexes() const
{
    return m_executeIndexes;
}

void DnEpochManager::Reset()
{
    std::lock_guard<std::mutex> lock(m_executionMutex);
    m_globalEpoch.store(1, std::memory_order_relaxed);
    std::fill(m_executeIndexes.begin(), m_executeIndexes.end(), 1);
}
