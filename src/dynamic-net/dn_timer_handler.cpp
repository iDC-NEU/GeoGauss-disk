#include "dn_timer_handler.h"
#include "dn_api.h"

DnTimerTickResult DnTimerHandler::OnTimerTick(const DnTimerTickInput &input)
{
    DnTimerTickResult result;
    DnRuntime &rt = DnRuntime::Instance();
    if (!rt.GetConfig().node_count) {
        return result;
    }

    result.strategy = rt.ConsultProposal(input.local_proposal_epoch);
    result.should_propose = result.strategy.should_propose;
    if (!result.should_propose) {
        return result;
    }

    int32_t epoch_to_propose = result.strategy.target_epoch;
    result.log_epoch_updates.clear();

    if (result.strategy.action == DnStrategyAction::JUMP) {
        result.log_epoch_updates = rt.ExecuteCatchupJump(epoch_to_propose);

        int32_t max_new_epoch = epoch_to_propose;
        for (const auto &kv : result.log_epoch_updates) {
            if (kv.second > max_new_epoch) {
                max_new_epoch = kv.second;
            }
        }
        epoch_to_propose = max_new_epoch + 1;
        result.strategy.jump_delta = epoch_to_propose - input.local_proposal_epoch;
    }

    result.proposed_entry.term = input.current_term;
    result.proposed_entry.index = input.next_log_index;
    result.proposed_entry.epoch = epoch_to_propose;

    if (input.pending_txns.empty()) {
        result.proposed_entry.type = DnEntryType::NOOP;
    } else {
        result.proposed_entry.type = DnEntryType::WRITE;
        result.proposed_entry.txns = input.pending_txns;
    }

    result.next_local_epoch = epoch_to_propose + 1;
    return result;
}
