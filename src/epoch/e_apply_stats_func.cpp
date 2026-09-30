#include "postgres.h"

#include "epoch/e_apply_stats.h"

#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(epoch_apply_commit_count);
PG_FUNCTION_INFO_V1(epoch_apply_abort_count);

Datum epoch_apply_commit_count(PG_FUNCTION_ARGS)
{
    uint64_t commit_count = 0;
    uint64_t abort_count = 0;

    EpochApplyStatsGetCounts(&commit_count, &abort_count);
    PG_RETURN_INT64(static_cast<int64>(commit_count));
}

Datum epoch_apply_abort_count(PG_FUNCTION_ARGS)
{
    uint64_t commit_count = 0;
    uint64_t abort_count = 0;

    EpochApplyStatsGetCounts(&commit_count, &abort_count);
    PG_RETURN_INT64(static_cast<int64>(abort_count));
}
