#include "postgres.h"

#include "occ/o_config.h"
#include "occ/o_staging.h"
#include "occ/occ_stats.h"

#include "utils/guc.h"

bool g_enable_occ_disk = true;

void OccConfigRegisterGucs(void)
{
    DefineCustomBoolVariable("enable_occ_disk",
                             "Enable OCC-based disk table concurrency control on local node.",
                             NULL,
                             &g_enable_occ_disk,
                             true,
                             PGC_USERSET,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("occ_enable_stats_log",
                             "Log OCC_STATS summary to pg_log on every commit/abort (default: sample every 1024).",
                             NULL,
                             &g_occ_enable_stats_log,
                             false,
                             PGC_SIGHUP,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    OccStagingRegisterGucs();
}
