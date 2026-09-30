#include "postgres.h"

#include "epoch/e_log_path.h"
#include "epoch/e_shmem.h"

#include "gs_thread.h"
#include "miscadmin.h"

#include <cstdio>
#include <cstring>
#include <ctime>

static char g_epoch_log_stamp_local[17] = {0};
static char g_epoch_cached_data_dir[MAXPGPATH] = {0};

void EpochSetAuxThreadName(const char* name)
{
    if (name == nullptr || name[0] == '\0') {
        return;
    }
    (void)ShowThreadName(name);
}

void EpochLogPathInit(void)
{
    if (t_thrd.proc_cxt.DataDir == nullptr || t_thrd.proc_cxt.DataDir[0] == '\0') {
        return;
    }
    errno_t rc = strcpy_s(g_epoch_cached_data_dir, sizeof(g_epoch_cached_data_dir), t_thrd.proc_cxt.DataDir);
    securec_check(rc, "\0", "\0");
}

static const char* EpochGetDataDirForLogs(void)
{
    if (g_epoch_cached_data_dir[0] != '\0') {
        return g_epoch_cached_data_dir;
    }
    if (t_thrd.proc_cxt.DataDir != nullptr && t_thrd.proc_cxt.DataDir[0] != '\0') {
        return t_thrd.proc_cxt.DataDir;
    }
    return nullptr;
}

static const char* EpochGetLogStamp(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state != nullptr && state->log_file_stamp[0] != '\0') {
        return state->log_file_stamp;
    }
    if (g_epoch_log_stamp_local[0] == '\0') {
        time_t now = time(nullptr);
        struct tm tm_buf;
        (void)localtime_r(&now, &tm_buf);
        (void)strftime(g_epoch_log_stamp_local, sizeof(g_epoch_log_stamp_local), "%Y%m%d_%H%M%S", &tm_buf);
    }
    return g_epoch_log_stamp_local;
}

bool EpochFormatLogPath(char* path, size_t path_size, const char* basename)
{
    if (path == nullptr || path_size == 0 || basename == nullptr || basename[0] == '\0') {
        return false;
    }
    const char* data_dir = EpochGetDataDirForLogs();
    if (data_dir == nullptr) {
        return false;
    }
    const char* stamp = EpochGetLogStamp();
    int rc = snprintf_s(path, path_size, path_size - 1, "%s/%s_%s.log", data_dir, basename, stamp);
    securec_check_ss(rc, "\0", "\0");
    return true;
}
