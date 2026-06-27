//
// Background flusher for version staging hash table (Deferred Write-back).
//
// Framework note: production integration should drive Tick() from a postmaster
// background worker or similar scheduler. An optional std::thread loop is provided
// for standalone testing when enable_background_flusher is set.
//

#ifndef OCC_OPT_O_BACKGROUND_FLUSHER_H
#define OCC_OPT_O_BACKGROUND_FLUSHER_H

#include "o_staging_types.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>

class O_BackgroundFlusher
{
public:
    static O_BackgroundFlusher &Instance();

    void Configure(const O_StagingConfig &config);

    void Start();
    void Stop();

    /*
     * Periodic driver — safe to call from postmaster/main loop without a dedicated thread.
     */
    void Tick();

    /*
     * Request immediate flush for one page (e.g. threshold exceeded on stage).
     */
    void RequestImmediateFlush(Oid relid, BlockNumber block);

    bool IsRunning() const;

private:
    O_BackgroundFlusher();
    ~O_BackgroundFlusher();

    O_BackgroundFlusher(const O_BackgroundFlusher &) = delete;
    O_BackgroundFlusher &operator=(const O_BackgroundFlusher &) = delete;

    void RunLoop();
    void ProcessPendingFlushes();
    void FlushDuePagesByThreshold();

    O_StagingConfig m_config;
    std::atomic<bool> m_running;
    std::atomic<bool> m_stopRequested;

    std::thread m_worker;
    std::mutex m_queueMutex;
    std::condition_variable m_queueCv;
    std::unordered_set<uint64_t, std::hash<uint64_t>> m_pendingPages;

    uint64_t m_lastTickMs;

    static uint64_t EncodePageKey(Oid relid, BlockNumber block);
    static void DecodePageKey(uint64_t encoded, Oid *relid, BlockNumber *block);
};

#endif /* OCC_OPT_O_BACKGROUND_FLUSHER_H */
