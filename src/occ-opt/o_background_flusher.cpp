//
// Background flusher implementation.
//

#include "o_background_flusher.h"
#include "o_staging_flush.h"
#include "o_dirty_page_tracker.h"

#include "postgres.h"

#include <chrono>

namespace {

uint64_t O_StagingNowMs()
{
    using clock = std::chrono::steady_clock;
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(clock::now().time_since_epoch())
            .count());
}

}  // namespace

O_BackgroundFlusher &O_BackgroundFlusher::Instance()
{
    static O_BackgroundFlusher instance;
    return instance;
}

O_BackgroundFlusher::O_BackgroundFlusher()
    : m_running(false), m_stopRequested(false), m_lastTickMs(0)
{
}

O_BackgroundFlusher::~O_BackgroundFlusher()
{
    Stop();
}

void O_BackgroundFlusher::Configure(const O_StagingConfig &config)
{
    m_config = config;
    O_DirtyPageTracker::Instance().Configure(config.dirty_threshold_per_page);
}

void O_BackgroundFlusher::Start()
{
    if (m_running.load()) {
        return;
    }
    if (!m_config.enable_background_flusher) {
        return;
    }

    m_stopRequested.store(false);
    m_running.store(true);
    m_worker = std::thread(&O_BackgroundFlusher::RunLoop, this);
}

void O_BackgroundFlusher::Stop()
{
    if (!m_running.load()) {
        return;
    }

    m_stopRequested.store(true);
    m_queueCv.notify_all();
    if (m_worker.joinable()) {
        m_worker.join();
    }
    m_running.store(false);

    std::lock_guard<std::mutex> lock(m_queueMutex);
    m_pendingPages.clear();
}

void O_BackgroundFlusher::Tick()
{
    uint64_t now = O_StagingNowMs();
    if (m_lastTickMs != 0 && (now - m_lastTickMs) < m_config.flush_interval_ms) {
        ProcessPendingFlushes();
        return;
    }
    m_lastTickMs = now;

    ProcessPendingFlushes();
    FlushDuePagesByThreshold();
}

void O_BackgroundFlusher::RequestImmediateFlush(Oid relid, BlockNumber block)
{
    uint64_t encoded = EncodePageKey(relid, block);
    {
        std::lock_guard<std::mutex> lock(m_queueMutex);
        m_pendingPages.insert(encoded);
    }
    m_queueCv.notify_one();
}

bool O_BackgroundFlusher::IsRunning() const
{
    return m_running.load();
}

void O_BackgroundFlusher::RunLoop()
{
    while (!m_stopRequested.load()) {
        std::unique_lock<std::mutex> lock(m_queueMutex);
        m_queueCv.wait_for(lock, std::chrono::milliseconds(m_config.flush_interval_ms),
                           [this]() { return m_stopRequested.load() || !m_pendingPages.empty(); });
        lock.unlock();

        if (m_stopRequested.load()) {
            break;
        }

        Tick();
    }
}

void O_BackgroundFlusher::ProcessPendingFlushes()
{
    std::unordered_set<uint64_t, std::hash<uint64_t>> localPending;
    {
        std::lock_guard<std::mutex> lock(m_queueMutex);
        localPending.swap(m_pendingPages);
    }

    O_StagingFlush &flusher = O_StagingFlush::Instance();
    for (uint64_t encoded : localPending) {
        Oid relid = InvalidOid;
        BlockNumber block = 0;
        DecodePageKey(encoded, &relid, &block);
        flusher.FlushPage(relid, block);
    }
}

void O_BackgroundFlusher::FlushDuePagesByThreshold()
{
    std::vector<O_StagingPageKey> pages;
    O_DirtyPageTracker::Instance().GetPagesExceedingThreshold(pages);

    O_StagingFlush &flusher = O_StagingFlush::Instance();
    for (const O_StagingPageKey &pageKey : pages) {
        flusher.FlushPage(pageKey.relid, pageKey.block);
    }
}

uint64_t O_BackgroundFlusher::EncodePageKey(Oid relid, BlockNumber block)
{
    return (static_cast<uint64_t>(relid) << 32) | static_cast<uint64_t>(block);
}

void O_BackgroundFlusher::DecodePageKey(uint64_t encoded, Oid *relid, BlockNumber *block)
{
    *relid = static_cast<Oid>(encoded >> 32);
    *block = static_cast<BlockNumber>(encoded & 0xFFFFFFFFu);
}
