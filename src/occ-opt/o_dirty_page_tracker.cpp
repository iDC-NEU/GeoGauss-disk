//
// Page-level dirty tuple tracking implementation.
//

#include "o_dirty_page_tracker.h"

O_DirtyPageTracker &O_DirtyPageTracker::Instance()
{
    static O_DirtyPageTracker instance;
    return instance;
}

O_DirtyPageTracker::O_DirtyPageTracker() : m_threshold(32) {}

void O_DirtyPageTracker::Configure(uint32_t dirty_threshold_per_page)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_threshold = dirty_threshold_per_page > 0 ? dirty_threshold_per_page : 1;
}

bool O_DirtyPageTracker::RecordStaged(Oid relid, ItemPointer tid, uint64_t csn, bool is_new_slot)
{
    if (tid == nullptr) {
        return false;
    }

    O_StagingPageKey pageKey = O_StagingPageKeyFromTid(relid, tid);
    std::lock_guard<std::mutex> lock(m_mutex);
    O_DirtyPageInfo &info = m_pages[pageKey];
    info.last_staged_csn = csn;
    if (is_new_slot) {
        info.dirty_count++;
    }
    return info.dirty_count >= m_threshold;
}

void O_DirtyPageTracker::RecordFlushed(Oid relid, BlockNumber block, uint32_t flushed_count)
{
    O_StagingPageKey pageKey;
    pageKey.relid = relid;
    pageKey.block = block;

    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_pages.find(pageKey);
    if (it == m_pages.end()) {
        return;
    }

    if (flushed_count >= it->second.dirty_count) {
        m_pages.erase(it);
        return;
    }
    it->second.dirty_count -= flushed_count;
}

bool O_DirtyPageTracker::ShouldFlush(const O_StagingPageKey &pageKey) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_pages.find(pageKey);
    if (it == m_pages.end()) {
        return false;
    }
    return it->second.dirty_count >= m_threshold;
}

uint32_t O_DirtyPageTracker::GetDirtyCount(const O_StagingPageKey &pageKey) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_pages.find(pageKey);
    if (it == m_pages.end()) {
        return 0;
    }
    return it->second.dirty_count;
}

void O_DirtyPageTracker::GetPagesExceedingThreshold(std::vector<O_StagingPageKey> &outPages) const
{
    outPages.clear();
    std::lock_guard<std::mutex> lock(m_mutex);
    for (const auto &kv : m_pages) {
        if (kv.second.dirty_count >= m_threshold) {
            outPages.push_back(kv.first);
        }
    }
}

void O_DirtyPageTracker::GetAllDirtyPages(std::vector<O_StagingPageKey> &outPages) const
{
    outPages.clear();
    std::lock_guard<std::mutex> lock(m_mutex);
    outPages.reserve(m_pages.size());
    for (const auto &kv : m_pages) {
        outPages.push_back(kv.first);
    }
}

void O_DirtyPageTracker::ClearPage(const O_StagingPageKey &pageKey)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_pages.erase(pageKey);
}

void O_DirtyPageTracker::ClearAll()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_pages.clear();
}

uint64_t O_DirtyPageTracker::GetTrackedPageCount() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_pages.size();
}
