//
// Page-level dirty tuple tracking for deferred write-back.
//

#ifndef OCC_OPT_O_DIRTY_PAGE_TRACKER_H
#define OCC_OPT_O_DIRTY_PAGE_TRACKER_H

#include "o_staging_types.h"

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct O_DirtyPageInfo
{
    uint32_t dirty_count;
    uint64_t last_staged_csn;

    O_DirtyPageInfo() : dirty_count(0), last_staged_csn(0) {}
};

class O_DirtyPageTracker
{
public:
    static O_DirtyPageTracker &Instance();

    void Configure(uint32_t dirty_threshold_per_page);

    /*
     * Record that one tuple slot on the page was newly staged.
     * Only increments dirty_count when is_new_slot is true (not an overwrite).
     * Returns true if the page now exceeds the flush threshold.
     */
    bool RecordStaged(Oid relid, ItemPointer tid, uint64_t csn, bool is_new_slot);

    /*
     * Decrement dirty count after a tuple is flushed; remove page entry when zero.
     */
    void RecordFlushed(Oid relid, BlockNumber block, uint32_t flushed_count);

    bool ShouldFlush(const O_StagingPageKey &pageKey) const;

    uint32_t GetDirtyCount(const O_StagingPageKey &pageKey) const;

    /*
     * Pages whose dirty tuple count >= threshold.
     */
    void GetPagesExceedingThreshold(std::vector<O_StagingPageKey> &outPages) const;

    /*
     * All pages with any staged dirty tuples.
     */
    void GetAllDirtyPages(std::vector<O_StagingPageKey> &outPages) const;

    void ClearPage(const O_StagingPageKey &pageKey);

    void ClearAll();

    uint64_t GetTrackedPageCount() const;

private:
    O_DirtyPageTracker();

    uint32_t m_threshold;
    mutable std::mutex m_mutex;
    std::unordered_map<O_StagingPageKey, O_DirtyPageInfo, O_StagingPageKeyHash> m_pages;
};

#endif /* OCC_OPT_O_DIRTY_PAGE_TRACKER_H */
