//
// Created by hcj2003 on 10/27/25.
//

#ifndef OPENGAUSS_O_CSN_MANAGER_H
#define OPENGAUSS_O_CSN_MANAGER_H

#include <atomic>
#include "o_global.h"

class O_CSNManager{
public:
    /** @brief Constructor. */
    O_CSNManager();

    /** @brief Destructor. */
    ~O_CSNManager()
    {}

    /** @brief Get the next csn. This method also increment the current csn. */
    uint64_t GetNextCSN()
    {
        uint64_t current = INVALID_CSN;
        uint64_t next = INVALID_CSN;
        do {
            current = m_csn;
            next = current + 1;
        } while (!m_csn.compare_exchange_strong(current, next, std::memory_order_acq_rel));
        return current;
    }

    uint64_t GetCurrentCSN()
    {
        return m_csn;
    }

    uint64_t GetGcEpoch()
    {
        return m_csn;
    }

    void SetCSN(uint64_t value);

private:
    std::atomic<uint64_t> m_csn;
};

#endif  // OPENGAUSS_O_CSN_MANAGER_H
