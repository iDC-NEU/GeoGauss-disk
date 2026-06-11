//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_GLOBAL_H
#define OPENGAUSS_O_GLOBAL_H

#include <cstdint>
#include "occ/occ_mem.h"

enum AccessType : uint8_t
{
    /** @var Invalid row access code. */
    INV = 0,

    /** @var Denotes read row access code. */
    RD,

    /** @var Denotes read for update row access code. */
    RD_FOR_UPDATE,

    /** @var Denotes write row access code. */
    WR,

    /** @var Denotes delete row access code. */
    DEL,

    /** @var Denotes insert row access code. */
    INS,

    /** @var Internal code for testing. */
    SCAN,

    /** @var Unused. */
    TEST,

    INC,

    DEC
};

enum RC : uint32_t
{
    /** @var Denotes success. */
    RC_OK = 0,

    /** @var Denotes failure. */
    RC_ERROR = 1,

    /** @var Denotes operation aborted. */
    RC_ABORT,

    /** @var Denotes operation aborted due to serialization error */
    RC_SERIALIZATION_FAILURE,

    RC_UNSUPPORTED_COL_TYPE,
    RC_UNSUPPORTED_COL_TYPE_ARR,
    RC_EXCEEDS_MAX_ROW_SIZE,

    RC_COL_NAME_EXCEEDS_MAX_SIZE,

    RC_COL_SIZE_INVALID,

    RC_TABLE_EXCEEDS_MAX_DECLARED_COLS,

    RC_INDEX_EXCEEDS_MAX_SIZE,

    RC_TABLE_EXCEEDS_MAX_INDEXES,

    RC_TXN_EXCEEDS_MAX_DDLS,

    /** @var Unique constraint violation */
    RC_UNIQUE_VIOLATION,

    RC_TABLE_NOT_FOUND,
    RC_INDEX_NOT_FOUND,
    RC_LOCAL_ROW_FOUND,
    RC_LOCAL_ROW_NOT_FOUND,
    RC_LOCAL_ROW_DELETED,
    RC_PRIMARY_SENTINEL_NOT_MAPPED,
    RC_INSERT_ON_EXIST,
    RC_INDEX_RETRY_INSERT,
    RC_INDEX_DELETE,
    RC_GC_INFO_REMOVE,
    RC_LOCAL_ROW_NOT_VISIBLE,
    RC_MEMORY_ALLOCATION_ERROR,
    RC_ILLEGAL_ROW_STATE,
    RC_NULL_VIOLATION,
    RC_PANIC,
    RC_JIT_SP_EXCEPTION,
    RC_TXN_ABORTED,
    RC_CONCURRENT_MODIFICATION,
    RC_STATEMENT_CANCELED,

    /** @var operation currently n.a. */
    RC_NA,
    /** @var The maximum value for return codes. */
    RC_MAX_VALUE, // This is always the highest value of the RC list
};

#define INVALID_CSN ((uint64_t)0)

#define INITIAL_CSN ((uint64_t)3)

#endif // OPENGAUSS_O_GLOBAL_H
