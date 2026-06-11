#include "occ/o_txn.h"
#include "access/xact.h"
#include "occ/occ_mem.h"

void OCCXactCallback(XactEvent event);

RC O_ValidateCommit();

void O_RecordCommit(uint64_t csn);