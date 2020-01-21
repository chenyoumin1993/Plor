#include "row.h"
#include "txn.h"
#include "row_olock.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_olock::init(row_t *row) {
    _row = row;
}

RC Row_olock::lock_get(lock_t type, txn_man *txn) {
    assert(CC_ALG == OLOCK);
    assert(type == LOCK_EX);
    
    // Insert a lock in the owner list via lockfree technique.
    return WAIT;
}

RC Row_olock::lock_release(txn_man *txn) {return RCOK;}