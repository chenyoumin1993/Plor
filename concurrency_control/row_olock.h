#ifndef ROW_OLOCK_H
#define ROW_OLOCK_H

/*********************************
 Two types of implementation:
 1) maintain a lockfree list.
 2) using a distributed lock.
 ********************************/


class Row_olock {
public:
    void init(row_t *row);
    RC lock_get(lock_t type, txn_man *txn);
    RC lock_release(txn_man *txn);

private:
    // ...
    row_t *_row;
};

#endif