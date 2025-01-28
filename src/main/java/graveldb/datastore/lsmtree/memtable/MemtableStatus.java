package graveldb.datastore.lsmtree.memtable;

public enum MemtableStatus {
    ACTIVE,
    TO_BE_FLUSHED,
    FLUSHING,
    FLUSHED
}
