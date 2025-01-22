package graveldb.datastore.lsmtree.memtable;

import graveldb.datastore.KeyValueStore;

public interface Memtable extends KeyValueStore, Iterable<String> {

    void updateMemtableStatus(MemtableStatus memtableStatus);
    MemtableStatus getMemtableStatus();

}
