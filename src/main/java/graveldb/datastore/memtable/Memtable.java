package graveldb.datastore.memtable;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.lsmtree.KeyValuePair;

public interface Memtable extends KeyValueStore, Iterable<KeyValuePair> {

    void updateMemtableStatus(MemtableStatus memtableStatus);
    MemtableStatus getMemtableStatus();
    boolean canFlush();

}
