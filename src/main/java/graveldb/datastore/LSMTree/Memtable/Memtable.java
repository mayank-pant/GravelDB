package graveldb.datastore.LSMTree.Memtable;

import graveldb.datastore.KeyValueStore;

public interface Memtable extends KeyValueStore, Iterable<String> {

    void updateMemtableStatus(MemtableStatus memtableStatus);
    MemtableStatus getMemtableStatus();

}
