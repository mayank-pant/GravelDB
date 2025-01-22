package graveldb.datastore.lsmtree.memtable;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConcurrentSkipListMemtable implements Memtable {

    private final ConcurrentMap<String, String> concurrentMap;
    private MemtableStatus memtableStatus;

    public ConcurrentSkipListMemtable() {
        this.concurrentMap = new ConcurrentSkipListMap<>();
        this.memtableStatus = MemtableStatus.ACTIVE;
    }

    @Override
    public void updateMemtableStatus(MemtableStatus memtableStatus) {this.memtableStatus = memtableStatus;}

    @Override
    public MemtableStatus getMemtableStatus() {return memtableStatus;}

    @Override
    public void put(String key, String value) {concurrentMap.put(key, value);}

    @Override
    public String get(String key) {return concurrentMap.get(key);}

    @Override
    public void delete(String key) {concurrentMap.put(key,null);}

    @Override
    public int size() {return concurrentMap.size();}

    @Override
    public String getAll() {return concurrentMap.toString();}

    @Override
    public Iterator<String> iterator() {return concurrentMap.keySet().iterator();}
}
