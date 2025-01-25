package graveldb.datastore.lsmtree.memtable;

import graveldb.datastore.lsmtree.KeyValuePair;

import java.util.Iterator;
import java.util.Map;
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
    public boolean canFlush() { return concurrentMap.size() > 10; }

    @Override
    public void put(String key, String value) {concurrentMap.put(key, value);}

    @Override
    public String get(String key) {return concurrentMap.get(key);}

    @Override
    public void delete(String key) {concurrentMap.put(key,null);}

    @Override
    public Iterator<KeyValuePair> iterator() {
        return new MemtableIterator();
    }

    public class MemtableIterator implements Iterator<KeyValuePair> {

        Iterator<Map.Entry<String, String>> itr;

        public MemtableIterator() {
            itr = concurrentMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return itr.hasNext();
        }

        @Override
        public KeyValuePair next() {
            Map.Entry<String, String> entry = itr.next();
            String key = entry.getKey();
            String value = entry.getValue();
            boolean isDeleted = entry.getValue().isEmpty();

            return new KeyValuePair(key, value, isDeleted);
        }
    }
}
