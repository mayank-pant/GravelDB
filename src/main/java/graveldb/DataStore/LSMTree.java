package graveldb.DataStore;

import graveldb.GravelServer;
import graveldb.WAL.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LSMTree implements KeyValueStore{

    private ConcurrentMap<String, String> mutMemtable;
    private final SSTable ssTable;
    private final WriteAheadLog writeAheadLog;
    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);


    public LSMTree(WriteAheadLog wal) {
        this.writeAheadLog = wal;
        this.ssTable = new SSTable();
        this.mutMemtable = new ConcurrentSkipListMap<>();
    }

    public String put(String key, String value) throws IOException {

        mutMemtable.put(key, value);
        if (!canFlushMemtable()) return "OK";

        try {
            flushMemtable(mutMemtable);
            mutMemtable = new ConcurrentSkipListMap<>();
        } catch (Exception e) {
            logger.error("error flushing the memtable");
            throw e;
        }

        return "OK";

    }

    public void flushMemtable(ConcurrentMap<String, String> memtable) throws IOException {
        ssTable.flushMemtable(memtable);
        writeAheadLog.clear();
    }

    public boolean canFlushMemtable() {
        return mutMemtable.size() > 10;
    }

    public String get(String key) {
        if (mutMemtable.containsKey(key)) return mutMemtable.get(key);

        return ssTable.get(key);

    }

    public String delete(String key) {
        return "delete";
    }

    public boolean containsKey(String key) {
        return true;
    }

    public int size() {
        return 1;
    }

    public String getAll() {
        return "B";
    }

}
