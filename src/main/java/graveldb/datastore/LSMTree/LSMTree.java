package graveldb.datastore.LSMTree;

import graveldb.datastore.*;
import graveldb.datastore.LSMTree.Memtable.ConcurrentSkipListMemtable;
import graveldb.datastore.LSMTree.Memtable.Memtable;
import graveldb.WAL.WriteAheadLog;
import graveldb.datastore.LSMTree.SSTable.SSTable;
import graveldb.datastore.LSMTree.SSTable.SSTableIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LSMTree implements KeyValueStore {

    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);

    private Memtable mutMemtable;
    private final SSTable ssTable;
    private final WriteAheadLog writeAheadLog;
    private static final String FILE_POSTFIX = "ssfile.data";

    public LSMTree(WriteAheadLog wal) {
        this.writeAheadLog = wal;
        this.ssTable = new SSTableIO();
        this.mutMemtable = new ConcurrentSkipListMemtable();
    }

    public void put(String key, String value) throws IOException {

        mutMemtable.put(key, value);
        if (!canFlushMemtable()) return;

        try {
            flushMemtable(mutMemtable);
            mutMemtable = new ConcurrentSkipListMemtable();
        } catch (Exception e) {
            logger.error("error flushing the memtable");
            throw e;
        }

    }

    public void flushMemtable(Memtable memtable) {
        try {
            String filePath = memtable.hashCode() +
                    "_" +
                    FILE_POSTFIX;

            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
            try (bos) {
                for (String key : memtable) {
                    int capacity = 0;

                    byte[] keyBytes = key.getBytes();
                    int keyLen = keyBytes.length;
                    capacity += 4;
                    capacity += keyBytes.length;

                    String value = memtable.get(key);
                    byte[] valueBytes = new byte[]{0};
                    int valueLen = 0;
                    capacity += 4;
                    if (value != null) {
                        valueBytes = value.getBytes();
                        capacity += valueBytes.length;
                        valueLen = valueBytes.length;
                    }

                    ByteBuffer pairEntry = ByteBuffer.allocate(capacity);

                    pairEntry.putInt(keyLen);
                    pairEntry.putInt(valueLen);
                    pairEntry.put(keyBytes);
                    if (valueLen != 0) pairEntry.put(valueBytes);

                    bos.write(pairEntry.array());

                }
            } catch (Exception e) {
                logger.error("error creating sstable",e);
            }

            ssTable.addSSTable(filePath);
            writeAheadLog.clear();
        } catch (Exception e) {
            logger.error("error while converting memtable to sstable",e);
        }
    }

    public boolean canFlushMemtable() {
        return mutMemtable.size() > 10;
    }

    public String get(String key) {
        String value = mutMemtable.get(key);
        if (value != null) return value;

        return ssTable.get(key);

    }

    public void delete(String key) throws IOException { mutMemtable.put(key, null); }

    public int size() {
        return 1;
    }

    public String getAll() {
        return "GETALL";
    }

}
