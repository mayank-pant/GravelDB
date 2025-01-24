package graveldb.datastore.lsmtree;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.lsmtree.memtable.ConcurrentSkipListMemtable;
import graveldb.datastore.lsmtree.memtable.Memtable;
import graveldb.datastore.lsmtree.sstable.SSTable;
import graveldb.datastore.lsmtree.sstable.SSTableIO;
import graveldb.datastore.lsmtree.sstable.SSTableX;
import graveldb.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Objects;
import java.util.UUID;

public class LSMTreeX implements KeyValueStore {

    private static final Logger logger = LoggerFactory.getLogger(LSMTreeX.class);

    private Memtable memtable;
    private final LinkedList<SSTable> ssTables;
    private final WriteAheadLog writeAheadLog;
    private static final String FILE_POSTFIX = "ssfile.data";
    private static final String DATA_DIR = "./dbdata";

    public LSMTree(WriteAheadLog wal) throws IOException {
        this.writeAheadLog = wal;
        this.ssTable = new SSTableIO();
        this.memtable = new ConcurrentSkipListMemtable();
    }

    public void put(String key, String value) throws IOException {

        memtable.put(key, value);
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
            String filePath = DATA_DIR + "/" + UUID.randomUUID() +
                    "_" +
                    FILE_POSTFIX;

            // create new SSTable and pass on the file, the sstable class will manage the flushing process

            SSTableX ssTableX = new SSTableX(filePath);
            ssTableX.writeMemtable(memtable);

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
                    if (!Objects.equals(value, "")) {
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
            ssTable.compaction();
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
        if (value == null) return ssTable.get(key);
        else if (value.isEmpty()) return null;
        else return value;

    }

    public void delete(String key) throws IOException { mutMemtable.put(key, ""); }

    public int size() {
        return 1;
    }

    public String getAll() {
        return "GETALL";
    }

}