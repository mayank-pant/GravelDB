package graveldb.datastore.lsmtree;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.lsmtree.memtable.ConcurrentSkipListMemtable;
import graveldb.datastore.lsmtree.memtable.Memtable;
import graveldb.datastore.lsmtree.memtable.MemtableStatus;
import graveldb.datastore.lsmtree.sstable.SSTableImpl;
import graveldb.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class LSMTree implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(LSMTree.class);

    private static final String FILE_POSTFIX = "_ssfile.data";
    private static final String DATA_DIR = "./dbdata/";

    private Memtable mutMemtable;
    private final LinkedList<Memtable> immMemtable;
    private final LinkedList<SSTableImpl> ssTables;
    private final WriteAheadLog mutWal;
    private final ConcurrentHashMap<Memtable, WriteAheadLog> memtableToWalfile;

    // private static final Object memtableLock = new Object();

    ScheduledExecutorService memtableFlusher;
    ScheduledExecutorService tableCompactor;

    public LSMTree() throws IOException {
        this.ssTables = new LinkedList<>();
        this.mutMemtable = new ConcurrentSkipListMemtable();
        this.immMemtable = new LinkedList<>();
        this.memtableToWalfile = new ConcurrentHashMap<>();
        this.mutWal = new WriteAheadLog();

        memtableToWalfile.put(mutMemtable, mutWal);

        fillSstableList();

        memtableFlusher = newSingleThreadScheduledExecutor();
        memtableFlusher.scheduleAtFixedRate(this::flushMemtable, 50, 50, TimeUnit.MILLISECONDS);

        tableCompactor = Executors.newScheduledThreadPool(3);
        tableCompactor.scheduleAtFixedRate(this::compaction, 200, 200, TimeUnit.MILLISECONDS);
    }

    private void fillSstableList() throws IOException {

        Files.createDirectories(Paths.get(DATA_DIR));

        File directory = new File(DATA_DIR);
        File[] files = directory.listFiles();
        if (files == null) return;

        Arrays.sort(files, Comparator.comparingLong(File::lastModified));

        for (File f : files){
            ssTables.addFirst(new SSTableImpl(DATA_DIR + f.getName()));
        }
    }

    @Override
    public void put(String key, String value) throws IOException {
        mutWal.append("SET", key, value);
        mutMemtable.put(key, value);

        if (mutMemtable.canFlush()) {
            synchronized (immMemtable) {
                immMemtable.addFirst(mutMemtable);
                mutMemtable.updateMemtableStatus(MemtableStatus.TO_BE_FLUSHED);
                mutMemtable = new ConcurrentSkipListMemtable();
            }
        }
    }

    @Override
    public String get(String key) {
        String value = mutMemtable.get(key);
        if (value == null) return getFromSstable(key);
        else if (value.isEmpty()) return null;
        else return value;
    }

    @Override
    public void delete(String key) throws IOException {
        mutWal.append("DEL", key, null);
        mutMemtable.put(key, "");
    }

    public void flushMemtable() {
        try {
            Memtable toBeFlushedMemtable = null;

            synchronized (immMemtable) {
                for (Iterator<Memtable> it = immMemtable.descendingIterator(); it.hasNext();) {
                    Memtable curMemtable = it.next();
                    if (curMemtable.getMemtableStatus().equals(MemtableStatus.ACTIVE)) {
                        toBeFlushedMemtable = curMemtable;
                        curMemtable.updateMemtableStatus(MemtableStatus.FLUSHING);
                        break;
                    }
                }
            }

            if (toBeFlushedMemtable == null) return;

            String ssTableFilePath = DATA_DIR + UUID.randomUUID() + FILE_POSTFIX;

            SSTableImpl ssTable = new SSTableImpl(ssTableFilePath);
            SSTableImpl.SSTableWriter ssTableWriter = ssTable.getWriter();
            Iterator<KeyValuePair> memtableIterator = toBeFlushedMemtable.iterator();

            try (ssTableWriter) {
                while (memtableIterator.hasNext()) {
                    ssTableWriter.write(memtableIterator.next());
                }
            }

            synchronized (immMemtable) {
                immMemtable.pollLast();
            }

            synchronized (ssTables) {
                ssTables.add(ssTable);
            }

            memtableToWalfile.get(mutMemtable).delete();
        } catch (Exception e) {
            log.error("error while converting memtable to sstable", e);
        }
    }

    public String getFromSstable(String targetKey) {
        for (SSTableImpl sstable: ssTables) {
            try (SSTableImpl.SSTableIterator itr = sstable.iterator()) {
                while (itr.hasNext()) {
                    KeyValuePair kvp = itr.next();
                    if (kvp.key().equals(targetKey)) {
                        return kvp.value().isEmpty() ? null : kvp.value();
                    }
                }
            } catch (IOException e) {
                log.error("error reading file",e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public void compaction() {
        // check the size of sstables, if it >= 2 then start compaction
        // pop the last two sstables

        if (ssTables.size() < 2) return;

        // get sstable to compact and there readers / iterator
        SSTableImpl ssTable2 = ssTables.pollLast();
        SSTableImpl ssTable1 = ssTables.pollLast();
        for (Iterator<SSTableImpl> it = ssTables.iterator(); it.hasNext(); ) {
            SSTableImpl curSstable = it.next();
            ssTable2 = curSstable;
            ssTable1 = curSstable;
        }
        SSTableImpl ssTable2 = ssTables.pollLast();
        SSTableImpl ssTable1 = ssTables.pollLast();
        if (ssTable1 == null || ssTable2 == null) return;
        SSTableImpl.SSTableIterator fis2 = ssTable2.iterator();
        SSTableImpl.SSTableIterator fis1 = ssTable1.iterator();

        // create new sstable and get its writer
        String outputFileName = DATA_DIR + UUID.randomUUID() + FILE_POSTFIX;
        SSTableImpl ssTableNew = new SSTableImpl(outputFileName);
        SSTableImpl.SSTableWriter fos = ssTableNew.getWriter();

        try(fis1; fis2; fos) {
            KeyValuePair kvp1 = null;
            KeyValuePair kvp2 = null;

            // both sstable has something to read
            if (fis1.hasNext() && fis2.hasNext()) {
                // get next key value pair
                kvp1 = fis1.next();
                kvp2 = fis2.next();

                while (kvp1 != null && kvp2 != null) {
                    int cmp = kvp1.key().compareTo(kvp2.key());
                    // if key1 is lexico smaller then add it to sstable
                    if (cmp < 0) {
                        if (!kvp1.isDeleted()) { fos.write(kvp1); }
                        kvp1 = null;
                        if (fis1.hasNext()) kvp1 = fis1.next();
                        else break;
                    // if key2 is lexico smaller then add it
                    } else if (cmp > 0) {
                        if (!kvp2.isDeleted()) { fos.write(kvp2); }
                        kvp2 = null;
                        if (fis2.hasNext()) kvp2 = fis2.next();
                        else break;
                    // if both key are equal add key1 is not deleted then add key1 to sstable
                    } else {
                        if (!kvp1.isDeleted()) { fos.write(kvp1); }
                        kvp1 = null; kvp2 = null;
                        if (fis1.hasNext()) kvp1 = fis1.next();
                        else break;
                        if (fis2.hasNext()) kvp2 = fis2.next();
                        else break;
                    }
                }
            }

            while (kvp2 != null) {
                if (!kvp2.isDeleted()) fos.write(kvp2);
                if (fis2.hasNext()) kvp2 = fis2.next();
                else break;
            }

            while (kvp1 != null) {
                if (!kvp1.isDeleted()) fos.write(kvp1);
                if (fis1.hasNext()) kvp1 = fis1.next();
                else break;
            }
        }

        Files.delete(Path.of(ssTable1.getFileName()));
        Files.delete(Path.of(ssTable2.getFileName()));

        ssTables.addLast(ssTableNew);
    }
}