package graveldb.datastore.lsmtree;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.lsmtree.memtable.ConcurrentSkipListMemtable;
import graveldb.datastore.lsmtree.memtable.Memtable;
import graveldb.datastore.lsmtree.sstable.SSTableImpl;
import graveldb.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class LSMTree implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(LSMTree.class);

    private Memtable memtable;
    private LinkedList<Memtable> immMemtable;
    private final LinkedList<SSTableImpl> ssTables;
    private final WriteAheadLog writeAheadLog;
    private static final String FILE_POSTFIX = "_ssfile.data";
    private static final String DATA_DIR = "./dbdata/";

    public LSMTree(WriteAheadLog wal) throws IOException {
        this.writeAheadLog = wal;
        this.ssTables = new LinkedList<>();
        this.memtable = new ConcurrentSkipListMemtable();
        this.immMemtable = new LinkedList<>();
        fillSstableList();
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

        memtable.put(key, value);
        if (!memtable.canFlush()) return;

        // if memtable exceeds a certain threshold
        //  start its compaction,
        //  replace the memtable with new memtable, this should be synchronized
        //  reads should also go to old memtable till the flushing process complete,
        //      make another immemtable variable and assign current memtable to it
        //      but what if during memtable flush, another memtable threshodld reached, but previous memtable flushing is not complete yet
        //          in that case we will use a linekdlist and befoe flushing the memtable it will added at the begginig of linkedlist
        //  once flushing complete the read should be now rediected to sstable
        // then make the memtable immutable by moving it to LinkedList by adding it first in the list
        // start its compaction

        // a memtable starts its flush and add itself to at the first pos of immlinkedlist
        // we will start flushing if threads are available and mark the status of memtable to flushing
        // another memtable threshold is reached, threads are available so it starts it flushing,
        // the first memtable has finished its flushing then i will start searching from the end of ll to pop the memtable
        // but what if the newer memtable has finished the flushing process beofre older memtable, this can give inconsistent result
        //      from db
        //      in that case on any start and end of flushing process, we will check the end of the memll if there is any mem that gahs finished its
        //          flushing so wew ill remove it

        immMemtable.addFirst(memtable);
        memtable = new ConcurrentSkipListMemtable();

        ExecutorService executorService = Executors.newFixedThreadPool();

        // LinkedBlockingQueue
        // ConcurrentLinkedQueue
        try {
            flushMemtable();
            memtable = new ConcurrentSkipListMemtable();
        } catch (Exception e) {
            log.error("error flushing the memtable",e);
            throw e;
        }

    }

    @Override
    public String get(String key) {

        String value = memtable.get(key);
        if (value == null) return getFromSstable(key);
        else if (value.isEmpty()) return null;
        else return value;

    }

    @Override
    public void delete(String key) throws IOException { memtable.put(key, ""); }

    public void flushMemtable() {
        try {
            String filePath = DATA_DIR + UUID.randomUUID() + FILE_POSTFIX;

            SSTableImpl ssTable = new SSTableImpl(filePath);
            SSTableImpl.SSTableWriter ssTableWriter = ssTable.getWriter();
            Iterator<KeyValuePair> memtableIterator = memtable.iterator();

            try (ssTableWriter) {
                while (memtableIterator.hasNext()) {
                    ssTableWriter.write(memtableIterator.next());
                }
            }

            ssTables.addFirst(ssTable);
            compaction();
            writeAheadLog.clear();
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

    public void compaction() throws Exception {
        // conditon to start compaction
        if (ssTables.size() < 2) return;

        // get sstable to compact and there readers / iterator
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