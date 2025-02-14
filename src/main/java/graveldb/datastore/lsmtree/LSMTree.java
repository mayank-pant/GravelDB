package graveldb.datastore.lsmtree;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.memtable.ConcurrentSkipListMemtable;
import graveldb.datastore.memtable.Memtable;
import graveldb.datastore.memtable.MemtableStatus;
import graveldb.datastore.sstable.SSTable;
import graveldb.datastore.sstable.SSTableImpl;
import graveldb.datastore.bloomfilter.BloomFilterImpl;
import graveldb.datastore.sparseindex.SparseIndexImpl;
import graveldb.util.Pair;
import graveldb.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class LSMTree implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(LSMTree.class);

    private static final String DATA_DIR = "./dbdata/";
    private static final String SSTABLE_DIR = "/sstable_{file_no}/";
    private static final String SSTABLE_FILE_POSTFIX = "_ssfile.data";
    private static final String SPARSE_INDEX_FILE_POSTFIX = "_index.data";
    private static final String BLOOM_FILTER_FILE_POSTFIX = "_filter.data";
    private static final int KEY_COUNT_FOR_SPARSE_INDEX = 500;

    AtomicInteger sstableCount;

    private Memtable mutMemtable;
    private final LinkedList<Memtable> immMemtables;
    private final LinkedList<SSTableImpl> ssTables;
    private WriteAheadLog mutWal;
    private final ConcurrentHashMap<Memtable, WriteAheadLog> memtableToWalfile;
    private final ConcurrentHashMap<SSTable, Pair<BloomFilterImpl, SparseIndexImpl>> ssTableToBloomAndSparse;

    private static final Object memTableObject = new Object();
    private static final Object mutWalObject = new Object();

    ScheduledExecutorService memtableFlusher;
    ScheduledExecutorService tableCompactor;

    public LSMTree() throws IOException {
        this.mutMemtable = new ConcurrentSkipListMemtable();
        this.immMemtables = new LinkedList<>();
        this.ssTables = new LinkedList<>();
        this.mutWal = new WriteAheadLog();
        this.memtableToWalfile = new ConcurrentHashMap<>();
        this.ssTableToBloomAndSparse = new ConcurrentHashMap<>();

        memtableToWalfile.put(mutMemtable, mutWal);

        fillSstableList();

        memtableFlusher = newSingleThreadScheduledExecutor();
        memtableFlusher.scheduleAtFixedRate(this::flushMemtable, 50, 50, TimeUnit.MILLISECONDS);

        tableCompactor = newSingleThreadScheduledExecutor();
        tableCompactor.scheduleAtFixedRate(this::compaction, 200, 200, TimeUnit.MILLISECONDS);
    }

    private void fillSstableList() throws IOException {

        Files.createDirectories(Paths.get(DATA_DIR));

        File directory = new File(DATA_DIR);
        File[] ssTableDirs = directory.listFiles();
        sstableCount = new AtomicInteger(0);
        if (ssTableDirs == null) return;
        if (ssTableDirs.length == 0) return;

        log.info("{} sstables found in the directory", ssTableDirs.length);

        Arrays.sort(ssTableDirs, Comparator.comparingLong(File::lastModified));

        log.info(ssTableDirs[0].getName());
        log.info(ssTableDirs[0].getPath());
        log.info(ssTableDirs[0].getAbsolutePath());
        log.info(ssTableDirs[0].getParent());
        log.info(ssTableDirs[0].getCanonicalPath());

        String[] toGetMaxSsTableCount = ssTableDirs[0].getName().split("_");
        sstableCount = new AtomicInteger(Integer.parseInt(toGetMaxSsTableCount[toGetMaxSsTableCount.length-1]));

        for (File ssTableDir : ssTableDirs){
            File[] ssTableFiles = ssTableDir.listFiles();
            if (ssTableFiles == null) continue;
            // TODO: if sstable not found then throw DBInvalidState Runtime custom exception
            // TODO: if bloom and sparse file not found then attempt to recreate or else throw DBInvalidState Runtime exception
            String ssTableFileName = Arrays.stream(ssTableFiles)
                    .filter(file -> file.getName().contains(SSTABLE_FILE_POSTFIX)).findAny()
                    .orElseThrow(() -> new RuntimeException("sstable not found")).getAbsolutePath();
            String bloomFilterFileName = Arrays.stream(ssTableFiles)
                    .filter(file -> file.getName().contains(BLOOM_FILTER_FILE_POSTFIX)).findAny()
                    .orElseThrow(() -> new RuntimeException("bloom filter not found")).getAbsolutePath();
            String sparseIndexFileName = Arrays.stream(ssTableFiles)
                    .filter(file -> file.getName().contains(SPARSE_INDEX_FILE_POSTFIX)).findAny()
                    .orElseThrow(() -> new RuntimeException("sparse index not found")).getAbsolutePath();
            Pair<BloomFilterImpl, SparseIndexImpl> bloomAndSparse = new Pair<>(
                    new BloomFilterImpl(bloomFilterFileName),
                    new SparseIndexImpl(sparseIndexFileName)
            );
            SSTableImpl ssTable = new SSTableImpl(ssTableFileName);
            ssTables.addFirst(ssTable);
            ssTableToBloomAndSparse.put(ssTable,bloomAndSparse);
        }
    }

    @Override
    public void put(String key, String value) throws IOException {
        synchronized (mutWalObject) {
            mutWal.append("SET", key, value);
        }

        synchronized (memTableObject) {
            mutMemtable.put(key, value);
        }

        if (mutMemtable.canFlush()) {
            synchronized (immMemtables) {
                immMemtables.addFirst(mutMemtable);
            }

            synchronized (memTableObject) {
                mutMemtable.updateMemtableStatus(MemtableStatus.TO_BE_FLUSHED);
                mutMemtable = new ConcurrentSkipListMemtable();
                synchronized (mutWalObject) {
                    mutWal = new WriteAheadLog();
                }
                memtableToWalfile.put(mutMemtable, mutWal);
            }
        }
    }

    @Override
    public String get(String key) throws IOException {
        String value = null;
        synchronized (memTableObject) {
            value = mutMemtable.get(key);
        }
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
            Memtable toBeFlushedMemtable = immMemtables.peekLast();

            if (toBeFlushedMemtable == null) return;

            String fileIdentifier = String.valueOf(sstableCount.incrementAndGet());
            String newSsTableDir = DATA_DIR + SSTABLE_DIR.replace("{file_no}", fileIdentifier);
            String ssTableFilePath = newSsTableDir + fileIdentifier + SSTABLE_FILE_POSTFIX;
            String sparseIndexFilePath = newSsTableDir + fileIdentifier + SPARSE_INDEX_FILE_POSTFIX;
            String bloomFilterFilePath = newSsTableDir + fileIdentifier + BLOOM_FILTER_FILE_POSTFIX;

            Iterator<KeyValuePair> memtableIterator = toBeFlushedMemtable.iterator();

            SSTableImpl ssTable = new SSTableImpl(ssTableFilePath);
            SSTableImpl.SSTableWriter ssTableWriter = ssTable.getWriter();

            SparseIndexImpl sparseIndex = new SparseIndexImpl(sparseIndexFilePath);
            SparseIndexImpl.SparseIndexWriter sparseIndexWriter = sparseIndex.getWriter();

            BloomFilterImpl bloomFilter = new BloomFilterImpl(bloomFilterFilePath);
            BloomFilterImpl.BloomFilterWriter bloomFilterWriter = bloomFilter.getWriter();

            int curKeyCount = 0;
            int offset = 0;
            try (ssTableWriter; sparseIndexWriter; bloomFilterWriter) {
                while (memtableIterator.hasNext()) {
                    KeyValuePair kvp = memtableIterator.next();
                    if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp.key(), offset);
                    ssTableWriter.write(kvp);
                    bloomFilterWriter.write(kvp.key());
                    curKeyCount++;
                    offset += 8;
                    offset += kvp.key().getBytes().length;
                    offset += kvp.value().getBytes().length;
                }
            }

            synchronized (immMemtables) {
                immMemtables.pollLast();
            }

            ssTableToBloomAndSparse.put(ssTable, new Pair<>(bloomFilter, sparseIndex));

            synchronized (ssTables) {
                ssTables.add(ssTable);
            }

            memtableToWalfile.get(toBeFlushedMemtable).delete();

        } catch (Exception e) {
            log.error("error while flushing memtable to sstable", e);
        }
    }

    public String getFromSstable(String targetKey) throws IOException {
        synchronized (ssTables) {
            for (SSTableImpl sstable: ssTables) {
                Pair<BloomFilterImpl, SparseIndexImpl> pair = ssTableToBloomAndSparse.get(sstable);
                if (!pair.ele1().check(targetKey)) continue;

                List<Pair<String, Integer>> sparseTable = null;
                try {
                    sparseTable = pair.ele2().getSparseIndexTable();
                } catch (IOException e) {
                    log.error("error reading sstable");
                    throw new RuntimeException("erro reading sstablee");
                }

                log.info(sparseTable.toString());


                int offset = -1;
                int l = 0, r = sparseTable.size()-1;

                while (l <= r) {
                    int m = l + (r - l) / 2;
                    Pair<String, Integer> kao = sparseTable.get(m);
                    int cmp = kao.ele1().compareTo(targetKey);

                    if (cmp == 0) {
                        offset = kao.ele2(); // Exact match
                        break;
                    } else if (cmp < 0) {
                        offset = kao.ele2(); // Update closest smaller key
                        l = m + 1;
                    } else {
                        r = m - 1;
                    }
                }

                if (offset == -1) continue;

                try (SSTableImpl.SSTableIterator itr = sstable.iterator(offset)) {
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
    }

    public void compaction() {
        try {
            if (ssTables.size() < 2) return;

            // get sstable to compact and there readers / iterator
            SSTableImpl ssTable2 = null;
            SSTableImpl ssTable1 = null;
            int count = 0;
            synchronized (ssTables) {
                for (Iterator<SSTableImpl> it = ssTables.descendingIterator(); it.hasNext(); ) {
                    SSTableImpl curSstable = it.next();
                    if (count == 0) ssTable2 = curSstable;
                    else if (count == 1) {
                        ssTable1 = curSstable;
                        break;
                    }
                    count++;
                }
            }

            assert ssTable2 != null;
            assert ssTable1 != null;
            SSTableImpl.SSTableIterator fis2 = ssTable2.iterator();
            SSTableImpl.SSTableIterator fis1 = ssTable1.iterator();

            String fileIdentifier = String.valueOf(sstableCount.incrementAndGet());
            String newSsTableDir = DATA_DIR + SSTABLE_DIR.replace("{file_no}", fileIdentifier);
            String ssTableFilePath = newSsTableDir + fileIdentifier + SSTABLE_FILE_POSTFIX;
            String sparseIndexFilePath = newSsTableDir + fileIdentifier + SPARSE_INDEX_FILE_POSTFIX;
            String bloomFilterFilePath = newSsTableDir + fileIdentifier + BLOOM_FILTER_FILE_POSTFIX;

            // create new sstable, bloom filter, sparse index
            SSTableImpl ssTableNew = new SSTableImpl(ssTableFilePath);
            BloomFilterImpl bloomFilterNew = new BloomFilterImpl(bloomFilterFilePath);
            SparseIndexImpl sparseIndexNew = new SparseIndexImpl(sparseIndexFilePath);

            // get sstable bloom filter, sparse index writer
            SSTableImpl.SSTableWriter ssTableWriter = ssTableNew.getWriter();
            BloomFilterImpl.BloomFilterWriter bloomFilterWriter = bloomFilterNew.getWriter();
            SparseIndexImpl.SparseIndexWriter sparseIndexWriter = sparseIndexNew.getWriter();

            int curKeyCount = 0;
            int offset = 0;
            try (fis1; fis2; ssTableWriter; bloomFilterWriter; sparseIndexWriter) {
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
                            if (!kvp1.isDeleted()) {
                                if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp1.key(), offset);
                                ssTableWriter.write(kvp1);
                                bloomFilterWriter.write(kvp1.key());
                                curKeyCount++;
                                offset += 8;
                                offset += kvp1.key().getBytes().length;
                                offset += kvp1.value().getBytes().length;
                            }
                            kvp1 = null;
                            if (fis1.hasNext()) kvp1 = fis1.next();
                            else break;
                            // if key2 is lexico smaller then add it
                        } else if (cmp > 0) {
                            if (!kvp2.isDeleted()) {
                                if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp2.key(), offset);
                                ssTableWriter.write(kvp2);
                                bloomFilterWriter.write(kvp2.key());
                                curKeyCount++;
                                offset += 8;
                                offset += kvp2.key().getBytes().length;
                                offset += kvp2.value().getBytes().length;
                            }
                            kvp2 = null;
                            if (fis2.hasNext()) kvp2 = fis2.next();
                            else break;
                            // if both key are equal add key1 is not deleted then add key1 to sstable
                        } else {
                            if (!kvp1.isDeleted()) {
                                if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp1.key(), offset);
                                ssTableWriter.write(kvp1);
                                bloomFilterWriter.write(kvp1.key());
                                curKeyCount++;
                                offset += 8;
                                offset += kvp1.key().getBytes().length;
                                offset += kvp1.value().getBytes().length;
                            }
                            kvp1 = null;
                            kvp2 = null;
                            if (fis1.hasNext()) kvp1 = fis1.next();
                            else break;
                            if (fis2.hasNext()) kvp2 = fis2.next();
                            else break;
                        }
                    }
                }

                while (kvp2 != null) {
                    if (!kvp2.isDeleted()) {
                        if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp2.key(), offset);
                        ssTableWriter.write(kvp2);
                        bloomFilterWriter.write(kvp2.key());
                        curKeyCount++;
                        offset += 8;
                        offset += kvp2.key().getBytes().length;
                        offset += kvp2.value().getBytes().length;
                    }
                    if (fis2.hasNext()) kvp2 = fis2.next();
                    else break;
                }

                while (kvp1 != null) {
                    if (!kvp1.isDeleted()) {
                        if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0) sparseIndexWriter.write(kvp1.key(), offset);
                        ssTableWriter.write(kvp1);
                        bloomFilterWriter.write(kvp1.key());
                        curKeyCount++;
                        offset += 8;
                        offset += kvp1.key().getBytes().length;
                        offset += kvp1.value().getBytes().length;
                    }
                    if (fis1.hasNext()) kvp1 = fis1.next();
                    else break;
                }
            }

            synchronized (ssTables) {
                ssTables.pollLast();
                ssTables.pollLast();
                ssTables.addLast(ssTableNew);
                ssTableToBloomAndSparse.put(ssTableNew, new Pair<>(bloomFilterNew, sparseIndexNew));
            }

            ssTableToBloomAndSparse.remove(ssTable1);
            ssTableToBloomAndSparse.remove(ssTable2);

            boolean filesDeleted = true;
            deleteSsTableFiles(ssTable1.getFileName());
            deleteSsTableFiles(ssTable2.getFileName());
            log.info("stables after compaction deleted status {}", filesDeleted);

        } catch (Exception e) {
            log.error("error while compaction",e);
        }
    }

    private boolean deleteSsTableFiles(String fileName) {
        boolean fileDeleted = true;
        File directory = new File(Path.of(fileName).getParent().toString());
        File[] files = directory.listFiles();
        if (files == null) return true;
        for (File file : files) {
            fileDeleted &= file.delete();
        }
        fileDeleted &= directory.delete();
        return fileDeleted;
    }

    public void stop() {
        memtableFlusher.shutdownNow();
        tableCompactor.shutdownNow();
    }
}