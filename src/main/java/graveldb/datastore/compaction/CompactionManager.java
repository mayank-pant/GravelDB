package graveldb.datastore.compaction;

import graveldb.datastore.bloomfilter.BloomFilterImpl;
import graveldb.datastore.lsmtree.KeyValuePair;
import graveldb.datastore.sparseindex.SparseIndexImpl;
import graveldb.datastore.sstable.SSTableImpl;
import graveldb.util.Pair;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionManager {

    private static final int TIER_COUNT = 4;
    private static final int TIER_SIZE = 4000;
    private static final int TIER_MULTIPLE = 4;
    LinkedList<LinkedList<SSTableImpl>> tieredSSTables;

    public CompactionManager() {
        tieredSSTables = new LinkedList<>();
        sstableCount = new AtomicInteger(0);
    }

    AtomicInteger sstableCount;

    public void addSSTable(SSTableImpl ssTable) {
        if (tieredSSTables.isEmpty()) {
            tieredSSTables.addFirst(new LinkedList<>());
            tieredSSTables.getFirst().add(ssTable);
            }
    }

    public Runnable checkAndPerformCompaction() {
        // check from last if threshold size is reached
        // start there compaction
        // repeat for other sstables
        int level = 0;
        for (Iterator<LinkedList<SSTableImpl>> it = tieredSSTables.descendingIterator(); it.hasNext(); ) {
            LinkedList<SSTableImpl> tier = it.next();
            if (checkCompaction(tier, level)) {
                startCompaction(tier);
            }
        }
    }

    private void startCompaction(LinkedList<SSTableImpl> tier) {
        // open iterator for the files
        // compare the current key of files
        //      loop over all the cur
        // select the key which is smallest
        // add the key and value to the new filer

        // get nex
        try {
            if (ssTables.size() < 2) return;

            // i will have multiple sstables linkedlist each of them having a threshold assigned to them
            // at each compaction trigger, i will check from the largest threshold linkedlist if the threshold is reached then start compacting
            // i will continue compacting

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

            LinkedList<SSTableImpl.SSTableIterator> ssTablesItr = new LinkedList<>();
            for (SSTableImpl ssTable : tier) ssTablesItr.add(ssTable.iterator());

            while (true) {
                KeyValuePair kvp = getNextSmallest(ssTablesItr);
                if (!kvp.isDeleted()) {
                    if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp1.key(), offset);
                    ssTableWriter.write(kvp1);
                    bloomFilterWriter.write(kvp1.key());
                    curKeyCount++;
                    offset += 8;
                    offset += kvp1.key().getBytes().length;
                    offset += kvp1.value().getBytes().length;
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

            LinkedList<SSTableImpl.SSTableIterator> ssTablesItr = new LinkedList<>();
            for (SSTableImpl ssTable : tier) ssTablesItr.add(ssTable.iterator());

            while (true) {
                KeyValuePair kvp = getNextSmallest(ssTablesItr);
                if (!kvp.isDeleted()) {
                    if (curKeyCount % KEY_COUNT_FOR_SPARSE_INDEX == 0 || curKeyCount == 0) sparseIndexWriter.write(kvp1.key(), offset);
                    ssTableWriter.write(kvp);
                    bloomFilterWriter.write(kvp.key());
                    curKeyCount++;
                    offset += 8;
                    offset += kvp.key().getBytes().length;
                    offset += kvp.value().getBytes().length;
                }
            }

            try (fis1; fis2; ssTableWriter; bloomFilterWriter; sparseIndexWriter) {
                KeyValuePair kvp1 = null;
                KeyValuePair kvp2 = null;

                // both sstable has something to read
                if (getSmallest)
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

    LinkedList<KeyValuePair> valueLinkedList = new LinkedList<>();

    private KeyValuePair getNextSmallest(LinkedList<SSTableImpl.SSTableIterator> ssTablesItr) {
        // loop over the cur value of linkedlist
        // if value is empty then call the next value from sstableItr and compare it with current smallest value
        // return the smallest value and the end
        // if the next value is null then move to next linkedlist
        // if the value we are coparing is equal then ignore the current linkedlist value

        int curSize = valueLinkedList.size();
        KeyValuePair curSmallest = null;
        SSTableImpl.SSTableIterator curSmallestItr = null;
        LinkedList<KeyValuePair> curItrVals = new LinkedList<>();
        for (int i=0; i<ssTablesItr.size(); i++) curItrVals.addLast(null);
        for (int i=0; i<ssTablesItr.size(); i++) {
            if (curItrVals.get(i) == null) {
                if (ssTablesItr.get(i).hasNext()) ssTablesItr.get(i).next();
            }

            if (curItrVals.get(i) != null) {
                if (curItrVals.get(i) < curSmallest) {
                    curSmallest = curItrVals.get(i);
                }
            }
        }

        // write the value to new file
    }

    private boolean checkCompaction(LinkedList<SSTableImpl> tier, int level) {
        int size = 0;
        for (SSTableImpl ssTable : tier) {
            size += ssTable.getSize();
        }
        return size > TIER_SIZE * Math.pow(TIER_MULTIPLE, level);
    }
}
