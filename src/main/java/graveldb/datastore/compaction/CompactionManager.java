package graveldb.datastore.compaction;

import graveldb.datastore.sstable.SSTableImpl;

import java.util.Iterator;
import java.util.LinkedList;

public class CompactionManager {

    private static final int TIER_COUNT = 4;
    private static final int TIER_SIZE = 4000;
    private static final int TIER_MULTIPLE = 4;
    LinkedList<LinkedList<SSTableImpl>> tieredSSTables;

    public CompactionManager() {
        tieredSSTables = new LinkedList<>();
    }

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
        // select the key which is smallest
        // add the key and value to the new file
        //
    }

    private boolean checkCompaction(LinkedList<SSTableImpl> tier, int level) {
        int size = 0;
        for (SSTableImpl ssTable : tier) {
            size += ssTable.getSize();
        }
        return size > TIER_SIZE * Math.pow(TIER_MULTIPLE, level);
    }
}
