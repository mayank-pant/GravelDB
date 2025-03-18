package graveldb.util;

import graveldb.datastore.lsmtree.KeyValuePair;
import graveldb.datastore.lsmtree.LSMTree;
import graveldb.datastore.sstable.SSTableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class TreeUtil {

    private static final Logger log = LoggerFactory.getLogger(TreeUtil.class);


    public boolean verifyCompaction(LinkedList<SSTableImpl> sourceTier, SSTableImpl newTable) {
        Set<String> sourceKeys = new HashSet<>();

        // Collect all non-deleted keys from source tables
        for (SSTableImpl table : sourceTier) {
            try (SSTableImpl.SSTableIterator itr = table.iterator()) {
                while (itr.hasNext()) {
                    KeyValuePair kvp = itr.next();
                    if (!kvp.isDeleted()) sourceKeys.add(kvp.key());
                }
            } catch (Exception e) {
                log.error("Error reading source table during verification", e);
                return false;
            }
        }

        // Check if all keys exist in new table
        Set<String> newKeys = new HashSet<>();
        try (SSTableImpl.SSTableIterator itr = newTable.iterator()) {
            while (itr.hasNext()) {
                KeyValuePair kvp = itr.next();
                if (!kvp.isDeleted()) newKeys.add(kvp.key());
            }
        } catch (Exception e) {
            log.error("Error reading new table during verification", e);
            return false;
        }

        // Check for missing keys
        Set<String> missingKeys = new HashSet<>(sourceKeys);
        missingKeys.removeAll(newKeys);

        if (!missingKeys.isEmpty()) {
            log.error("Keys lost during compaction: {}", missingKeys);
            return false;
        }

        return true;
    }
}
