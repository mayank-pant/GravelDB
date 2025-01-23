package graveldb.datastore.lsmtree.sstable;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface SSTable {

    String get(String key);
    String getAll();
    int size();
    void addSSTable(String filePath);
    void compaction() throws IOException;
}
