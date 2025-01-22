package graveldb.datastore.lsmtree.sstable;

public interface SSTable {

    String get(String key);
    String getAll();
    int size();
    void addSSTable(String filePath);

}
