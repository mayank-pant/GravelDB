package graveldb.datastore.LSMTree.SSTable;

public interface SSTable {

    String get(String key);
    String getAll();
    int size();
    void addSSTable(String filePath);

}
