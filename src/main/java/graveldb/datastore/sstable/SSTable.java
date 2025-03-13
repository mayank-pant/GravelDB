package graveldb.datastore.sstable;

import graveldb.datastore.lsmtree.KeyValuePair;

import java.io.FileNotFoundException;

public interface SSTable extends Iterable<KeyValuePair> {
    public String getFileName();
    public SSTableImpl.SSTableWriter getWriter() throws FileNotFoundException;
    public long getSize();
}
