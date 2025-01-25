package graveldb.datastore.lsmtree.sstable;

import graveldb.datastore.lsmtree.KeyValuePair;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface SSTable extends Iterable<KeyValuePair> {
    public String getFileName();
    public SSTableImpl.SSTableWriter getWriter() throws FileNotFoundException;
}
