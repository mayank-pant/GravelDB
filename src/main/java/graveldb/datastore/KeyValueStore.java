package graveldb.datastore;

import java.io.IOException;

public interface KeyValueStore {

    void put(String key, String value) throws IOException;
    String get(String key);
    void delete(String key) throws IOException;

}
