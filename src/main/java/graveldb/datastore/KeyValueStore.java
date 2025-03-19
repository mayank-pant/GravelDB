package graveldb.datastore;

import java.io.IOException;

public interface KeyValueStore {

    void put(String key, String value);
    String get(String key);
    void delete(String key);

}
