package graveldb.DataStore;

import java.io.IOException;

public interface KeyValueStore {
    String put(String key, String value) throws IOException;      // Insert or update a value for the given key
    String get(String key);                 // Retrieve the value for the given key
    String delete(String key);                // Delete the key-value pair
    boolean containsKey(String key);        // Check if a key exists
    String getAll();
    int size();
}
