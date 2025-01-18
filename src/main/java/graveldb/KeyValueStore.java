package graveldb;

public interface KeyValueStore {
    String put(String key, String value);      // Insert or update a value for the given key
    String get(String key);                 // Retrieve the value for the given key
    String delete(String key);                // Delete the key-value pair
    boolean containsKey(String key);        // Check if a key exists
    int size();
}
