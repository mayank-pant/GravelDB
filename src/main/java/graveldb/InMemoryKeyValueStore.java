package graveldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKeyValueStore implements KeyValueStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryKeyValueStore.class);
    private final ConcurrentHashMap<String, String> store;

    public InMemoryKeyValueStore() {
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public String put(String key, String value) {
        store.put(key, value);
        return "OK";
    }

    @Override
    public String get(String key) {
        return store.getOrDefault(key, null);
    }

    @Override
    public String delete(String key) {
        return store.remove(key) != null ? "1" : "0";
    }

    @Override
    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    @Override
    public int size() {
        return store.size();
    }

    public String getAll() {
        return store.toString();
    }

    // Reactive API
//    public Mono<String> reactiveGet(String key) {
//        return Mono.fromCallable(() -> get(key));
//    }
//
//    public Mono<String> reactiveSet(String key, String value) {
//        return Mono.fromCallable(() -> set(key, value));
//    }
//
//    public Mono<String> reactiveDel(String key) {
//        return Mono.fromCallable(() -> del(key));
//    }
//
//    public Mono<Integer> reactiveDbsize() {
//        return Mono.fromCallable(this::dbsize);
//    }
}
