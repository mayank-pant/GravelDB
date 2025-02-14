package graveldb.datastore.lsmtree;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class LSMTreeTest {

    LSMTree lsmTree = new LSMTree();

    LSMTreeTest() throws IOException {
    }

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void dbCorrectnessTest() throws IOException {
        // load keys
        String[] keys = new String[100000];
        for (int i=0; i<keys.length; i++) { keys[i] = String.valueOf(i); }

        // create executor service
        ExecutorService executorServiceOne = Executors.newFixedThreadPool(2);
        ExecutorService executorServiceTwo = Executors.newFixedThreadPool(2);

        // put the values inside the db
        for (String key : keys) { executorServiceOne.submit(() -> assertDoesNotThrow(() -> lsmTree.put(key,key))); }
        executorServiceOne.shutdown();
        try {
            if (!executorServiceOne.awaitTermination(1000, TimeUnit.SECONDS)) {
                executorServiceOne.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorServiceOne.shutdownNow();
        }

        // get the values and assertEquals
        for (String key : keys) {
            executorServiceTwo.submit(() -> assertEquals(key, assertDoesNotThrow(() -> lsmTree.get(key))));
        }
        executorServiceTwo.shutdown();
        try {
            if (!executorServiceTwo.awaitTermination(1000, TimeUnit.SECONDS)) {
                executorServiceTwo.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorServiceTwo.shutdownNow();
        }
    }

    @Test
    void put() {
    }

    @Test
    void get() {
    }
}