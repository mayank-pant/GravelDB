package datastore;

import graveldb.datastore.lsmtree.LSMTree;
import graveldb.wal.WalRecovery;
import graveldb.wal.WriteAheadLog;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class LSMTreeAddBenchmark {

    private static final Logger log = LoggerFactory.getLogger(LSMTreeAddBenchmark.class);

    LSMTree tree;
    String[] keys;
    static int index = 0;
    static final int NUM_ITEMS = 1000000;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        log.info("setting up LSM tree for each benchmark iteration");
        index = 0;
        tree = new LSMTree();

        keys = genItems();
        shuffleKeys();

    }

    private String[] genItems() {
        String[] generatedKeys = new String[NUM_ITEMS];
        for (int i=0; i<NUM_ITEMS; i++) {
            generatedKeys[i] = String.valueOf(i);
        }
        return generatedKeys;
    }

    @TearDown(Level.Iteration)
    public void teardown() {
        log.info("started teardown of the LSM tree");

        try {
            tree.stop();
            Thread.sleep(5000);
        } catch (Exception ignored) {
        }

        boolean result1 = deleteDirectory(new File("./waldata"));
        boolean result2 = deleteDirectory(new File("./dbdata"));

        if (result1 && result2) log.info("teardown complete for the files for this iteration");
        else throw new RuntimeException("not able to delete fies from folder");
    }

    boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private void shuffleKeys() {
        List<String> shuffled = Arrays.asList(keys.clone());
        Collections.shuffle(shuffled);
        keys = shuffled.toArray(new String[0]);
    }

    @Benchmark
    public void add() throws IOException {
        tree.put(keys[index], keys[index]);
        index = (index + 1) % NUM_ITEMS;
    }
}
