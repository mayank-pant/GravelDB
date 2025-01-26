package datastore;

import graveldb.datastore.lsmtree.LSMTree;
import graveldb.wal.WriteAheadLog;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LSMTreeAddBenchmark {
    LSMTree tree;
    String[] keys;
    static int index = 0;
    static final int NUM_ITEMS = 1000000;

    @Setup
    public void setup() throws IOException {
        WriteAheadLog wal = new WriteAheadLog("./wal.txt");
        tree = new LSMTree(wal);
        keys = genItems();
    }

    private String[] genItems() {
        keys = new String[NUM_ITEMS];
        for (int i=0; i<NUM_ITEMS; i++) {
            keys[i] = String.valueOf(i);
        }
        return keys;
    }

    @TearDown
    public void teardown() {}

    @Benchmark
    public void add() throws IOException {
        tree.put(keys[index], keys[index]);

        index = (index + 1) % NUM_ITEMS;
    }
}
