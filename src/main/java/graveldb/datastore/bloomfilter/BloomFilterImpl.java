package graveldb.datastore.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

import com.google.common.hash.Hashing;

public class BloomFilterImpl {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterImpl.class);

    private static final int HASH_FUNCTIONS = 7;
    private static final int BLOOM_BUCKET = 10000;

    private final String fileName;


    public BloomFilterImpl(String fileName) throws IOException {
        this.fileName = fileName;
        if (!Files.exists(Path.of(fileName))) {
            Files.createDirectories(Path.of(fileName).getParent());
            Files.createFile(Path.of(fileName));
        }
    }

    public BloomFilterWriter getWriter() throws FileNotFoundException { return new BloomFilterImpl.BloomFilterWriter(); }

    public boolean check(String key) {
        // if bitset array is empty then fetch the
        return true;
    }

    public class BloomFilterWriter implements AutoCloseable {

        BufferedOutputStream bos;
        BitSet bitArray = new BitSet(BLOOM_BUCKET);

        public BloomFilterWriter() throws FileNotFoundException {
            bos = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        public void write(String key) {
            for (int i=0; i<HASH_FUNCTIONS; i++) {
                int setBit = (Hashing.murmur3_128().hashBytes(key.getBytes()).asInt() + i * Hashing.sipHash24().hashBytes(key.getBytes()).asInt()) % (BLOOM_BUCKET-1);
                if (setBit < 0) {
                    setBit += setBit*-2;  // Ensure it's non-negative
                }
                bitArray.set(setBit);
            }
        }

        @Override
        public void close() throws Exception {
            bos.write(bitArray.toByteArray());
            bos.flush();
            bos.close();
        }
    }

}
