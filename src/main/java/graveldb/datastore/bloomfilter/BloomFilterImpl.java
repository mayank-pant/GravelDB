package graveldb.datastore.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

import com.google.common.hash.Hashing;

public class BloomFilterImpl {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterImpl.class);

    private static final int HASH_FUNCTIONS = 7;
    private static final int BLOOM_BUCKET = 10000;
    private BitSet bitArray = null;

    private final String fileName;


    public BloomFilterImpl(String fileName) throws IOException {
        this.fileName = fileName;
        if (!Files.exists(Path.of(fileName))) {
            Files.createDirectories(Path.of(fileName).getParent());
            Files.createFile(Path.of(fileName));
        }
    }

    public BloomFilterWriter getWriter() throws FileNotFoundException { return new BloomFilterImpl.BloomFilterWriter(); }

    public boolean check(String key) throws IOException {
        for (int i=0; i<HASH_FUNCTIONS; i++) {
            int setBit = getKeyHashValue(key, i) % BLOOM_BUCKET;
            log.info("get mode - key {}, index {}", key, setBit);
            if (setBit < 0) setBit += setBit*-2;  // Ensure it's non-negative
            if (!checkSetBit(setBit)) return false;
        }
        return true;
    }

    private boolean checkSetBit(int setBit) throws IOException {
        if (bitArray == null) generateBitArray();
        return bitArray.get(setBit);
    }

    private void generateBitArray() throws IOException {
        byte[] byteArray = new byte[(BLOOM_BUCKET + 7) / 8];
        RandomAccessFile fis = new RandomAccessFile(fileName, "r");
        try (fis) {
            int res = fis.read(byteArray);
            if (res != -1) bitArray = BitSet.valueOf(byteArray);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public class BloomFilterWriter implements AutoCloseable {

        BufferedOutputStream bos;
        BitSet bitArray = new BitSet(BLOOM_BUCKET);

        public BloomFilterWriter() throws FileNotFoundException {
            bos = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        public void write(String key) {
            for (int i=0; i<HASH_FUNCTIONS; i++) {
                int setBit = getKeyHashValue(key, i) % BLOOM_BUCKET;
                log.info("put mode - key {}, index {}", key, setBit);
                if (setBit < 0) setBit += setBit*-2;  // Ensure it's non-negative
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

    public int getKeyHashValue(String key, int salt) {
        return Hashing.murmur3_128().hashBytes(key.getBytes()).asInt() + salt * Hashing.sipHash24().hashBytes(key.getBytes()).asInt();
    }

}
