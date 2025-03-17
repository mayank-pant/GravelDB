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
    private BitSet bitArray;

    private final String fileName;

    public BloomFilterImpl(String fileName) throws IOException {
        this.fileName = fileName;
        this.bitArray = new BitSet(BLOOM_BUCKET);
        Path path = Path.of(fileName);
        if (!Files.exists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        } else generateBitArray();
    }

    public BloomFilterWriter getWriter() throws FileNotFoundException { return new BloomFilterImpl.BloomFilterWriter(); }

    public boolean check(String key) throws IOException {
        for (int i=0; i<HASH_FUNCTIONS; i++) {
            int setBit = Math.abs(getKeyHashValue(key, i) % BLOOM_BUCKET);
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
            if (res >= 0) bitArray = BitSet.valueOf(byteArray);
            else bitArray = new BitSet(BLOOM_BUCKET);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public class BloomFilterWriter implements AutoCloseable {

        BufferedOutputStream bos;

        public BloomFilterWriter() throws FileNotFoundException {
            bos = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        public void write(String key) {
            for (int i=0; i<HASH_FUNCTIONS; i++) {
                int setBit = Math.abs(getKeyHashValue(key, i) % BLOOM_BUCKET);
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
