package graveldb.datastore.sparseindex;

import graveldb.datastore.lsmtree.KeyValuePair;
import graveldb.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class SparseIndex {

    private static final Logger log = LoggerFactory.getLogger(SparseIndex.class);

    private final String fileName;
    List<Pair<String, Integer>> sparseTable = null;


    public SparseIndex(String fileName) {
        this.fileName = fileName;
        Path path = Path.of(fileName);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path.getParent());
                Files.createFile(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public SparseIndexWriter getWriter() { return new SparseIndexWriter(); }

    public List<Pair<String, Integer>> getSparseIndexTable() {
        if (sparseTable != null) return sparseTable;

        this.sparseTable = new ArrayList<>();

        try (RandomAccessFile fis = new RandomAccessFile(fileName, "r");) {
            while (fis.getFilePointer() < fis.length()) {
                byte[] keyLenByte = new byte[4];
                fis.read(keyLenByte);
                byte[] keyByte = new byte[byteArrayToInt(keyLenByte)];
                fis.read(keyByte);
                byte[] offsetByte = new byte[4];
                fis.read(offsetByte);

                sparseTable.add(new Pair<>(new String(keyByte), byteArrayToInt(offsetByte)));
            }
            return sparseTable;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class SparseIndexWriter implements AutoCloseable {

        BufferedOutputStream bos;

        public SparseIndexWriter() {
            try {
                bos = new BufferedOutputStream(new FileOutputStream(fileName));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void write(String key, int offset) {
            try {
                bos.write(intToByteArray(key.getBytes().length));
                bos.write(key.getBytes());
                bos.write(intToByteArray(offset));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws Exception {
            bos.flush();
            bos.close();
        }
    }

    public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte) (value >>> 24), // Extracts the most significant byte
                (byte) (value >>> 16), // Extracts the second most significant byte
                (byte) (value >>> 8),  // Extracts the third most significant byte
                (byte) value           // Extracts the least significant byte
        };
    }

    private static int byteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);
    }
}
