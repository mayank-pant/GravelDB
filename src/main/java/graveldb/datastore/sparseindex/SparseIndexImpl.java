package graveldb.datastore.sparseindex;

import graveldb.datastore.lsmtree.KeyValuePair;
import graveldb.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class SparseIndexImpl {

    private static final Logger log = LoggerFactory.getLogger(SparseIndexImpl.class);

    private final String fileName;
    List<Pair<String, Integer>> sparseTable = null;


    public SparseIndexImpl(String fileName) throws IOException {
        this.fileName = fileName;
        Path path = Path.of(fileName);
        if (!Files.exists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }
    }

    public SparseIndexWriter getWriter() throws FileNotFoundException { return new SparseIndexWriter(); }

    public List<Pair<String, Integer>> getSparseIndexTable() throws IOException {
        if (sparseTable != null) return sparseTable;

        this.sparseTable = new ArrayList<>();

        RandomAccessFile fis = new RandomAccessFile(fileName, "r");
        try (fis) {
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
            throw new RuntimeException("error reading sparse table");
        }
    }

    public class SparseIndexWriter implements AutoCloseable {

        BufferedOutputStream bos;

        public SparseIndexWriter() throws FileNotFoundException {
            bos = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        public void write(String key, int offset) throws IOException {
            bos.write(intToByteArray(key.getBytes().length));
            bos.write(key.getBytes());
            bos.write(intToByteArray(offset));
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
