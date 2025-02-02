package graveldb.datastore.sstable;

import graveldb.datastore.lsmtree.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class SSTableImpl implements SSTable {

    private static final Logger log = LoggerFactory.getLogger(SSTableImpl.class);

    private final String fileName;


    public SSTableImpl(String fileName) throws IOException {
        this.fileName = fileName;
        if (!Files.exists(Path.of(fileName))) {
            Files.createDirectories(Path.of(fileName).getParent());
            Files.createFile(Path.of(fileName));
        }
    }

    @Override
    public SSTableIterator iterator() {
        try {
            return new SSTableIterator();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file not found exception");
        }
    }

    public SSTableIterator iterator(int offset) throws Exception{
        try {
            return new SSTableIterator(offset);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file not found exception");
        }
    }

    @Override
    public String getFileName() {return fileName;}

    @Override
    public SSTableWriter getWriter() throws FileNotFoundException { return new SSTableWriter(); }

    public class SSTableIterator implements Iterator<KeyValuePair>, AutoCloseable {

        RandomAccessFile fis;

        public SSTableIterator() throws FileNotFoundException { fis = new RandomAccessFile(fileName, "r"); }
        public SSTableIterator(int offset) throws IOException {
            fis = new RandomAccessFile(fileName, "r");
            fis.seek(offset);
        }

        @Override
        public boolean hasNext() {
            try {
                return fis.getFilePointer() < fis.length();
            } catch (IOException e) {
                log.error("error while checking for available bytes to read from sstable {}", fileName);
                throw new RuntimeException(e);
            }
        }

        @Override
        public KeyValuePair next() {
            try {
                byte[] keyLenBytes = new byte[4];
                fis.read(keyLenBytes);
                byte[] valLenBytes = new byte[4];
                fis.read(valLenBytes);
                byte[] keybytes = new byte[byteArrayToInt(keyLenBytes)];
                fis.read(keybytes);
                String value = "";
                boolean isDeleted = true;
                if (byteArrayToInt(valLenBytes) != 0) {
                    byte[] valueBytes = new byte[byteArrayToInt(valLenBytes)];
                    fis.read(valueBytes);
                    value = new String(valueBytes);
                    isDeleted = false;
                }
                return new KeyValuePair(
                        new String(keybytes),
                        value,
                        isDeleted
                );
            } catch (Exception e) {
                log.error("error reading the next value in sstable file {}", fileName);
                return null; }
        }

        @Override
        public void close() throws Exception { fis.close(); }
    }

    public class SSTableWriter implements AutoCloseable {

        BufferedOutputStream bos;

        public SSTableWriter() throws FileNotFoundException {
            bos = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        public void write(KeyValuePair kvp) throws IOException {
            bos.write(intToByteArray(kvp.key().getBytes().length));
            bos.write(intToByteArray(kvp.value().getBytes().length));
            bos.write(kvp.key().getBytes());
            bos.write(kvp.value().getBytes());
        }

        @Override
        public void close() throws Exception { bos.flush();
            bos.close(); }
    }

    private static int byteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);
    }

    public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte) (value >>> 24), // Extracts the most significant byte
                (byte) (value >>> 16), // Extracts the second most significant byte
                (byte) (value >>> 8),  // Extracts the third most significant byte
                (byte) value           // Extracts the least significant byte
        };
    }
}
