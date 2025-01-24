package graveldb.datastore.lsmtree.sstable;

import graveldb.datastore.lsmtree.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class SSTableX implements Iterable<KeyValuePair> {

    private final String FILE_NAME;
    private static final Logger log = LoggerFactory.getLogger(SSTableX.class);

    public SSTableX(String fileName) {
        this.FILE_NAME = fileName;
    }

    @Override
    public Iterator<KeyValuePair> iterator() {
        try {
            return new SSTableIterator();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file not found exception");
        }
    }

    public boolean delete() throws IOException {
        Files.delete(Path.of(FILE_NAME));
        return true;
    }

    class SSTableIterator implements Iterator<KeyValuePair> {

        BufferedInputStream fis;

        public SSTableIterator() throws FileNotFoundException {
            fis = new BufferedInputStream(new FileInputStream(FILE_NAME));
        }

        @Override
        public boolean hasNext() {
            try {
                return fis.available() > 0;
            } catch (IOException e) {
                log.error("error while checking for available bytes to read from sstable {}",FILE_NAME);
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
                if (byteArrayToInt(valLenBytes) != 0) {
                    byte[] valueBytes = new byte[byteArrayToInt(valLenBytes)];
                    fis.read(valueBytes);
                    value = new String(valueBytes);
                }

                KeyValuePair kvp = new KeyValuePair(
                        new String(keybytes),
                        value
                );

                return kvp;
            } catch (Exception e) {
                log.error("error reading the next value in sstable file {}", FILE_NAME);
                return new KeyValuePair(); }
        }
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
