package graveldb.DataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class SSTable {

    private static final String FILE_PREFIX = "./ssfile.data";
    private static final Logger log = LoggerFactory.getLogger(SSTable.class);

    public void flushMemtable(ConcurrentMap<String, String> memtable) {
        try {

            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(FILE_PREFIX));
            try (bos) {
                for (Map.Entry<String, String> entry : memtable.entrySet()) {
                    byte[] key = entry.getKey().getBytes();
                    byte[] value = entry.getValue().getBytes();

                    int capacity = 4 + 4 + key.length + value.length;
                    ByteBuffer pairEntry = ByteBuffer.allocate(capacity);

                    pairEntry.putInt(key.length);
                    pairEntry.putInt(value.length);
                    pairEntry.put(key);
                    pairEntry.put(value);

                    bos.write(pairEntry.array());

                }
            } catch (Exception e) {
                log.error("error creating sstable",e);
            }

        } catch (Exception e) {
            log.error("error while converting memtable to sstable",e);
        }
    }

    public String get(String targetKey) {
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(FILE_PREFIX))) {
            while (true) {
                byte[] keyLengthBytes = new byte[4];
                if (inputStream.read(keyLengthBytes) == -1) break; // EOF reached
                int keyLength = byteArrayToInt(keyLengthBytes);

                byte[] valueLengthBytes = new byte[4];
                if (inputStream.read(valueLengthBytes) == -1) break; // EOF reached
                int valueLength = byteArrayToInt(valueLengthBytes);

                byte[] keyBytes = new byte[keyLength];
                if (inputStream.read(keyBytes) == -1) break; // EOF reached
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                byte[] valueBytes = new byte[valueLength];
                if (inputStream.read(valueBytes) == -1) break; // EOF reached
                String value = new String(valueBytes, StandardCharsets.UTF_8);

                if (key.equals(targetKey)) {
                    return value;
                }
            }
        } catch (IOException e) {
            log.error("error reading file",e);
        }
        return null;
    }

    private static int byteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);
    }
}
