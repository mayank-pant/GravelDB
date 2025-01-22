package graveldb.datastore.lsmtree.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

public class SSTableIO implements SSTable {

    private static final Logger log = LoggerFactory.getLogger(SSTableIO.class);
    private final LinkedList<String> ssTableList;

    public SSTableIO() {
        this.ssTableList = new LinkedList<>();
    }

    @Override
    public void addSSTable(String filePath) { ssTableList.addFirst(filePath); }

    public String get(String targetKey) {

        for (String curFile: ssTableList) {

            try (InputStream inputStream = new BufferedInputStream(new FileInputStream(curFile))) {
                while (true) {
                    byte[] keyLengthBytes = new byte[4];
                    if (inputStream.read(keyLengthBytes) == -1) break;
                    int keyLength = byteArrayToInt(keyLengthBytes);

                    byte[] valueLengthBytes = new byte[4];
                    if (inputStream.read(valueLengthBytes) == -1) break;
                    int valueLength = byteArrayToInt(valueLengthBytes);

                    byte[] keyBytes = new byte[keyLength];
                    if (inputStream.read(keyBytes) == -1) break;
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    byte[] valueBytes = new byte[valueLength];
                    if (inputStream.read(valueBytes) == -1) break;
                    String value = new String(valueBytes, StandardCharsets.UTF_8);

                    if (key.equals(targetKey) && valueLength != 0) {
                        return value;
                    } else if (key.equals(targetKey)) {
                        return null;
                    }
                }
            } catch (IOException e) {
                log.error("error reading file",e);
            }
        }
        return null;
    }

    @Override
    public String getAll() {
        return "GETALL";
    }

    @Override
    public int size() {
        return 0;
    }

    private static int byteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);
    }
}
