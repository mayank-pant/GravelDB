package graveldb.datastore.lsmtree.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.UUID;

public class SSTableIO implements SSTable {

    private static final Logger log = LoggerFactory.getLogger(SSTableIO.class);
    private final LinkedList<String> ssTableList;
    private static final String DATA_DIR = "./dbdata";
    private static final String FILE_POSTFIX = "ssfile.data";

    public SSTableIO() throws IOException {
        this.ssTableList = new LinkedList<>();
        fillSstableList();
    }

    private void fillSstableList() throws IOException {

        if (!Files.exists(Path.of(DATA_DIR))) {
            Files.createDirectories(Paths.get(DATA_DIR));
            return;
        }

        File directory = new File(DATA_DIR);
        File[] files = directory.listFiles();
        if (files == null) return;

        Arrays.sort(files, Comparator.comparingLong(File::lastModified));

        for (File f : files){
            ssTableList.addFirst(DATA_DIR +"/"+ f.getName());
        }
    }

    @Override
    public void addSSTable(String filePath) { ssTableList.addFirst(filePath); }

    @Override
    public void compaction() throws IOException {

        // check if compaction threshold reached
        // get the files to compact
        // start reading the key and value from file
        //

        // ss table size should atleast be 2 to proceed
        if (ssTableList.size() < 2) return;

        // generate new output file
        String outputFileName = DATA_DIR+"/"+ UUID.randomUUID() +"_"+FILE_POSTFIX;
        File outputFile = Files.createFile(Path.of(outputFileName)).toFile();

        // open input and output streams
        String file2 = ssTableList.pollLast();
        String file1 = ssTableList.pollLast();
        InputStream fis2 = new BufferedInputStream(new FileInputStream(file2));
        InputStream fis1 = new BufferedInputStream(new FileInputStream(file1));
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));

        // initialize variables to store values
        byte[] keyLenBytes1 = new byte[4];
        byte[] valLenBytes1 = new byte[4];
        int keyLen1 = 0;
        int valLen1 = 0;
        byte[] keyByte1 = new byte[0];
        byte[] valByte1 = new byte[0];
        String key1 = "";

        byte[] keyLenBytes2 = new byte[4];
        byte[] valLenBytes2 = new byte[4];
        int keyLen2 = 0;
        int valLen2 = 0;
        byte[] keyByte2 = new byte[0];
        byte[] valByte2 = new byte[0];
        String key2 = "";

        // try with resource
        try (fis1; fis2; bos) {
            while (true) {
                // if not keys are read yet then go inside the block
                if (Arrays.equals(keyLenBytes1, new byte[4]) && Arrays.equals(keyLenBytes2, new byte[4])) {
                    // if any of the key reads from file are empty then go out of loop
                    if (fis1.read(keyLenBytes1) == -1 || fis2.read(keyLenBytes2) == -1) break;

                    // read first key from both of the file
                    keyLen1 = byteArrayToInt(keyLenBytes1);
                    if (fis1.read(valLenBytes1) == -1) break;
                    valLen1 = byteArrayToInt(valLenBytes1);
                    keyByte1 = new byte[keyLen1];
                    if (fis1.read(keyByte1) == -1) break;
                    key1 = new String(keyByte1);

                    keyLen2 = byteArrayToInt(keyLenBytes2);
                    if (fis2.read(valLenBytes2) == -1) break;
                    valLen2 = byteArrayToInt(valLenBytes2);
                    keyByte2 = new byte[keyLen2];
                    if (fis2.read(keyByte2) == -1) break;
                    key2 = new String(keyByte2);
                }

                // compare the key value
                int cmp = key1.compareTo(key2);

                // if key 1 is smller
                if (cmp < 0) {

                    // if key1 value is present
                    if (valLen1 != 0) {
                        // get the key1 value
                        valByte1 = new byte[valLen1];
                        if (fis1.read(valByte1) == -1) break;

                        // write the key1 and val1 in file
                        bos.write(intToByteArray(keyLen1));
                        bos.write(intToByteArray(valLen1));
                        bos.write(keyByte1);
                        bos.write(valByte1);
                    }

                    // move to next key in file1 and load key and value length
                    key1 = "";
                    if (fis1.read(keyLenBytes1) == -1) break;
                    keyLen1 = byteArrayToInt(keyLenBytes1);
                    if (fis1.read(valLenBytes1) == -1) break;
                    valLen1 = byteArrayToInt(valLenBytes1);
                    keyByte1 = new byte[keyLen1];
                    if (fis1.read(keyByte1) == -1) break;
                    key1 = new String(keyByte1);

                } else if (cmp > 0) {
                    if (valLen2 != 0) {
                        valByte2 = new byte[valLen2];
                        if (fis2.read(valByte2) == -1) break;

                        bos.write(intToByteArray(keyLen2));
                        bos.write(intToByteArray(valLen2));
                        bos.write(keyByte2);
                        bos.write(valByte2);
                    }

                    key2 = "";
                    if (fis2.read(keyLenBytes2) == -1) break;
                    keyLen2 = byteArrayToInt(keyLenBytes2);
                    if (fis2.read(valLenBytes2) == -1) break;
                    valLen2 = byteArrayToInt(valLenBytes2);
                    keyByte2 = new byte[keyLen2];
                    if (fis2.read(keyByte2) == -1) break;
                    key2 = new String(keyByte2);

                } else {
                    if (valLen1 != 0) {
                        bos.write(intToByteArray(keyLen1));
                        bos.write(intToByteArray(valLen1));
                        bos.write(keyByte1);
                        bos.write(valByte1);
                    }
                    // file1 key read
                    key1 = "";
                    if (fis1.read(keyLenBytes1) == -1) break;
                    keyLen1 = byteArrayToInt(keyLenBytes1);
                    if (fis1.read(valLenBytes1) == -1) break;
                    valLen1 = byteArrayToInt(valLenBytes1);
                    keyByte1 = new byte[keyLen1];
                    if (fis1.read(keyByte1) == -1) break;
                    key1 = new String(keyByte1);

                    // file2 key read
                    key2 = "";
                    if (fis2.read(keyLenBytes2) == -1) break;
                    keyLen2 = byteArrayToInt(keyLenBytes2);
                    if (fis2.read(valLenBytes2) == -1) break;
                    valLen2 = byteArrayToInt(valLenBytes2);
                    keyByte2 = new byte[keyLen2];
                    if (fis2.read(keyByte2) == -1) break;
                    key2 = new String(keyByte2);
                }
            }

            if (!key1.isEmpty()) {
                while(true) {
                    if (valLen1 != 0) {
                        // get the key1 value
                        valByte1 = new byte[valLen1];
                        if (fis1.read(valByte1) == -1) break;

                        // write the key1 and val1 in file
                        bos.write(intToByteArray(keyLen1));
                        bos.write(intToByteArray(valLen1));
                        bos.write(keyByte1);
                        bos.write(valByte1);
                    }

                    if (fis1.read(keyLenBytes1) == -1) break;
                    keyLen1 = byteArrayToInt(keyLenBytes1);
                    if (fis1.read(valLenBytes1) == -1) break;
                    valLen1 = byteArrayToInt(valLenBytes1);
                    keyByte1 = new byte[keyLen1];
                    if (fis1.read(keyByte1) == -1) break;
                }
            }

            if (!key2.isEmpty()) {
                while(true) {
                    if (valLen2 != 0) {
                        valByte2 = new byte[valLen2];
                        if (fis2.read(valByte2) == -1) break;

                        bos.write(intToByteArray(keyLen2));
                        bos.write(intToByteArray(valLen2));
                        bos.write(keyByte2);
                        bos.write(valByte2);
                    }

                    if (fis2.read(keyLenBytes2) == -1) break;
                    keyLen2 = byteArrayToInt(keyLenBytes2);
                    if (fis2.read(valLenBytes2) == -1) break;
                    valLen2 = byteArrayToInt(valLenBytes2);
                    keyByte2 = new byte[keyLen2];
                    if (fis2.read(keyByte2) == -1) break;
                }
            }

        } catch (Exception e) {
            log.error("error during compaction",e);
            throw e;
        }

        Files.delete(Path.of(file1));
        Files.delete(Path.of(file2));
        ssTableList.addLast(outputFileName);
    }

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

    public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte) (value >>> 24), // Extracts the most significant byte
                (byte) (value >>> 16), // Extracts the second most significant byte
                (byte) (value >>> 8),  // Extracts the third most significant byte
                (byte) value           // Extracts the least significant byte
        };
    }

}
