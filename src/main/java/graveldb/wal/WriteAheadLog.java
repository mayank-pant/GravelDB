package graveldb.wal;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.UUID;

public class WriteAheadLog {
    private final Path walFile;
    private static final String WAL_DIR = "./waldata/";
    private static final String FILE_POSTFIX = "_wal.data";

    public WriteAheadLog() throws IOException {
        this.walFile = Paths.get(WAL_DIR + UUID.randomUUID() + FILE_POSTFIX);
        Files.createDirectories(walFile.getParent());
        if (!Files.exists(walFile)) Files.createFile(walFile);
    }

    public synchronized void append(String operation, String key, String value) throws IOException {
        try (FileWriter writer = new FileWriter(walFile.toFile(), true)) {
            String entry = operation + " " + key + (value != null ? " " + value : "") + "\n";
            writer.write(entry);
        }
    }

    public List<String> readAll() throws IOException {
        return Files.readAllLines(walFile);
    }

    public synchronized void clear() throws IOException {
        Files.write(walFile, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
    }

    public synchronized void delete() throws IOException {
        Files.delete(walFile);
    }
}

