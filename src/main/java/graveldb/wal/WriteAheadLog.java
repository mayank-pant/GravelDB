package graveldb.wal;

import java.io.*;
import java.nio.file.*;
import java.util.List;

public class WriteAheadLog {
    private final Path walFile;

    public WriteAheadLog(String walFilePath) throws IOException {
        this.walFile = Paths.get(walFilePath);
        Files.createDirectories(walFile.getParent()); // Ensure directory exists
        if (!Files.exists(walFile)) {
            Files.createFile(walFile); // Create the file if it doesn't exist
        }
    }

    // Append an operation to the log
    // TODO : Optimize for performance by batching WAL writes or using asynchronous processing.
    public synchronized void append(String operation, String key, String value) throws IOException {
        try (FileWriter writer = new FileWriter(walFile.toFile(), true)) {
            String entry = operation + " " + key + (value != null ? " " + value : "") + "\n";
            writer.write(entry);
        }
    }

    // Read the WAL file for recovery
    public List<String> readAll() throws IOException {
        return Files.readAllLines(walFile);
    }

    // Clear the WAL after recovery or compaction
    public synchronized void clear() throws IOException {
        Files.write(walFile, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
    }

}

