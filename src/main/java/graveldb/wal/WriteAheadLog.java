package graveldb.wal;

import graveldb.parser.Command;
import graveldb.parser.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class WriteAheadLog implements Iterable<Request> {

    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    private static final String WAL_DIR = "./waldata/";
    private static final String FILE_POSTFIX = "_wal.data";

    private final Path walFile;

    public WriteAheadLog() throws IOException {
        this.walFile = Paths.get(WAL_DIR + UUID.randomUUID() + FILE_POSTFIX);
        Files.createDirectories(walFile.getParent());
        if (!Files.exists(walFile)) Files.createFile(walFile);
    }

    public WriteAheadLog(String fileName) {
        this.walFile = Paths.get(fileName);
    }

    public synchronized void append(String operation, String key, String value) throws IOException {
        try (FileWriter writer = new FileWriter(walFile.toFile(), true)) {
            String entry = operation + " " + key + (value != null ? " " + value : "") + "\n";
            writer.write(entry);
        }
    }

    public synchronized void clear() throws IOException {
        Files.write(walFile, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
    }

    public synchronized void delete() throws IOException {
        Files.delete(walFile);
    }

    @Override
    public Iterator<Request> iterator() {
        try {
            return new WALIterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class WALIterator implements Iterator<Request> {

        Iterator<String> iterator;

        public WALIterator() throws IOException {
            List<String> data = Files.readAllLines(walFile);
            iterator = data.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Request next() {
            String[] parts = iterator.next().split(" ");
            Command command = Command.valueOf(parts[0]);
            switch (command) {
                case SET -> {
                    return new Request(command, parts[1], parts[2]);
                }
                case DEL -> {
                    return new Request(command, parts[1], null);
                }
                default -> {
                    return null;
                }
            }
        }
    }
}

