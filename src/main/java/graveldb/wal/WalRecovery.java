package graveldb.wal;

import graveldb.parser.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class WalRecovery implements Iterable<Request> {

    private static final Logger log = LoggerFactory.getLogger(WalRecovery.class);

    private static final String WAL_DIR = "./waldata/";

    private final LinkedList<WriteAheadLog> walFiles;

    public WalRecovery() throws IOException {
        this.walFiles = new LinkedList<>();
        getWalFiles();
    }

    private void getWalFiles() throws IOException {

        Files.createDirectories(Paths.get(WAL_DIR));

        File directory = new File(WAL_DIR);
        File[] files = directory.listFiles();
        if (files == null) return;

        log.info("{} sstables found in the directory", files.length);

        Arrays.sort(files, Comparator.comparingLong(File::lastModified));

        for (int i=0; i<files.length-1; i++) {
            walFiles.addLast(new WriteAheadLog(WAL_DIR + files[i].getName()));
        }
    }

    @Override
    public Iterator<Request> iterator() {
        return new WalFilesIterator();
    }

    public void deleteFiles() throws IOException {
        boolean skipFirst = true;
        for (Iterator<WriteAheadLog> it = walFiles.descendingIterator(); it.hasNext(); ) {
            if (skipFirst) {
                skipFirst = false;
                continue;
            }
            it.next().delete();
        }
    }

    public class WalFilesIterator implements Iterator<Request> {

        Iterator<WriteAheadLog> walIteratorIterator;
        Iterator<Request> curWalItr;

        public WalFilesIterator() {
            this.walIteratorIterator = walFiles.iterator();
            this.curWalItr = null;
        }

        @Override
        public boolean hasNext() {
            if (curWalItr == null) {
                if (walIteratorIterator.hasNext()) {
                    curWalItr = walIteratorIterator.next().iterator();
                    return curWalItr.hasNext();
                } else return false;
            } else {
                if (curWalItr.hasNext()) {
                    return true;
                } else {
                    if (walIteratorIterator.hasNext()) {
                        curWalItr = walIteratorIterator.next().iterator();
                        return curWalItr.hasNext();
                    } else return false;
                }
            }
        }

        @Override
        public Request next() {
            return curWalItr.next();
        }
    }
}
