package graveldb;

import graveldb.datastore.lsmtree.LSMTree;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LSMTreeTest {

    LSMTree lsmTree = new LSMTree();

    LSMTreeTest() throws IOException {
    }

    @Test
    void test_dbPutAndGet() throws IOException, InterruptedException {
        int start = 1, end = 10000;
        List<Integer> numbers = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            numbers.add(i);
        }
        Collections.shuffle(numbers);

        for (Integer ele : numbers) {
            String sele = String.valueOf(ele);
            assertDoesNotThrow(() -> lsmTree.put(sele,sele));
        }


        Thread.sleep(100);
        Collections.shuffle(numbers);

        for (Integer ele : numbers) {
            String sele = String.valueOf(ele);
            assertEquals(sele, lsmTree.get(sele));
        }

        List<String> notPresentElements = new ArrayList<>();
        for (int i=end+1; i<=end+1000; i++) notPresentElements.add(String.valueOf(i));
        for (String ele : notPresentElements) {
            assertNull(lsmTree.get(ele));
        }
    }

    @Test
    void test_dbPutAndGetAndDel() throws IOException {
        assertDoesNotThrow(() -> lsmTree.put("1","1"));
        assertEquals("1", lsmTree.get("1"));
    }

    @AfterEach
    void tearDown() {
        try {
            lsmTree.stop();
            Thread.sleep(5000);
        } catch (Exception ignored) {
        }

        boolean result1 = deleteDirectory(new File("./waldata"));
        boolean result2 = deleteDirectory(new File("./dbdata"));

        if (!(result1 && result2)) throw new RuntimeException("db files are not deleted");
    }

    boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}