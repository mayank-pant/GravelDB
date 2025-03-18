package graveldb;

import graveldb.datastore.lsmtree.LSMTree;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LSMTreeTest {

    static LSMTree lsmTree;
    static List<Integer> numbers;
    static int start = 0;
    static int end = 50000;
    static int count = 0;

    private static final Logger log = LoggerFactory.getLogger(LSMTreeTest.class);

    @BeforeAll
    static void setup() throws IOException {
        Random random = new Random(42);
        numbers = new ArrayList<>();
        for (int i = start; i < end; i++) {
            int randomNumber = random.nextInt(end);
            numbers.add(randomNumber);
        }
        lsmTree = new LSMTree();
    }

//    @BeforeAll
//    static void setup() throws IOException {
//        numbers = new ArrayList<>();
//        for (int i = start; i < end; i++) {
//            numbers.add(i);
//        }
//        lsmTree = new LSMTree();
//    }

    @Test
    @Order(1)
    void test_dbPut() throws IOException, InterruptedException {
        Collections.shuffle(numbers);

        for (Integer ele : numbers) {
            String sele = String.valueOf(ele);
            assertDoesNotThrow(() -> lsmTree.put(sele,sele));
        }

    }

    @Test
    @Order(2)
    void test_dbGet() throws IOException {
        Collections.shuffle(numbers);

        for (Integer ele : numbers) {
            count++;
            String sele = String.valueOf(ele);
            String val = lsmTree.get(sele);
            if (val == null) log.info("null value {}", ele);
            assertEquals(sele, val);
        }
    }

    @Test
    @Order(3)
    void test_dbNotPresent() throws IOException {
        List<String> notPresentElements = new ArrayList<>();
        for (int i=end+1; i<=end+1000; i++) notPresentElements.add(String.valueOf(i));
        for (String ele : notPresentElements) {
            assertNull(lsmTree.get(ele));
        }
    }

//    @Test
//    void test_dbPutAndGetAndDel() throws IOException {
//        assertDoesNotThrow(() -> lsmTree.put("1","1"));
//        assertEquals("1", lsmTree.get("1"));
//    }

    @AfterAll
    static void tearDown() {
        log.info("total current count of keys {}", count);

        try {
            lsmTree.stop();
            Thread.sleep(5000);
        } catch (Exception e) {
            log.info("exception while stopping the tree");
        }

        boolean result1 = deleteDirectory(new File("./waldata"));
        boolean result2 = deleteDirectory(new File("./dbdata"));

        if (!(result1 && result2)) throw new RuntimeException("db files are not deleted");
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}