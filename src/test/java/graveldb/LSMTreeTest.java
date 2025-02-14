package graveldb;

import graveldb.datastore.lsmtree.LSMTree;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LSMTreeTest {

    LSMTree lsmTree = new LSMTree();

    LSMTreeTest() throws IOException {

    }

    @Test
    void dbGet() throws IOException {
        assertDoesNotThrow(() -> lsmTree.put("1","1"));
        assertEquals("1", lsmTree.get("1"));
    }
}