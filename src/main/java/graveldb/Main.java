package graveldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {new GravelServer(6371).start();}
        catch (Exception e) {logger.error("Some error during db start {}",e.getMessage(), e);}
    }
}