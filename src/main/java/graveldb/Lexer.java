package graveldb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Lexer {
    public List<String> tokenize(String command) throws IllegalArgumentException {
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Empty or null command");
        }

        String[] tokens = command.trim().split("\\s+");
        return new ArrayList<>(Arrays.asList(tokens));
    }
}
