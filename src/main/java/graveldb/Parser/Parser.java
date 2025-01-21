package graveldb.Parser;

import java.util.List;

public class Parser {

    // TODO : Enumify the Command token
    public Command parse(List<String> tokens) throws IllegalArgumentException {
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Invalid command: No tokens found");
        }

        String operation = tokens.get(0).toUpperCase();
        return switch (operation) {
            case "SET" -> {
                if (tokens.size() != 3) {
                    throw new IllegalArgumentException("SET command requires exactly 2 arguments");
                }
                yield new Command("SET", tokens.get(1), tokens.get(2));
            }
            case "GET" -> {
                if (tokens.size() != 2) {
                    throw new IllegalArgumentException("GET command requires exactly 1 argument");
                }
                yield new Command("GET", tokens.get(1), null);
            }
            case "DEL" -> {
                if (tokens.size() != 2) {
                    throw new IllegalArgumentException("DEL command requires exactly 1 argument");
                }
                yield new Command("DEL", tokens.get(1), null);
            }
            case "DBSIZE" -> {
                if (tokens.size() != 1) {
                    throw new IllegalArgumentException("DBSIZE command does not take any arguments");
                }
                yield new Command("DBSIZE", null, null);
            }
            case "GETALL" -> {
                if (tokens.size() != 1) {
                    throw new IllegalArgumentException("GETALL command does not take any arguments");
                }
                yield new Command("GETALL", null, null);
            }
            default -> throw new IllegalArgumentException("Unknown command: " + operation);
        };
    }
}

