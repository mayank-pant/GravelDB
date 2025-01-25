package graveldb.parser;

import java.util.List;

public class Parser {

    // TODO : Enumify the Command token
    public Request parse(List<String> tokens) throws IllegalArgumentException {
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Invalid command: No tokens found");
        }

        Command command = Command.valueOf(tokens.get(0).toUpperCase());
        return switch (command) {
            case Command.SET -> {
                if (tokens.size() != 3) {
                    throw new IllegalArgumentException("SET command requires exactly 2 arguments");
                }
                yield new Request(Command.SET, tokens.get(1), tokens.get(2));
            }
            case Command.GET -> {
                if (tokens.size() != 2) {
                    throw new IllegalArgumentException("GET command requires exactly 1 argument");
                }
                yield new Request(Command.GET, tokens.get(1), null);
            }
            case Command.DEL -> {
                if (tokens.size() != 2) {
                    throw new IllegalArgumentException("DEL command requires exactly 1 argument");
                }
                yield new Request(Command.DEL, tokens.get(1), null);
            }
        };
    }
}

