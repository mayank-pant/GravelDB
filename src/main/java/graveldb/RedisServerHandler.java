package graveldb;

import graveldb.DataStore.KeyValueStore;
import graveldb.Lexer.Lexer;
import graveldb.Parser.Command;
import graveldb.Parser.Parser;
import graveldb.WAL.WriteAheadLog;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RedisServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final KeyValueStore store;
    private final WriteAheadLog wal;
    private final Lexer lexer = new Lexer();
    private final Parser parser = new Parser();

    public RedisServerHandler(KeyValueStore store, WriteAheadLog wal) {
        this.store = store;
        this. wal = wal;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        // Decode the incoming ByteBuf to a string
        String input = msg.toString(StandardCharsets.UTF_8).trim();

        try {
            // Step 1: Tokenize
            List<String> tokens = lexer.tokenize(input);

            // Step 2: Parse
            Command command = parser.parse(tokens);

            // Step 3: Process command synchronously
            String response = processCommand(command);

            // Write response back to client
            ctx.writeAndFlush(ctx.alloc().buffer().writeBytes(response.getBytes()));

        } catch (IllegalArgumentException e) {
            // Send error response for invalid input
            String errorResponse = "-ERR " + e.getMessage() + "\r\n";
            ctx.writeAndFlush(ctx.alloc().buffer().writeBytes(errorResponse.getBytes()));
        }
    }

    private String processCommand(Command command) throws IllegalArgumentException {
        try {
            switch (command.operation()) {
                case "SET":
                    wal.append("SET", command.key(), command.value());
                    store.put(command.key(), command.value());
                    return "+OK\r\n";
                case "GET":
                    String value = store.get(command.key());
                    return value == null ? "$-1\r\n" : "$" + value.length() + "\r\n" + value + "\r\n";
                case "DEL":
                    wal.append("DEL", command.key(), null);
                    String deleted = store.delete(command.key());
                    return ":" + deleted + "\r\n";
                case "DBSIZE":
                    return ":" + store.size() + "\r\n";
                case "GETALL":
                    return store.getAll() + "\r\n";
                default:
                    return "-ERR Unknown command\r\n";
            }
        } catch (IOException e) {
            return "-ERR Failed to process command\r\n";
        }
    }
}
