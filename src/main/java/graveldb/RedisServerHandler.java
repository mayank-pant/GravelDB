package graveldb;

import graveldb.datastore.KeyValueStore;
import graveldb.lexer.Lexer;
import graveldb.parser.Command;
import graveldb.parser.Parser;
import graveldb.parser.Request;
import graveldb.wal.WriteAheadLog;
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
            // Tokenize
            List<String> tokens = lexer.tokenize(input);

            // Parse
            Request request = parser.parse(tokens);

            // process command synchronously
            String response = processCommand(request);

            // write response back to client
            ctx.writeAndFlush(ctx.alloc().buffer().writeBytes(response.getBytes()));

        } catch (IllegalArgumentException e) {
            // Send error response for invalid input
            String errorResponse = "-ERR " + e.getMessage() + "\r\n";
            ctx.writeAndFlush(ctx.alloc().buffer().writeBytes(errorResponse.getBytes()));
        }
    }

    private String processCommand(Request request) throws IllegalArgumentException {
        try {
            switch (request.command()) {
                case Command.SET:
                    wal.append("SET", request.key(), request.value());
                    store.put(request.key(), request.value());
                    return "+OK\r\n";
                case Command.GET:
                    String value = store.get(request.key());
                    return value == null ? "$-1\r\n" : "$" + value.length() + "\r\n" + value + "\r\n";
                case Command.DEL:
                    wal.append("DEL", request.key(), null);
                    store.delete(request.key());
                    return ":OK\r\n";
                default:
                    return "-ERR Unknown command\r\n";
            }
        } catch (IOException e) {
            return "-ERR Failed to process command\r\n";
        }
    }
}
