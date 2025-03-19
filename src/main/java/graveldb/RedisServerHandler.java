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
    private final Lexer lexer = new Lexer();
    private final Parser parser = new Parser();

    public RedisServerHandler(KeyValueStore store) {
        this.store = store;
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
            return switch (request.command()) {
                case Command.SET -> {
                    store.put(request.key(), request.value());
                    yield "+OK\r\n";
                }
                case Command.GET -> {
                    String value = store.get(request.key());
                    yield value == null ? "$-1\r\n" : "$" + value.length() + "\r\n" + value + "\r\n";
                }
                case Command.DEL -> {
                    store.delete(request.key());
                    yield ":OK\r\n";
                }
            };
        } catch (Exception e) {
            return "-ERR Failed to process command\r\n";
        }
    }
}
