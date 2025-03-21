package graveldb;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.lsmtree.LSMTree;
import graveldb.parser.Request;
import graveldb.wal.WalRecovery;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravelServer {
    private final int port;
    private final KeyValueStore store;
    private static final Logger logger = LoggerFactory.getLogger(GravelServer.class);

    public GravelServer(int port) {
        this.port = port;
        this.store = new LSMTree();
    }

    public void start() {
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();

        try (bossGroup; workerGroup) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RedisServerHandler(store));
                        }
                    });

            try {
                recover();
            } catch (Exception e) {
                logger.error("WAL Recovery Failed.",e);
                throw new RuntimeException("WAL Recovery Failed.");
            }

            ChannelFuture future = bootstrap.bind(port).sync();
            if (future.isSuccess()) {
                logger.info("Server started on port {}", port);
                future.channel().closeFuture().sync();
            } else {
                logger.error("Failed to bind to port {}: {}", port, future.cause().getMessage());
            }

        } catch (Exception e) {
            logger.error("error in establishing connection",e);
            throw new RuntimeException("error in establishing connection, port - "+port);
        }
    }

    private void recover() {
        WalRecovery walRecovery = new WalRecovery();
        for (Request request : walRecovery) {
            switch (request.command()) {
                case SET -> store.put(request.key(), request.value());
                case DEL -> store.delete(request.key());
                default -> logger.error("invalid command");
            }
        }
        walRecovery.deleteFiles();
        logger.info("Recovery complete.");
    }
}
