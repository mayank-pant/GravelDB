package graveldb;

import graveldb.datastore.KeyValueStore;
import graveldb.datastore.lsmtree.LSMTree;
import graveldb.wal.WriteAheadLog;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;

public class GravelServer {
    private final int port;
    private final KeyValueStore store;
    private final WriteAheadLog wal;
    private static final Logger logger = LoggerFactory.getLogger(GravelServer.class);
    private static final String WAL_FILE = "./wal.txt";

    public GravelServer(int port) throws IOException {
        this.port = port;
        this.wal = new WriteAheadLog(WAL_FILE);
        this.store = new LSMTree(this.wal);
    }

    public void start() throws InterruptedException {
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();

        try (bossGroup; workerGroup) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RedisServerHandler(store, wal));
                        }
                    });

            try {
                recover();
            } catch (Exception e) {
                logger.error("WAL Recovery Failed.",e);
                throw new InterruptedException("WAL Recovery Failed.");
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

    private void recover() throws IOException {
        List<String> entries = wal.readAll();
        for (String entry : entries) {
            String[] parts = entry.split(" ");
            if (parts[0].equals("SET")) {
                store.put(parts[1], parts[2]);
            } else if (parts[0].equals("DEL")) {
                store.delete(parts[1]);
            }
        }
        logger.info("Recovery complete.");
    }
}
