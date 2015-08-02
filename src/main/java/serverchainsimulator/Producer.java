package serverchainsimulator;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    final private MessageGenerator generator;
    final private CountDownLatch done;
    final private String host;
    final private int port;
    Path path;

    public Producer(CountDownLatch latch, Path readerPath, MessageGenerator messageGenerator, String host, int port) {
        done = latch;
        path = readerPath;
        generator = messageGenerator;
        this.port = port;
        this.host = host;
    }

    public void run() {

        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
            if (serverSocketChannel.isOpen()) {
                serverSocketChannel.configureBlocking(true);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(new InetSocketAddress(host, port));
                try (SocketChannel socketChannel = serverSocketChannel.accept()) {
                    logger.info("Producer connected " + socketChannel.getLocalAddress() + " <- " + socketChannel.getRemoteAddress());
                    String line = null;
                    int seq = 1;
                    ByteBuffer buffer = ByteBuffer.allocateDirect(2024);
                    try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                        while ((line = reader.readLine()) != null) {
                            generator.write(line, buffer);
                            buffer.flip();
                            socketChannel.write(buffer);
                            buffer.clear();
                        }
                    }
                } catch (IOException ex) {
                    logger.error("Producer cannot read data ", ex);
                }
            } else {
                logger.warn("The server socket channel cannot be opened!");
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }
        try {
            Thread.sleep(1000 * 2);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        done.countDown();
        logger.info("Producer is done! " + done.getCount());
    }

}
