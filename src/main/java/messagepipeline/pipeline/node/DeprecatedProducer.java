package messagepipeline.pipeline.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.message.Encoder;

public class DeprecatedProducer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DeprecatedProducer.class);
    final private Encoder generator;
    final private CountDownLatch done;
    final private InetSocketAddress address;
    Path path;

    public DeprecatedProducer(CountDownLatch latch, Path readerPath, Encoder encoder, InetSocketAddress address) {
        done = latch;
        path = readerPath;
        generator = encoder;
        this.address = address;
    }

    public void run() {
    	logger.info("Producer opening");
        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) { logger.info("Producer open");
            if (serverSocketChannel.isOpen()) {  logger.info("Producer is open");
                serverSocketChannel.configureBlocking(true);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(address); logger.info("Producer accepting on " + address.toString());
                try (SocketChannel socketChannel = serverSocketChannel.accept()) {  
                    logger.info("Producer connected " + socketChannel.getLocalAddress() + " <- " + socketChannel.getRemoteAddress());
                    String line;
                    ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
                    try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                    	// logger.info("Producer sending " + path);
                        while ((line = reader.readLine()) != null) {
                            if(line.length()>0) {
                                try {
                                    if(generator.write(line, buffer)) {
                                    buffer.flip();
                                    socketChannel.write(buffer);
                                    buffer.clear();
                                    }
                                }catch(BufferOverflowException ex){
                                    logger.error("Producer error", ex);
                                }
                            }
                        }
                    }
                } catch (IOException ex) {
                    logger.error("Producer cannot read data ", ex);
                }
            } else {
                logger.warn("server socket channel cannot be opened");
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
        logger.info("Producer is done " + done.getCount());
    }

}
