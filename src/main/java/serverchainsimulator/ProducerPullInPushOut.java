package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class ProducerPullInPushOut implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProducerPullInPushOut.class);
    final private MessageGenerator generator;
    final private CountDownLatch done;
    final private InetSocketAddress address;
    final private CyclicBarrier barrier;
    List<Path> paths;
    final private NonBlockingConsumerEagerIn otherThread;
    public ProducerPullInPushOut(CountDownLatch latch, List<Path> readerPaths, MessageGenerator messageGenerator, InetSocketAddress address, CyclicBarrier barrier, NonBlockingConsumerEagerIn otherThread) {
        done = latch;
        paths = readerPaths;
        generator = messageGenerator;
        this.address = address;
        this.barrier = barrier;
        this.otherThread = otherThread;
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
                    for(Path path : paths)
                    {
                        try {
                            Thread.sleep(1000 * 3);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                            logger.info("Producer sending " + path);
                            while ((line = reader.readLine()) != null) {
                                if (line.length() > 0) {
                                    try {
                                        generator.write(line, buffer, Boolean.FALSE);
                                        buffer.flip();
                                        socketChannel.write(buffer);
                                        buffer.clear();
                                    } catch (BufferOverflowException ex) {
                                        logger.error("Producer error", ex);
                                    }
                                }
                            }
                            try {
                                Thread.sleep(1000 * 3);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            try {
                                otherThread.signalOfBatch();
                                barrier.await();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (BrokenBarrierException e) {
                                e.printStackTrace();
                            }
                        }
                        //end of this file
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
