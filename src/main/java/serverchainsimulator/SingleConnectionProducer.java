package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.*;

public class SingleConnectionProducer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SingleConnectionProducer.class);
    final private MessageGenerator generator;
    private volatile Boolean process;
    final private String host;
    final private int port;
    final private Queue<String> input;
    final private CyclicBarrier turn;
    public SingleConnectionProducer(CyclicBarrier turn, Queue<String> input, MessageGenerator messageGenerator, String host, int port) {

        generator = messageGenerator;
        this.port = port;
        this.host = host;
        this.input = input;
        this.turn = turn;
    }

    public void terminate() {
        process = false;
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
                    String line;
                    ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
                    while(process) {
                        boolean run = true;
                        while (run) {

                                line = input.remove();

                            if (line != null && "".equals(line)) {
                                //terminal
                                run = false;
                            } else if (line != null && line.length() > 0) {
                                try {
                                    generator.write(line, buffer, false);
                                    buffer.flip();
                                    socketChannel.write(buffer);
                                    buffer.clear();
                                } catch (BufferOverflowException ex) {
                                    logger.error("Producer error", ex);
                                }
                            }
                        }
                        try {
                            turn.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (BrokenBarrierException e) {
                            e.printStackTrace();
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

        logger.info("Producer is done! " );
    }

}
