package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 *
 */
public class NonBlockingConsumerEagerIn implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NonBlockingConsumerEagerIn.class);

    public final InetSocketAddress adress;
    volatile private boolean process = true;
    private final List<Path> paths;
    private final MessageReceiver receiver;
    final private CyclicBarrier barrier;
    RemoteShellScripTask otherThread;
    private int readCount;

    public NonBlockingConsumerEagerIn(List<Path> writerPaths, MessageReceiver messageReceiver, InetSocketAddress adress, CyclicBarrier barrier,RemoteShellScripTask otherThread) {
        paths = writerPaths;
        receiver = messageReceiver;
        this.adress = adress;
        this.barrier = barrier;
        this.otherThread = otherThread;
    }

    public void signalOfBatch() {
        process = false;
        logger.info("process set to "+ process);
    }

    @SuppressWarnings("rawtypes")
    public void run() {


        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        logger.info("Consumer opening, path  " +adress);


        try (Selector selector = Selector.open();
             SocketChannel socketChannel = SocketChannel.open()) {
            if ((socketChannel.isOpen()) && (selector.isOpen())) {
                socketChannel.configureBlocking(false);
                socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
                socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                socketChannel.register(selector, SelectionKey.OP_CONNECT);
                socketChannel.connect(adress);
                logger.info("Consumer: " + socketChannel.getRemoteAddress());
                while (selector.select(1000) > 0) {
                    Set keys = selector.selectedKeys();
                    Iterator its = keys.iterator();
                    while (its.hasNext()) {
                        SelectionKey key = (SelectionKey) its.next();
                        its.remove();
                        try (SocketChannel keySocketChannel = (SocketChannel) key.channel()) {
                            if (key.isConnectable()) {
                                //logger.info("Consumer connecting " + path);
                                if (keySocketChannel.isConnectionPending()) {
                                    keySocketChannel.finishConnect();
                                }

                                for(Path path : paths) {
                                    logger.info("Consumer connected " + path);
                                    if (Files.notExists(path.getParent())) {
                                        Files.createDirectories(path.getParent());
                                    }
                                    try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.CREATE)) {
                                        long x = 0;
                                        long y = 0;
                                        logger.info("awaiting 0 reads  " + x + " 1 reads " + y);

                                        while (keySocketChannel.read(buffer) != -1) {
                                            // if ( ((x+1) % 100000 ==0) || ((y+1) % 100000 == 0) ) {
                                            //	 logger.info("0 reads  " + x +" 1 reads "+ y);
                                            // }
                                            //logger.info("process "+ process);
                                            if (buffer.position() > 0) {
                                                y++;
                                                buffer.flip();
                                                String line = receiver.read(buffer);
                                                writer.write(line);
                                                writer.write("\n");
                                                if (logger.isTraceEnabled()) {
                                                    logger.trace("Read " + line);
                                                } else {
                                                    logger.info("Read " + line);
                                                }
                                                readCount++;
                                                if (buffer.hasRemaining()) {
                                                    buffer.compact();
                                                } else {
                                                    buffer.clear();
                                                }
                                            } else if (!process) {
                                                logger.info("Consumer says good bye;");
                                                //return;
                                                break;
                                            } else {
                                                x++;
                                            }
                                        }
                                    }
                                    try {
                                        if(otherThread!=null) {
                                            otherThread.signalBeginOfBatch();
                                        }
                                        barrier.await();
                                        process = true;
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    } catch (BrokenBarrierException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        } catch (IOException ex) {
                            logger.error("Consumer", ex);
                        }
                    }
                }
            } else {
                logger.warn("The socket channel or selector cannot be opened!");
            }
        } catch (IOException ex) {
            logger.error("Consumer", ex);
        }
    }

    public int getReadCount(){
        return readCount;
    }
}
