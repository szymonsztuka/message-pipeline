package messagepipeline.pipeline.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.message.Decoder;

import java.io.BufferedWriter;
import java.io.File;
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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class PullConsumer implements Runnable, Node {

    private static final Logger logger = LoggerFactory.getLogger(PullConsumer.class);
    public final InetSocketAddress address;
    private volatile boolean process = true;
    private final List<Path> paths;
    private final Decoder receiver;
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final Path baseDir;
    private final String name;

    public PullConsumer(String name, String directory, List<String> messagePaths, Decoder decoder, InetSocketAddress address, CyclicBarrier start, CyclicBarrier end) {
        this.name = name;
        this.baseDir = Paths.get(directory);
        this.paths = messagePaths.stream().map(s -> Paths.get(this.baseDir + File.separator + s)).collect(Collectors.toList());
        this.receiver = decoder;
        this.address = address;
        this.batchStart = start;
        this.batchEnd = end;
    }

    public void signalBatchEnd() {
        process = false;
        //logger.info("process set to " + process);
    }

    public void run() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        try (Selector selector = Selector.open();
             SocketChannel socketChannel = SocketChannel.open()) {
            if ((socketChannel.isOpen()) && (selector.isOpen())) {
                socketChannel.configureBlocking(false);
                socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
                socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                socketChannel.register(selector, SelectionKey.OP_CONNECT);
                socketChannel.connect(address);
                while (selector.select(10000) > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator its = keys.iterator();
                    while (its.hasNext()) {
                        SelectionKey key = (SelectionKey) its.next();
                        its.remove();
                        try (SocketChannel keySocketChannel = (SocketChannel) key.channel()) {
                            if (key.isConnectable()) {
                                if (keySocketChannel.isConnectionPending()) {
                                    System.out.println(keySocketChannel.getLocalAddress());
                                    System.out.println(keySocketChannel.getRemoteAddress());
                                   keySocketChannel.finishConnect();

                                }
                               // logger.info("Source " + socketChannel.getLocalAddress() + " -> " + socketChannel.getRemoteAddress()
                               //         + ", destination " + baseDir.toString());
                                for (Path path : paths) {
                                    logger.trace("connection " + path);
                                    try {

                                        batchStart.await();                                       logger.debug(name + " s "+ batchStart.getParties() + " "+ batchStart.getNumberWaiting());

                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    } catch (BrokenBarrierException e) {
                                        e.printStackTrace();
                                    }
                                    if (Files.notExists(path.getParent())) {
                                        Files.createDirectories(path.getParent());
                                    }
                                    try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.CREATE)) {
                                        while (keySocketChannel.read(buffer) != -1) {
                                            if (buffer.position() > 0) {
                                                buffer.flip();
                                                String line = receiver.read(buffer);
                                                writer.write(line);
                                                writer.newLine();
                                                logger.trace("read " + line);
                                                if (buffer.hasRemaining()) {
                                                    buffer.compact();
                                                } else {
                                                    buffer.clear();
                                                }
                                            } else if (!process) {
                                                logger.trace("stopped");
                                                break;
                                            }
                                        }
                                    }
                                    try {
                                         batchEnd.await();                                     logger.debug(name + " e "+ batchStart.getParties() + " "+ batchStart.getNumberWaiting());

                                        process = true;
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    } catch (BrokenBarrierException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        } catch (IOException ex) {
                            logger.error("consumer", ex);
                            logger.error(ex.getMessage());
                        }
                    }
                }
                logger.info("done");
            } else {
                logger.warn("socket channel or selector cannot be opened");
            }
        } catch (IOException ex) {
            logger.error("consumer", ex);
        }
    }

    public String getName(){
        return this.name;
    }
}
