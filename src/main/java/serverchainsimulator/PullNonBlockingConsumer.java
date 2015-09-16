package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
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
import java.util.Set;

/**
 *
 */
public class PullNonBlockingConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PullNonBlockingConsumer.class);
    
    public final InetSocketAddress adress;
    volatile private boolean process = true;
    private final Path path;
    private final PullMessageReceiver receiver;
    
    private int readCount;

    public PullNonBlockingConsumer(Path writerPath, PullMessageReceiver messageReceiver, InetSocketAddress adress) {
        path = writerPath;
        receiver = messageReceiver;
        this.adress = adress;
    }

    public void terminate() {
        process = false;
    }

    @SuppressWarnings("rawtypes")
    public void run() {


        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        logger.info("Consumer opening, path " + path +" " +adress);


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
                                logger.info("Consumer connecting " + path);
                                if (keySocketChannel.isConnectionPending()) {
                                    keySocketChannel.finishConnect();
                                }
                                logger.info("Consumer connected " + path);
                                if (Files.notExists(path.getParent())) {
                                    Files.createDirectories(path.getParent());
                                }
                                try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.CREATE)) {
                                	boolean firstMessage  = true;
                                	ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                    while (keySocketChannel.read(buffer) != -1) {
                                        if (buffer.position() > 0) {
                                            buffer.flip();
                                            boolean more = receiver.read(buffer, firstMessage, baos );
                                            logger.info(""+more);
                                            if(!more) {
                                            	String str = receiver.get(baos);
                                            	writer.write(str);
                                            	writer.write("\n");
                                            	logger.info("-> " + str);
                                            	if (logger.isTraceEnabled()) {
                                            		logger.trace("Read " +str);
                                            	}
                                            	readCount++;
                                            	baos.reset();
                                            	firstMessage = true;
                                            } else {
                                            	firstMessage = false;
                                            }
                                            if (buffer.hasRemaining()) {
                                                buffer.compact();
                                            } else {
                                                buffer.clear();
                                            }
                                        } else if (!process) {
                                            logger.info("Consumer says good bye;");
                                            return;
                                        }
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
