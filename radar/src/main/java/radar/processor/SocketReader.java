package radar.processor;

import radar.message.ByteConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Set;

/**
 * from socket to file
 */
public class SocketReader implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SocketReader.class);

    private final InetSocketAddress address;
    private final Path dir;
    private final ByteConverter receiver;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    private volatile boolean process = true;
    private SocketChannel socketChannel;
    private SocketChannel keySocketChannel;
    private Selector selector;

    public SocketReader(InetSocketAddress src, Path dst, ByteConverter receiver) {
        this.receiver = receiver;
        this.address = src;
        this.dir = dst;
    }

    @Override
    public void start() {
        try {
            socketChannel = SocketChannel.open();
            selector = Selector.open();
            if ((socketChannel.isOpen()) && (selector.isOpen())) {
                socketChannel.configureBlocking(false);
                socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
                socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                socketChannel.register(selector, SelectionKey.OP_CONNECT);
                socketChannel.connect(address);
                if (selector.select(10000) > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator its = keys.iterator();
                    if (its.hasNext()) {
                        SelectionKey key = (SelectionKey) its.next();
                        its.remove();
                        keySocketChannel = (SocketChannel) key.channel();
                        if (key.isConnectable()) {
                            if (keySocketChannel.isConnectionPending()) {
                                keySocketChannel.finishConnect();
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void signalStepEnd() {
        process = false;
    }

    @Override
    public void end() {
        if (selector != null) {
            try {
                selector.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (keySocketChannel != null) {
            if (keySocketChannel.isConnected()) {
                try {
                    keySocketChannel.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        if (socketChannel != null) {
            if (socketChannel.isConnected()) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Read from socket and convert to file
     */
    @Override
    public void step(Path step) {
        Path path = Paths.get(dir + File.separator + step);
        if (Files.notExists(path.getParent())) {
            try {
                Files.createDirectories(path.getParent());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        process = true;
        try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.CREATE)) {
            logger.trace("convert started");
            while (keySocketChannel.read(buffer) != -1) {
                if (buffer.position() > 0) {
                    buffer.flip();
                    String line = receiver.convert(buffer);
                    writer.write(line);
                    writer.newLine();
                    logger.trace("convert " + line);
                    if (buffer.hasRemaining()) {
                        buffer.compact();
                    } else {
                        buffer.clear();
                    }
                } else if (!process) {
                    logger.trace("convert stopped");
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
