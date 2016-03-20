package messagepipeline.pipeline.node;

import messagepipeline.message.MessageReceiver;
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
import java.util.Set;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(PullConsumer.class);
    public final InetSocketAddress address;
    private volatile boolean process = true;
    MessageReceiver receiver;

    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

    SocketChannel socketChannel;
    SocketChannel keySocketChannel ;
    Selector selector;

    public Client(InetSocketAddress address,  MessageReceiver receiver) {
        this.receiver = receiver;
       this.address = address;
    }

    public void finishStep() {
        process = false;
    }

    public void connect() {

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
                                System.out.println(keySocketChannel.getLocalAddress());
                                System.out.println(keySocketChannel.getRemoteAddress());
                                keySocketChannel.finishConnect();
                            }
                        }
                    }
                }
            }  } catch (IOException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);

        }
    }

    public void close(){
        if(selector != null) {
            try {
                selector.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error( e.getMessage(), e);
            }
        }
        if(keySocketChannel != null){
            if(keySocketChannel.isConnected()) {
                try {
                    keySocketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
            }
        }
        if(socketChannel != null){
            if(socketChannel.isConnected()) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
            }
        }
    }

    public void read(Path path) {
        process = true;
        try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.CREATE)) {
            logger.debug("read started");
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
                    logger.debug("read stopped");
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        }

    }
}
