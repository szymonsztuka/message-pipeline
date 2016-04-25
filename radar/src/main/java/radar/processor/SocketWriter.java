package radar.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radar.message.StringConverter;
import radar.message.Reader;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SocketWriter implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SocketWriter.class);

    private final Path dir;
    private final InetSocketAddress address;
    private final List<Reader> readers;
    private final List<StringConverter> stringConverters;
    private final CyclicBarrier internalStepStart;
    private final CyclicBarrier internalStepEnd;
    private final List<InternalSocketWriter> writers = new ArrayList<>();
    private final List<Thread> writerThreads = new ArrayList<>();
    private ServerSocketChannel serverSocketChannel;
    private final List<SocketChannel> sockets = new ArrayList<>(1);

    public SocketWriter(Path src, InetSocketAddress dst, List<Reader> readers, List<StringConverter> stringConverters) {

        this.dir = src;
        this.address = dst;
        this.readers = readers;
        this.stringConverters = stringConverters;
        this.internalStepStart = new CyclicBarrier(this.stringConverters.size() + 1);
        this.internalStepEnd = new CyclicBarrier(this.stringConverters.size() + 1);
    }

    @Override
    public void start() {
        logger.trace("connect");
        try {
            serverSocketChannel = ServerSocketChannel.open();
            if (serverSocketChannel.isOpen()) {
                serverSocketChannel.configureBlocking(true);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(address);
                logger.trace(stringConverters.size() + " connections available on " + address.toString());
                for (int i = 0; i < stringConverters.size(); i++) {
                    try {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        sockets.add(socketChannel);
                        InternalSocketWriter internalSocketWriter = new InternalSocketWriter(socketChannel, readers.get(i), stringConverters.get(i), internalStepStart, internalStepEnd); //TODO below should be factory invocation
                        writers.add(internalSocketWriter);
                        Thread subThread = new Thread(internalSocketWriter);
                        subThread.setName(Thread.currentThread().getName() + "-" + i); //optional
                        subThread.start();
                        writerThreads.add(subThread);
                    } catch (IOException e) {
                        logger.error("cannot read data", e);
                    }
                }
            } else {
                logger.warn("server socket channel cannot be opened");
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void signalStepEnd() {

    }

    @Override
    public void end() {
        writers.forEach(t -> t.process = false);
        try {
            logger.debug("is (e) " + internalStepStart.getParties() + " " + internalStepStart.getNumberWaiting());
            internalStepStart.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
        sockets.forEach(s -> {
            try {
                s.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        });
        if (serverSocketChannel != null) {
            try {
                serverSocketChannel.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Read from file and write onto socket
     */
    @Override
    public void step(Path step) {
        Path path = Paths.get(dir + File.separator + step);
        writers.forEach(p -> p.path = path);
        try {
            logger.debug("is " + internalStepStart.getParties() + " " + internalStepStart.getNumberWaiting() + " " + path.toString());
            internalStepStart.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
        try {
            logger.debug("ie " + internalStepEnd.getParties() + " " + internalStepEnd.getNumberWaiting() + " " + path.toString());
            internalStepEnd.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
    }

    class InternalSocketWriter implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(InternalSocketWriter.class);

        private final SocketChannel socketChannel;
        private final Reader reader;
        private final StringConverter stringConverter;
        private final CyclicBarrier internalStepStart;
        private final CyclicBarrier internalStepEnd;
        volatile boolean process = true;
        volatile Path path;

        public InternalSocketWriter(SocketChannel socketChannel, Reader reader, StringConverter stringConverter,
                                    CyclicBarrier internalStepStart, CyclicBarrier internalStepEnd) {

            this.socketChannel = socketChannel;
            this.reader = reader;
            this.stringConverter = stringConverter;
            this.internalStepStart = internalStepStart;
            this.internalStepEnd = internalStepEnd;
        }

        public void run() {
            ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
            while (true) {
                logger.trace("producer opening " + path);
                try {
                    logger.debug("is " + internalStepStart.getParties() + " " + internalStepStart.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                    internalStepStart.await();
                    logger.debug("is - passed " + internalStepStart.getParties() + " " + internalStepStart.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                } catch (InterruptedException | BrokenBarrierException e) {
                    logger.error(e.getMessage(), e);
                }
                if (!process) {
                    logger.info("end");
                    return;
                }
                reader.open(path);
                String message;
                while ((message = reader.readMessage()) != null) {
                    try {
                        stringConverter.convert(message, buffer);
                        buffer.flip();
                        socketChannel.write(buffer);
                        //if (buffer.remaining() > 0) { System.out.println("! remaining " + buffer.remaining() + " " + buffer.limit() + " " + buffer.position()); }
                        buffer.clear();
                    } catch (BufferOverflowException | IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                reader.close();
                try {
                    logger.debug("ie " + internalStepEnd.getParties() + " " + internalStepEnd.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                    internalStepEnd.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
