package radar.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radar.message.Encoder;
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

public class SocketWriter<T> implements Node {

    private static final Logger logger = LoggerFactory.getLogger(SocketWriter.class);

    private final List<Encoder<T>> generators;
    private final List<Reader<T>> readers;
    private final InetSocketAddress address;
    private final Path dir;
    private final CyclicBarrier writerBatchStart;
    private final CyclicBarrier writerBatchEnd;
    private final List<SubProducer> writers = new ArrayList<>();
    private final List<Thread> writerThreads = new ArrayList<>();
    private ServerSocketChannel serverSocketChannel;
    private final List<SocketChannel> sockets = new ArrayList<>(1);

    public SocketWriter(Path src, InetSocketAddress dst, List<Reader<T>> readers, List<Encoder<T>> encoders) {

        this.dir = src;
        this.address = dst;
        this.readers = readers;
        this.generators = encoders;
        this.writerBatchStart = new CyclicBarrier(this.generators.size() + 1);
        this.writerBatchEnd = new CyclicBarrier(this.generators.size() + 1);
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
                logger.trace(generators.size() + " connections available on " + address.toString());
                for (int i = 0; i < generators.size(); i++) {
                    try {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        sockets.add(socketChannel); //TODO below should be factory invocation
                        SubProducer subProducer = new SubProducer(socketChannel, readers.get(i), generators.get(i), writerBatchStart, writerBatchEnd);
                        writers.add(subProducer);
                        Thread subThread = new Thread(subProducer);
                        subThread.setName(Thread.currentThread().getName() + "-" + i); //optional
                        subThread.start();
                        writerThreads.add(subThread);
                    } catch (IOException ex) {
                        logger.error("cannot read data", ex);
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
            logger.debug("is (e) " + writerBatchStart.getParties() + " " + writerBatchStart.getNumberWaiting());
            writerBatchStart.await();
            logger.debug("is (e) - passed " + writerBatchStart.getParties() + " " + writerBatchStart.getNumberWaiting());
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        sockets.forEach(e -> {
            try {
                e.close();
            } catch (IOException ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage(), ex);
            }
        });
        if (serverSocketChannel != null) {
            try {
                serverSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
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
            logger.debug("is " + writerBatchStart.getParties() + " " + writerBatchStart.getNumberWaiting() + " " + path.toString());
            writerBatchStart.await();
            logger.debug("is - passed " + writerBatchStart.getParties() + " " + writerBatchStart.getNumberWaiting() + " " + path.toString());
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        try {
            logger.debug("ie " + writerBatchEnd.getParties() + " " + writerBatchEnd.getNumberWaiting() + " " + path.toString());
            writerBatchEnd.await();
            logger.debug("ie - passed " + writerBatchEnd.getParties() + " " + writerBatchEnd.getNumberWaiting() + " " + path.toString());
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
    }

    class SubProducer<T> implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(SubProducer.class);

        private final SocketChannel socketChannel;
        private final Reader<T> reader;
        private final Encoder<T> generator;
        private final CyclicBarrier internalBatchStart;
        private final CyclicBarrier internalBatchEnd;
        volatile boolean process = true;
        volatile Path path;

        public SubProducer(SocketChannel socketChannel, Reader<T> reader, Encoder<T> encoder,
                           CyclicBarrier internalBatchStart, CyclicBarrier internalBatchEnd) {

            this.socketChannel = socketChannel;
            this.reader = reader;
            this.generator = encoder;
            this.internalBatchStart = internalBatchStart;
            this.internalBatchEnd = internalBatchEnd;
        }

        public void run() {
            ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
            while (true) {
                logger.trace("producer opening " + path);
                try {
                    logger.debug("is " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                    internalBatchStart.await();
                    logger.debug("is - passed " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage(), e);
                }
                if (!process) {
                    logger.info("end");
                    return;
                }
                reader.open(path);
                T message;
                while ((message = reader.readMessage()) != null) {
                    try {
                        generator.write(message, buffer);
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
                    logger.debug("ie " + internalBatchEnd.getParties() + " " + internalBatchEnd.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                    internalBatchEnd.await();
                    logger.debug("ie - passed " + internalBatchEnd.getParties() + " " + internalBatchEnd.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
