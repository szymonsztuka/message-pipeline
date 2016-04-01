package radar.node;

import radar.message.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SocketWriter implements Node {

    private static final Logger logger = LoggerFactory.getLogger(SocketWriter.class);

    private final List<Encoder> generators;
    private final InetSocketAddress address;
    private final Path dir;
    private final CyclicBarrier writerBatchStart;
    private final CyclicBarrier writerBatchEnd;
    private final List<SubProducer> writers = new ArrayList<>();
    private final List<Thread> writerThreads = new ArrayList<>();
    private ServerSocketChannel serverSocketChannel;
    private final List<SocketChannel> sockets = new ArrayList<>(1);

    public SocketWriter(Path src, InetSocketAddress dst, List<Encoder> encoders) {

        this.generators = encoders;
        this.dir = src;
        this.address = dst;
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
                        sockets.add(socketChannel);
                        SubProducer subProducer = new SubProducer(socketChannel, generators.get(i), writerBatchStart, writerBatchEnd);
                        writers.add(subProducer);
                        Thread subThread = new Thread(subProducer);
                        subThread.setName(Thread.currentThread().getName()+ "-" + i); //optional
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
        writers.forEach(t ->  t.process = false );
        try {
            logger.debug("is (e) " + writerBatchStart.getParties() + " " + writerBatchStart.getNumberWaiting());
            writerBatchStart.await();
            logger.debug("is (e) - passed " + writerBatchStart.getParties() + " " + writerBatchStart.getNumberWaiting());
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        sockets.forEach( e -> {
            try {
                e.close();
            } catch(IOException ex){
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
        writers.forEach(p -> p.path = path );
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

    class SubProducer implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(SubProducer.class);

        private final SocketChannel socketChannel;
        private final Encoder generator;
        private final CyclicBarrier internalBatchStart;
        private final CyclicBarrier internalBatchEnd;
        volatile boolean process = true;
        volatile Path path;

        public SubProducer(SocketChannel socketChannel, Encoder encoder,
                           CyclicBarrier internalBatchStart,  CyclicBarrier internalBatchEnd) {
            this.generator = encoder;
            this.socketChannel = socketChannel;
            this.internalBatchStart = internalBatchStart;
            this.internalBatchEnd = internalBatchEnd;
        }

        public void run() {
            String line;
            ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
            while (true) {
                logger.trace("Producer opening " + path);
                try {
                    logger.debug("is " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                    internalBatchStart.await();
                    logger.debug("is - passed " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting() + " " + (path != null ? path.toString() : null) + " " + process);
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage(), e);
                }
                if (!process) {
                    logger.info("End");
                    return;
                }
                try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                    while ((line = reader.readLine()) != null) {
                        if (line.length() > 0) {
                            try {
                                logger.trace("Producer sends   " + line);
                                generator.write(line, buffer);
                                buffer.flip();
                                socketChannel.write(buffer);
                                //if (buffer.remaining() > 0) { System.out.println("! remaining " + buffer.remaining() + " " + buffer.limit() + " " + buffer.position()); }
                                buffer.clear();
                            } catch (BufferOverflowException ex) {
                                logger.error("error", ex);
                            }
                        }
                    }
                } catch (IOException ex) {
                    logger.error("cannot write data ", ex);
                }
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
