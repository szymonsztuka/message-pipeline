package messagepipeline.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.content.MessageGenerator;

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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class PushProducer implements Runnable, LeafNode {

    private static final Logger logger = LoggerFactory.getLogger(PushProducer.class);
    private final List<MessageGenerator> generators;
    private volatile boolean done = false;
    private final InetSocketAddress address;
    private final List<Path> paths;
    private final boolean sendAtTimestamps;
    final private CyclicBarrier batchStart;
    final private CyclicBarrier batchEnd;

    final private CyclicBarrier internalBatchStart;
    final private CyclicBarrier internalBatchEnd;

    public PushProducer(String directory, List<String> messagePaths, List<MessageGenerator> messageGenerators, InetSocketAddress address, boolean sendAtTimestamps, CyclicBarrier batchStart, CyclicBarrier batchEnd) {
        final Path outputDir = Paths.get(directory);
        this.paths = messagePaths.stream().map(s -> Paths.get(outputDir + s)).collect(Collectors.toList());
        this.generators = messageGenerators;
        this.address = address;
        this.sendAtTimestamps = sendAtTimestamps;
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.internalBatchStart = new CyclicBarrier(this.generators.size() + 1);
        this.internalBatchEnd = new CyclicBarrier(this.generators.size() + 1);
    }

    public void run() {
        List<SubProducer> threads = new ArrayList<>();
        List<Thread> realThreads = new ArrayList<>();
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            //logger.info("Producer open");
            if (serverSocketChannel.isOpen()) {
                serverSocketChannel.configureBlocking(true);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(address);
                logger.info(generators.size() + " connections available on " + address.toString());
                for (int i = 0; i < generators.size(); i++) {
                    try {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        logger.info("connection " + socketChannel.getLocalAddress() + " <- " + socketChannel.getRemoteAddress());
                        SubProducer subProducer = new SubProducer(socketChannel, paths, generators.get(i), internalBatchStart, internalBatchEnd);
                        threads.add(subProducer);
                        Thread subThread = new Thread(subProducer);
                        subThread.setName("producer_" + i);
                        subThread.start();
                        realThreads.add(subThread);
                    } catch (IOException ex) {
                        logger.error("cannot read data", ex);
                    }
                }
            } else {
                logger.warn("server socket channel cannot be opened");
            }
            while (!allDone(threads)) {
                try {
                    batchStart.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                try {
                    internalBatchStart.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                try {
                    internalBatchEnd.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                try {
                    batchEnd.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
            serverSocketChannel.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        done = true;
        logger.info("done");
    }

    public boolean isDone() {
        return done;
    }

    private boolean allDone(List<SubProducer> threads) {
        boolean reread;
        boolean result = true;
        do {
            reread = false;
            try {
                for (SubProducer prod : threads) {
                    if (!prod.internalDone) {
                        result = false;
                    }
                }
            } catch (ConcurrentModificationException e) {
                reread = true;
            }
        } while (reread);
        return result;
    }

    class SubProducer implements Runnable {
        private final SocketChannel socketChannel;
        private final List<Path> paths;
        final private MessageGenerator generator;
        final private CyclicBarrier internalBatchStart;
        final private CyclicBarrier internalBatchEnd;
        volatile boolean internalDone = false;

        public SubProducer(SocketChannel socketChannel, List<Path> readerPaths, MessageGenerator messageGenerator,
                           CyclicBarrier internalBatchStart, CyclicBarrier internalBatchEnd) {
            this.paths = readerPaths;
            this.generator = messageGenerator;
            this.socketChannel = socketChannel;
            this.internalBatchStart = internalBatchStart;
            this.internalBatchEnd = internalBatchEnd;
        }

        public void run() {
            String line;
            ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
            Iterator<Path> pathIt = paths.iterator();
            for (Path path : paths) {
                logger.trace("Producer opening " + path);
                try {
                    Thread.sleep(1000 * 3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    internalBatchStart.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }

                try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                    //logger.trace("Producer receiving " + path);
                    while ((line = reader.readLine()) != null) {
                        if (line.length() > 0) {
                            try {
                                generator.write(line, buffer, sendAtTimestamps);
                                buffer.flip();
                                socketChannel.write(buffer);
                                if (buffer.remaining() > 0) {
                                    //System.out.println("! remaining " + buffer.remaining() + " " + buffer.limit() + " " + buffer.position());
                                }
                                buffer.clear();
                            } catch (BufferOverflowException ex) {
                                logger.error("SubProducer error", ex);
                            }
                        }
                    }
                } catch (IOException ex) {
                    logger.error("cannot read data ", ex);
                }
                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                     if(!pathIt.hasNext()) {
                         internalDone = true;
                     }
                     internalBatchEnd.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                generator.resetSequencNumber();
            }
            try {
                logger.info("SubProducer closing socket");
                socketChannel.close();
            } catch (Exception e) {
                logger.error("SubProducer error", e);
            }
            internalDone = true;
            logger.info("SubProducer closing");
        }
    }
}
