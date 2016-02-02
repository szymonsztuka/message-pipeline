package serverchainsimulator.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serverchainsimulator.control.SelfStoppable;
import serverchainsimulator.content.MessageGenerator;

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
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class PushProducer implements Runnable, SelfStoppable {

    private static final Logger logger = LoggerFactory.getLogger(PushProducer.class);
    final private List<MessageGenerator> generators;
    private volatile boolean done = false;
    final private InetSocketAddress address;
    List<Path> paths;
    final private int noClients;
    final private boolean sendAtTimestamps;

    final private CyclicBarrier batchStart;
    final private CyclicBarrier batchEnd;

    final private CyclicBarrier internalBatchStart;
    final private CyclicBarrier internalBatchEnd;

    public PushProducer(List<Path> readerPaths, List<MessageGenerator> messageGenerators, InetSocketAddress address, int noClients, boolean sendAtTimestamps, CyclicBarrier batchStart, CyclicBarrier batchEnd) {
        paths = readerPaths;
        generators = messageGenerators;
        this.address = address;
        this.noClients = noClients > 0 ? noClients : 1;
        this.sendAtTimestamps = sendAtTimestamps;
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.internalBatchStart = new CyclicBarrier(this.noClients + 1);
        this.internalBatchEnd = new CyclicBarrier(this.noClients + 1);
    }

    public void run() {
        List<SubProducer> threads = new ArrayList<>();
        List<Thread> realThreads = new ArrayList<>();

        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            //logger.info("DepracetedProducer open");

            if (serverSocketChannel.isOpen()) {
                serverSocketChannel.configureBlocking(true);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(address);
                logger.info(noClients + " connections available on " + address.toString());
                for (int i = 0; i < noClients; i++) {
                    try {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        logger.info("connection " + socketChannel.getLocalAddress() + " <- " + socketChannel.getRemoteAddress());
                        SubProducer subProducer = new SubProducer(socketChannel, paths, generators.get(i), internalBatchStart, internalBatchEnd);
                        threads.add(subProducer);
                        Thread subThread = new Thread(subProducer);
                        subThread.start();
                        realThreads.add(subThread);
                    } catch (IOException ex) {
                        logger.error("DepracetedProducer cannot read data ", ex);
                    }
                }
            } else {
                logger.warn("The server socket channel cannot be opened!");
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

            /*for (Thread x : realThreads) {
                System.out.println("Awaiting " + x.toString());
                try {
                    x.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }*/
            serverSocketChannel.close();
            done = true;
            logger.info("DepracetedProducer is done! ");
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
            try {
                Thread.sleep(1000 * 2);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            done = true;
            logger.info("DepracetedProducer is done! ");
        }
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
            //while(pathIt.hasNext()) {
            //   Path path = pathIt.next();
            for (Path path : paths) {
                logger.trace("DepracetedProducer opening " + path);
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
                    //logger.trace("DepracetedProducer receiving " + path);
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
                                logger.error("DepracetedProducer error", ex);
                            }
                        }
                    }
                } catch (IOException ex) {
                    logger.error("DepracetedProducer cannot read data ", ex);
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
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            internalDone = true;
            logger.info("SubProducer closing");
        }
    }
}
