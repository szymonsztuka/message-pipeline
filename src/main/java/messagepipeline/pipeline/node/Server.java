package messagepipeline.pipeline.node;

import messagepipeline.message.MessageGenerator;
import messagepipeline.message.MessageReceiver;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final List<MessageGenerator> generators;
    //private volatile boolean done = false;
    private final InetSocketAddress address;

    private final boolean sendAtTimestamps;


    final private CyclicBarrier internalBatchStart;
    final private CyclicBarrier internalBatchEnd;
    //final private String name;
    //private final Path inputDir;

    List<SubProducer> threads = new ArrayList<>();
    List<Thread> realThreads = new ArrayList<>();
    ServerSocketChannel serverSocketChannel = null;
    List<SocketChannel> sockets = new ArrayList<>(1);
    public Server( List<MessageGenerator> messageGenerators, InetSocketAddress address, boolean sendAtTimestamps) {
        //this.inputDir = Paths.get(directory);
        //this.paths = messagePaths.stream().map(s -> Paths.get(this.inputDir + File.separator + s)).collect(Collectors.toList());
        this.generators = messageGenerators;
        this.address = address;
        this.sendAtTimestamps = sendAtTimestamps;
        //this.batchStart = batchStart;
       // this.batchEnd = batchEnd;
        this.internalBatchStart = new CyclicBarrier(this.generators.size() + 1);
        this.internalBatchEnd = new CyclicBarrier(this.generators.size() + 1);
        //this.name = name;
    }

    public void connect() {
        logger.debug("connect");
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
                    //logger.debug("source " + inputDir.toString() + ", destination " + socketChannel.getLocalAddress() + " <- " + socketChannel.getRemoteAddress());
                    SubProducer subProducer = new SubProducer(socketChannel, generators.get(i), internalBatchStart, internalBatchEnd);
                    threads.add(subProducer);
                    Thread subThread = new Thread(subProducer);
                    //subThread.setName(name + "_" + i);
                    subThread.start();
                    realThreads.add(subThread);
                } catch (IOException ex) {
                    logger.error("cannot read data", ex);
                }
            }
        } else {
            logger.warn("server socket channel cannot be opened");
        }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        }
    }
    public void close(){
        for(SubProducer p : threads){
            p.run = false;
        }
        try {
            logger.debug("is " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting()+" closing ");
            internalBatchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        }
        logger.debug("close");
        for(SocketChannel socket: sockets) {
            if (socket != null) {
                if (socket.isConnected()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error( e.getMessage(), e);
                    }
                }
            }
        }
        if(serverSocketChannel != null) {
            try {
                serverSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error( e.getMessage(), e);
            }
        }
    }

    public void write(Path path) {
        for(SubProducer p : threads){
            p.path = path;
        }
        try {
            logger.debug("is " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting()+" "+path.toString());
            internalBatchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        }


        try {
            logger.debug("ie " + internalBatchEnd.getParties() + " " + internalBatchEnd.getNumberWaiting());
            internalBatchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        }
    }

    class SubProducer implements Runnable {
        private final SocketChannel socketChannel;
        final private MessageGenerator generator;
        final private CyclicBarrier internalBatchStart;
        final private CyclicBarrier internalBatchEnd;
        volatile boolean run = true;
        volatile Path path;

        public SubProducer(SocketChannel socketChannel, MessageGenerator messageGenerator,
                           CyclicBarrier internalBatchStart, CyclicBarrier internalBatchEnd) {
            this.generator = messageGenerator;
            this.socketChannel = socketChannel;
            this.internalBatchStart = internalBatchStart;
            this.internalBatchEnd = internalBatchEnd;
        }

        public void run() {
            String line;
            ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
            while(run) {
                logger.trace("Producer opening " + path);
                try {
                    Thread.sleep(1000 * 3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
                try {
                    logger.debug("iis " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting()+ " "+(path!=null?path.toString():null));
                    internalBatchStart.await();
                    logger.debug("iis-passed " + internalBatchStart.getParties() + " " + internalBatchStart.getNumberWaiting()+ " "+(path!=null?path.toString():null));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
                try {
                    Thread.sleep(1000 * 3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
                if(!run) return;
                try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                    while ((line = reader.readLine()) != null) {
                        if (line.length() > 0) {
                            try {
                                logger.trace("Producer sends   " + line);
                                generator.write(line, buffer, sendAtTimestamps);
                                buffer.flip();
                                socketChannel.write(buffer);
                                if (buffer.remaining() > 0) {
                                    //System.out.println("! remaining " + buffer.remaining() + " " + buffer.limit() + " " + buffer.position());
                                }
                                buffer.clear();
                            } catch (BufferOverflowException ex) {
                                logger.error("error", ex);
                            }
                        }
                    }
                } catch (IOException ex) {
                    logger.error("cannot write data ", ex);
                }
                path = null;
                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
                try {
                    logger.debug("iie " + internalBatchEnd.getParties() + " " + internalBatchEnd.getNumberWaiting());
                    internalBatchEnd.await();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                    logger.error( e.getMessage(), e);
                }
                generator.resetSequencNumber();



            }
            logger.info("closed");
        }
    }
}
