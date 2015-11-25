package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MultiProducer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MultiProducer.class);
    final List<MessageGenerator> generators;
    final private CountDownLatch done;
    final private InetSocketAddress address;
    Path path;
    final private int noClients ;
    final private boolean sendAtTimestamps;
    public MultiProducer(CountDownLatch latch, Path readerPath, List<MessageGenerator> msgProducers, InetSocketAddress address, int noClients, boolean sendAtTimestamps) {
        done = latch;
        path = readerPath;
        generators = msgProducers;
        this.address = address;
        this.noClients = noClients > 0 ? noClients : 1;
        this.sendAtTimestamps = sendAtTimestamps;
    }

    public void run() {
        List<SubProducer> threads = new ArrayList<>();
        List<Thread> realThreads = new ArrayList<>();
    	logger.info("Multi Producer opening for "+ noClients +" clients");
        try { 
        	ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();  logger.info("Producer open");
        
            if (serverSocketChannel.isOpen()) {  logger.info("Producer is open");
                serverSocketChannel.configureBlocking(true);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(address); logger.info("Producer accepting on " + address.toString());
                for (int i =0 ; i <noClients; i++) {
                //while(true && !allDone(threads)) {
                    try{ 
                    	SocketChannel socketChannel = serverSocketChannel.accept();              
                        logger.info("Producer connected " + socketChannel.getLocalAddress() + " <- " + socketChannel.getRemoteAddress());
                        SubProducer subProducer = new SubProducer(socketChannel, path, generators.get(i));
                        threads.add(subProducer);                
                        Thread subThread = new Thread(subProducer);
                        subThread.start();
                        realThreads.add(subThread);
                    } catch (IOException ex) {
                        logger.error("Producer cannot read data ", ex);
                    }
                }
            } else {
                logger.warn("The server socket channel cannot be opened!");
            }
           
            for(Thread x: realThreads){
            	System.out.println("Awaiting "+ x.toString());
            	try {
					x.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            serverSocketChannel.close();
            
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        try {
            Thread.sleep(1000 * 2);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        done.countDown();
        logger.info("Producer is done! " + done.getCount());
    }
    }

  /*  private boolean allDone(List<SubProducer> threads){

        boolean result = threads.size() > 0;
        try {
            for (SubProducer prod : threads) {
                if (!prod.done) {
                    result = false;
                }
            }
        } catch(ConcurrentModificationException e ){
            result = false;
        }
        return result;
    }*/

    class SubProducer implements Runnable {
        private final SocketChannel socketChannel;
        private final Path path;
        final private MessageGenerator generator;
    public volatile boolean done = false;
        public SubProducer(SocketChannel socketChannel,  Path readerPath, MessageGenerator messageGenerator) {
            //done = latch;
            this.path = readerPath;
            this.generator = messageGenerator;
            this.socketChannel = socketChannel;
        }

        public void run() {
            logger.info("SubProducer opening");
            String line;
            ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
            try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
                logger.info("Producer sending " + path);
                while ((line = reader.readLine()) != null) {
                    if(line.length()>0) {
                        try {
                            generator.write(line, buffer, sendAtTimestamps);
                            buffer.flip();
                            socketChannel.write(buffer);
                            buffer.clear();
                        }catch(BufferOverflowException ex){
                            logger.error("Producer error", ex);
                        }
                    }
                }
            }  catch (IOException ex) {
            logger.error("Producer cannot read data ", ex);
        }
            
            try {
				socketChannel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            done = true;
        }
    }


}
