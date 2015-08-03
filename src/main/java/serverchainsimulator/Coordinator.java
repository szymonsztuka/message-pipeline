package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final String producerInputDirectory;
    private final String consumerOutputDirectory;
    private final Integer producerPort;
    private final String producerHostIp;
    private final Integer consumerPort;
    private final String consumerHostIp;
    private final String bootstrapMainClass;
    private final String bootstrapClasspath;
    private final String[] bootstrapJvmArguments;
    private final String[] bootstrapProgramArguments;
    private final boolean restartServerAfterEachFile;


    /**
     * @param args path to configuration file
     */
    public static void main(String[] args) {

        if (args.length < 1) {
            throw new IllegalArgumentException();
        }
        final Coordinator coordinator = new Coordinator(args[0]);
        coordinator.run();

    }

    final Properties configProp;

    public Coordinator(String propertiesPath) {
        configProp = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(propertiesPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            configProp.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        producerInputDirectory = configProp.getProperty("inputDirectory");
        consumerOutputDirectory = configProp.getProperty("outputDirectory");
        producerPort = Integer.parseInt(configProp.getProperty("producerPort"));
        consumerPort = Integer.parseInt(configProp.getProperty("consumerPort"));
        bootstrapMainClass = configProp.getProperty("bootstrapMainClass");
        bootstrapClasspath = configProp.getProperty("bootstrapClasspath");
        bootstrapJvmArguments = configProp.getProperty("bootstrapJvmArguments").split(" ");
        bootstrapProgramArguments = configProp.getProperty("bootstrapProgramArguments").split(" ");
        producerHostIp = configProp.getProperty("producerHostIp", "127.0.0.1");
        consumerHostIp = configProp.getProperty("consumerHostIp", "127.0.0.1");
        restartServerAfterEachFile = Boolean.parseBoolean(configProp.getProperty("restartServerAfterEachFile", "true"));
    }
    public void run() {
        if(restartServerAfterEachFile) {
            statelessBootstrap();
        } else {
            statefullBootstrap();
        }

    }

    public void statelessBootstrap() {

        final List<Path> writerFileNames = new ArrayList<Path>();
        Path inputDir = Paths.get(producerInputDirectory);
        RecuriveFileCollector walk= new RecuriveFileCollector();
        try {
            Files.walkFileTree(inputDir, walk);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        Path outputDir = Paths.get(consumerOutputDirectory);
        int x = outputDir.getNameCount();
        final List<Path> readerFileNames = walk.getResult();
        for(Path path: readerFileNames) {
            Path outputSubpath = path.subpath(x, path.getNameCount());
            Path newOne = outputDir.resolve(outputSubpath);
            readerFileNames.add(newOne);
        }

        Iterator<Path> readerIt = readerFileNames.iterator();
        Iterator<Path> writerIt = writerFileNames.iterator();

        while (readerIt.hasNext() && writerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            Producer producer = new Producer(done, readerIt.next(), new SimpleMessageGenerator(), producerHostIp, producerPort);
            NonBlockingConsumer consumer = new NonBlockingConsumer(writerIt.next(), new SimpleMessageReceiver(), consumerHostIp, consumerPort);
            Bootstrap bootstrap = new Bootstrap(done, bootstrapClasspath, bootstrapJvmArguments, bootstrapMainClass, bootstrapProgramArguments);

            Thread producerThread = new Thread(producer);
            Thread consumerThread = new Thread(consumer);
            Thread bootstrapThread = new Thread(bootstrap);

            bootstrapThread.start();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumerThread.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producerThread.start();


            try {
                done.await();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            try {
                producerThread.join();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.info("Terminating consumer!");
            consumer.terminate();
            logger.info("Awaiting join consumer!");
            try {
                consumerThread.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            logger.info("Awaiting join bootsrap!");
            try {
                bootstrapThread.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        logger.info("All done!");
    }

    public void statefullBootstrap() {

        final List<Path> writerFileNames = new ArrayList<Path>();
        Path inputDir = Paths.get(producerInputDirectory);
        RecuriveFileCollector walk= new RecuriveFileCollector();
        try {
            Files.walkFileTree(inputDir, walk);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        Path outputDir = Paths.get(consumerOutputDirectory);
        int x = outputDir.getNameCount();
        final List<Path> readerFileNames = walk.getResult();
        for(Path path: readerFileNames) {
            Path outputSubpath = path.subpath(x, path.getNameCount());
            Path newOne = outputDir.resolve(outputSubpath);
            readerFileNames.add(newOne);
        }

        Iterator<Path> readerIt = readerFileNames.iterator();
        Iterator<Path> writerIt = writerFileNames.iterator();

        CountDownLatch turnOff = new CountDownLatch(1);
        Bootstrap bootstrap = new Bootstrap(turnOff, bootstrapClasspath, bootstrapJvmArguments, bootstrapMainClass, bootstrapProgramArguments);
        Thread bootstrapThread = new Thread(bootstrap);

        bootstrapThread.start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        while (readerIt.hasNext() && writerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            Producer producer = new Producer(done, readerIt.next(), new SimpleMessageGenerator(), producerHostIp, producerPort);
            NonBlockingConsumer consumer = new NonBlockingConsumer(writerIt.next(), new SimpleMessageReceiver(), consumerHostIp, consumerPort);

            Thread producerThread = new Thread(producer);
            Thread consumerThread = new Thread(consumer);
            consumerThread.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producerThread.start();


            try {
                done.await();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            try {
                producerThread.join();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.info("Terminating consumer!");
            consumer.terminate();
            logger.info("Awaiting join consumer!");
            try {
                consumerThread.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }


        }
        turnOff.countDown();
        logger.info("Awaiting join bootsrap!");
        try {
            bootstrapThread.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logger.info("All done!");
    }

    class RecuriveFileCollector extends SimpleFileVisitor<Path> {
        private final List<Path> result = new ArrayList<Path>();
        public RecuriveFileCollector() {
        }
        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
             return FileVisitResult.CONTINUE;
        }
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            result.add(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        public List<Path> getResult(){
            return result;
        }
    }
}