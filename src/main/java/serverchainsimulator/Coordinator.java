package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    }

    public void run() {

        List<String> readerFileNames = new ArrayList<String>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(producerInputDirectory))) {
            for (Path path : directoryStream) {
                readerFileNames.add(path.toString());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        List<String> writerFileNames = new ArrayList<String>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(consumerOutputDirectory))) {
            for (Path path : directoryStream) {
                readerFileNames.add(path.toString());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }


        Iterator<String> readerIt = readerFileNames.iterator();
        Iterator<String> writerIt = writerFileNames.iterator();

        while (readerIt.hasNext() && writerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            Producer producer = new Producer(done, Paths.get(readerIt.next()), new SimpleMessageGenerator(), producerHostIp, producerPort);
            NonBlockingConsumer consumer = new NonBlockingConsumer(Paths.get(writerIt.next()), new SimpleMessageReceiver(), consumerHostIp, consumerPort);
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
}
