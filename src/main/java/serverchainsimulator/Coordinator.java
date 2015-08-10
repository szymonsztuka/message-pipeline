package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

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

    private final List<String> consumersHostIp = new ArrayList<>(2);
    private final List<Integer> consumersPort = new ArrayList<>(2);
    private final List<String> consumersOutputDirectory= new ArrayList<>(2);
    private final List<JvmInstanceConfiguration> jvmsConfigurations =  new ArrayList<>(2);

    /**
     * @param args path to configuration file
     */
    public static void main(String[] args) {

        if (args.length < 1) {
            throw new IllegalArgumentException();
        }

        final Coordinator coordinator = new Coordinator(args[0], args[1]);
        coordinator.run();

    }

    protected MessageReceiver getMessageReceiver() {
        return new SimpleMessageReceiver();
    }

    protected MessageGenerator getMessageGenerator() {
        return new SimpleMessageGenerator();
    }

    public Coordinator(String propertiesPath, String inputFile) {
        final Properties configProp = new Properties();

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
        if (inputFile != null && inputFile.length() > 4) {
            producerInputDirectory = inputFile;
        } else {
            producerInputDirectory = configProp.getProperty("inputDirectory");
        }
        consumerOutputDirectory = configProp.getProperty("outputDirectory");
        producerPort = Integer.parseInt(configProp.getProperty("producerPort"));
        consumerPort = configProp.getProperty("consumerPort") != null ? Integer.parseInt(configProp.getProperty("consumerPort")) : null;

        producerHostIp = configProp.getProperty("producerHostIp", "127.0.0.1");
        consumerHostIp = configProp.getProperty("consumerHostIp", "127.0.0.1");

        if(configProp.getProperty("outputDirectory2") != null
                && configProp.getProperty("consumerPort2") !=null) {
            consumersHostIp.add(consumerHostIp);
            consumersPort.add(consumerPort);
            consumersOutputDirectory.add(consumerOutputDirectory);

            consumersHostIp.add(configProp.getProperty("consumerHostIp2", "127.0.0.1"));
            consumersPort.add(Integer.parseInt(configProp.getProperty("consumerPort2")));
            consumersOutputDirectory.add(configProp.getProperty("outputDirectory2"));
        }







        if (producerInputDirectory == null || producerPort == null) {
            //requierd
        }

        if ((consumerOutputDirectory == null || consumerPort == null) && (consumerOutputDirectory != null || consumerPort != null)) {
            //impartial configuration
        }
        bootstrapMainClass = configProp.getProperty("bootstrapMainClass");
        bootstrapClasspath = configProp.getProperty("bootstrapClasspath");
        bootstrapJvmArguments = configProp.getProperty("bootstrapJvmArguments") != null ? configProp.getProperty("bootstrapJvmArguments").split(" ") : null;
        bootstrapProgramArguments = configProp.getProperty("bootstrapProgramArguments") != null ? configProp.getProperty("bootstrapProgramArguments").split(" ") : null;
        if (bootstrapMainClass == null && bootstrapClasspath == null && bootstrapJvmArguments == null && bootstrapProgramArguments == null) {

        } else if ((bootstrapMainClass != null || bootstrapClasspath != null || bootstrapJvmArguments != null || bootstrapProgramArguments != null)
                &&
                (bootstrapMainClass == null || bootstrapClasspath == null || bootstrapJvmArguments == null || bootstrapProgramArguments == null)
                ) {
            //impartial configuration
        }
        restartServerAfterEachFile = Boolean.parseBoolean(configProp.getProperty("restartServerAfterEachFile", "true"));

    }

    public void run() {
        if (bootstrapMainClass != null && bootstrapClasspath != null &&
                bootstrapJvmArguments != null && bootstrapProgramArguments != null) { //managed
            if (restartServerAfterEachFile) {
                if(consumersOutputDirectory.size() > 1) {
                    multiStatelessBootstrap();
                }else {
                    statelessBootstrap();
                }
            } else {
                statefullBootstrap();
            }
        } else { //remote
            if (consumerOutputDirectory != null && consumerPort != null) {
                remoteBootstrap();
            } else {
                sendingOnly();
            }
        }
    }

    public void statelessBootstrap() {
        logger.info("statelessBootstrap");
        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);

        final Path outputDir = generateConsumerRootDir(consumerOutputDirectory);

        final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);

        Iterator<Path> readerIt = readerFileNames.iterator();
        Iterator<Path> writerIt = writerFileNames.iterator();

        while (readerIt.hasNext() && writerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(2);
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerHostIp, producerPort);
            NonBlockingConsumer consumer = new NonBlockingConsumer(writerIt.next(), getMessageReceiver(), consumerHostIp, consumerPort);
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
                producerThread.join();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            try {
                Thread.sleep(1000 * 60);
            } catch (InterruptedException e) {
                e.printStackTrace();
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

            done.countDown();

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

    public void multiStatelessBootstrap() {
        logger.info("multiStatelessBootstrap");
        //final int consumersNumber = 2;
        //final List<Path> consumersDirs = new ArrayList<>(2);
        final List<List<Path>> writersFileNames = new ArrayList<>(consumersOutputDirectory.size());

        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);
        for(String x: consumersOutputDirectory ) {
            final Path outputDir = generateConsumerRootDir(x);//consumerOutputDirectory);
            //consumersDirs.add(outputDir);
            final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);
            writersFileNames.add(writerFileNames);
        }

        Iterator<Path> readerIt = readerFileNames.iterator();
        //Iterator<Path> writerIt = writerFileNames.iterator();
        //Iterator<List<Path>> writersIt = writersFileNames.iterator();
        int i= 0;
        while (readerIt.hasNext() ) {
            CountDownLatch done = new CountDownLatch(1 + writersFileNames.size());
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerHostIp, producerPort);
            List<NonBlockingConsumer> consumers = new ArrayList<>();
            int j = 0;
            for(List<Path> x: writersFileNames){
                NonBlockingConsumer consumer = new NonBlockingConsumer(x.get(i), getMessageReceiver(),
                        consumersHostIp.get(j),
                        consumersPort.get(j));
                j++;
                consumers.add(consumer);
            }
            i++;
            List<Bootstrap> jvms = new ArrayList<>(jvmsConfigurations.size());
            List<Thread> jvmThreads = new ArrayList<>(jvmsConfigurations.size());
            for(JvmInstanceConfiguration x: jvmsConfigurations) {
                Bootstrap bootstrap = new Bootstrap(done, bootstrapClasspath, bootstrapJvmArguments, bootstrapMainClass, bootstrapProgramArguments);
                jvms.add(bootstrap);
                Thread bootstrapThread = new Thread(bootstrap);
                jvmThreads.add(bootstrapThread);
            }
            Thread producerThread = new Thread(producer);
            List<Thread> consumersThread = new ArrayList<>();
            for(NonBlockingConsumer c: consumers) {
                Thread consumerThread = new Thread(c);
                consumersThread.add(consumerThread);
            }
            for(Thread c: jvmThreads) {
                c.start();
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for(Thread c: consumersThread) {
                c.start();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producerThread.start();
            try {
                producerThread.join();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            try {
                Thread.sleep(1000 * 60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Terminating consumer!");
            for(NonBlockingConsumer c: consumers) {
                c.terminate();
            }
            logger.info("Awaiting join consumer!");
            try {
                for(Thread c: consumersThread) {
                    c.join();
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            done.countDown();

            logger.info("Awaiting join bootsrap!");
            try {
                for(Thread c: jvmThreads) {
                    c.join();
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        logger.info("All done!");

    }
    public void statefullBootstrap() {

        logger.info("statefullBootstrap");
        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);

        final Path outputDir = generateConsumerRootDir(consumerOutputDirectory);

        final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);

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
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerHostIp, producerPort);
            NonBlockingConsumer consumer = new NonBlockingConsumer(writerIt.next(), getMessageReceiver(), consumerHostIp, consumerPort);


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

    public void remoteContiniusSend() {

        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);

        Iterator<Path> readerIt = readerFileNames.iterator();

        CyclicBarrier turn = new CyclicBarrier(2);
        final Queue<String> queue = new ConcurrentLinkedQueue<String>();
        SingleConnectionProducer producer = new SingleConnectionProducer(turn, queue, getMessageGenerator(), producerHostIp, producerPort);

        Thread producerThread = new Thread(producer);
        producerThread.start();

        String line;
        while (readerIt.hasNext()) {
            try (BufferedReader reader = Files.newBufferedReader(readerIt.next(), Charset.forName("UTF-8"))) {
                while ((line = reader.readLine()) != null) {
                    queue.add(line);
                }
                queue.add(""); //terminal string
            } catch (IOException ex) {
                logger.error("Producer cannot read data ", ex);
            }
            try {
                try {
                    turn.await();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        producer.terminate();
        try {
            producerThread.join();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


        logger.info("All done!");
    }

    public void remoteBootstrap() {
        logger.info("remoteBootstrap");
        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);

        final Path outputDir = generateConsumerRootDir(consumerOutputDirectory);

        final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);

        Iterator<Path> readerIt = readerFileNames.iterator();
        Iterator<Path> writerIt = writerFileNames.iterator();

        while (readerIt.hasNext() && writerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerHostIp, producerPort);
            NonBlockingConsumer consumer = new NonBlockingConsumer(writerIt.next(), getMessageReceiver(), consumerHostIp, consumerPort);


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
        logger.info("All done!");
    }

    public void sendingOnly() {
        logger.info("sendingOnly");
        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);
        Iterator<Path> readerIt = readerFileNames.iterator();

        while(readerIt.hasNext())
        {
            CountDownLatch done = new CountDownLatch(1);
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerHostIp, producerPort);
            Thread producerThread = new Thread(producer);

            producerThread.start();

            try {
                producerThread.join();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        logger.info("All done!");
    }

    private static boolean isDirNonEmpty(final Path directory) throws IOException {
        try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            return dirStream.iterator().hasNext();
        }
    }

    private List<Path>collectProducerPaths (String dirOrFilePath){
        Path inputDir = Paths.get(dirOrFilePath);

        if(Files.isDirectory(inputDir,LinkOption.NOFOLLOW_LINKS)) {
            RecursiveFileCollector walk= new RecursiveFileCollector();
            try {
                Files.walkFileTree(inputDir, walk);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return walk.getResult();
        } else {
            List<Path>  singleFile = new ArrayList<Path>();
            singleFile.add(inputDir);
            return singleFile;
        }
    }


    private Path generateConsumerRootDir(String dirName) {
        Path outputDir = Paths.get(dirName +"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
        int i=1;
        try {
            while (Files.exists(outputDir) && isDirNonEmpty(outputDir)) {
                String candidate = outputDir.toString();
                if(i>1){
                    outputDir = Paths.get(candidate.substring(0,candidate.length()-2)
                            + "-" + (i < 10 ? "0" + i : i));
                } else{
                    outputDir = Paths.get(candidate + "-" + (i < 10 ? "0" + i : i));
                }
                i++;
            }
            if(Files.notExists(outputDir)) {
                Files.createDirectory(outputDir);
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
        return outputDir;
    }

    private List<Path> generateConsumerPaths(Path outputDir,List<Path> producerInputPath){
        int x = outputDir.getNameCount();
        final List<Path> readerFileNames = new ArrayList<Path>();
        readerFileNames.addAll(producerInputPath);
        for(Path path: readerFileNames) {
            Path outputSubPath = path.subpath(x, path.getNameCount());
            Path newOne = outputDir.resolve(outputSubPath);
            readerFileNames.add(newOne);
            if (Files.notExists(newOne.getParent())) {
                try {
                    Files.createDirectory(newOne.getParent());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return readerFileNames;
    }
    class RecursiveFileCollector extends SimpleFileVisitor<Path> {
        private final List<Path> result = new ArrayList<Path>();
        public RecursiveFileCollector() {
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