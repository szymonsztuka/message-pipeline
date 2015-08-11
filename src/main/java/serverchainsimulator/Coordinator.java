package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final List<NetworkEndConfiguration> producerConfigurations = new ArrayList<>(1);
    private final List<NetworkEndConfiguration> consumerConfigurations = new ArrayList<>(1);
    private final List<JvmInstanceConfiguration> jvmConfigurations =  new ArrayList<>(1);

    /**
     * @param args path to configuration file
     */
    public static void main(String[] args) {

        if (args.length < 1) {
            throw new IllegalArgumentException();
        }

        String mode = "";
        String file = null;
        if (args.length > 1) {
        	if (args[1].startsWith("-")) {
        		mode = args[1].substring(1, args[1].length());
        	} else { //file
        		file = args[1];
        	}
        }
        if (args.length > 2) {
          	if (args[2].startsWith("-")) {
        		mode = args[2].substring(1, args[2].length());
        	} else { //file
        		file = args[2];
        	}
        }
        final Coordinator coordinator = new Coordinator(args[0], mode, file);
        coordinator.run();

    }

    protected MessageReceiver getMessageReceiver() {
        return new SimpleMessageReceiver();
    }

    protected MessageGenerator getMessageGenerator() {
        return new SimpleMessageGenerator();
    }

    public Coordinator(String propertiesPath, String mode, String inputFile) {
    	final Properties properties = new Properties();

        InputStream in = null;
        try {
        	System.out.println(propertiesPath);
            in = new FileInputStream(propertiesPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (inputFile != null) {
        	if (Files.exists(Paths.get(inputFile))) {
        		properties.setProperty("input.directory", inputFile);
        	} else {
        		throw new IllegalArgumentException("Can't find directory or file " + inputFile + ".");
        	}
        }
        for (Object x : properties.keySet()) {
        	System.out.println(x + "=" + properties.getProperty((String) x));
        }
        if (properties.getProperty("input.ip") != null) {
        	producerConfigurations.add(
        			new NetworkEndConfiguration(properties.getProperty("input.ip"), properties.getProperty("input.port"), properties.getProperty("input.directory")));
        }
        int i = 1;
        String prefix = "1.";
        while (properties.getProperty(prefix + "input.ip") != null) {
        	producerConfigurations.add(
        			new NetworkEndConfiguration(properties.getProperty(prefix + "input.ip"), properties.getProperty(prefix + "input.port"), properties.getProperty("input.directory")));
        	prefix = String.valueOf(++i) + ".";
        }
        if (mode.contains("o") || mode.contains("O")) {
        	System.out.println("o");
        	if (properties.getProperty("output.ip") != null) {
        		consumerConfigurations.add(
        				new NetworkEndConfiguration(properties.getProperty("output.ip"), properties.getProperty("output.port"), properties.getProperty("output.directory")));
        	}
        	i = 1;
        	prefix = "1.";
        	while (properties.getProperty(prefix + "output.ip") != null) {
        		consumerConfigurations.add(
        				new NetworkEndConfiguration(properties.getProperty(prefix + "output.ip"), properties.getProperty(prefix + "output.port"), properties.getProperty(prefix + "output.directory")));
        		prefix = String.valueOf(++i) + ".";
        	}
        }
        if (mode.contains("p") || mode.contains("P")) {
        	System.out.println("p");
       		if (properties.getProperty("server.classpath") != null) {
       			jvmConfigurations.add(
       					new JvmInstanceConfiguration(properties.getProperty("server.classpath"), properties.getProperty("server.jvm-arguments"), properties.getProperty("server.main-class"), properties.getProperty("server.program-arguments"),  properties.getProperty("server.restart")));
       		}
       		i = 1;
       		prefix = "1.";
       		while (properties.getProperty(prefix + "server.classpath") != null) {
       			jvmConfigurations.add(
       					new JvmInstanceConfiguration(properties.getProperty(prefix + "server.classpath"), properties.getProperty(prefix + "server.jvm-arguments"), properties.getProperty(prefix + "server.main-class"), properties.getProperty(prefix + "server.program-arguments"),  properties.getProperty("server.restart")));
       			prefix = String.valueOf(++i) + ".";
       		}
        }
    }

    public void run() {
    	if (producerConfigurations.size() > 0 && jvmConfigurations.size() > 0 && consumerConfigurations.size() > 0) {
    		if (producerConfigurations.size() > 1 && jvmConfigurations.size() > 1 && consumerConfigurations.size() > 1) {
    			System.out.println("No action implemented case 1.");
    		} else {
    			statelessBootstrap(producerConfigurations.get(0), jvmConfigurations.get(0), consumerConfigurations.get(0));
    		}
    	} else if (producerConfigurations.size() > 0 && jvmConfigurations.size() == 0 && consumerConfigurations.size() > 0) {
    		System.out.println("No action implemented case 3.");
    	} else if (producerConfigurations.size() > 0 && jvmConfigurations.size() == 0 && consumerConfigurations.size() == 0) {
    		send(producerConfigurations.get(0));
    	} else {
    		System.out.println("No action " + producerConfigurations.size() + " " + jvmConfigurations.size() + " " + consumerConfigurations.size());
    	}
    }

    public void statelessBootstrap(NetworkEndConfiguration producerConfig, JvmInstanceConfiguration serverConfig, NetworkEndConfiguration consumerConfig) {
        logger.info("statelessBootstrap");
        final List<Path> readerFileNames = collectProducerPaths(producerConfig.directory);
        for(Path x: readerFileNames) {
        	System.out.println("reader " + x);
        }
       /* final Path outputDir = generateConsumerRootDir(producerConfig.directory);
        System.out.println("writer root " + outputDir);
        final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);
        for(Path x: writerFileNames) {
        	System.out.println("writer " + x);
        }*/
        /*final Path outputDir = generateConsumerRootDir2(producerConfig.directory, consumerConfig.directory);
        System.out.println("writer root " + outputDir);
        final List<Path> writerFileNames = generateConsumerPaths2(producerConfig.directory, outputDir, readerFileNames);
        for(Path x: writerFileNames) {
        	System.out.println("writer " + x);
        }*/
        final Path outputDir = generateConsumerRootDir3(consumerConfig.directory);
        System.out.println("output " +outputDir);
        final List<Path> writerFileNames = generateConsumerPaths3(outputDir, readerFileNames, producerConfig.directory);
        for(Path x: writerFileNames) {
        	System.out.println("writer " + x);
        }
        
        Iterator<Path> readerIt = readerFileNames.iterator();
        Iterator<Path> writerIt = writerFileNames.iterator();

        while (readerIt.hasNext() && writerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(2);
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerConfig.adress);
            NonBlockingConsumer consumer = new NonBlockingConsumer(writerIt.next(), getMessageReceiver(), consumerConfig.adress);
            Bootstrap bootstrap = new Bootstrap(done, serverConfig.classpath, serverConfig.jvmArguments, serverConfig.mainClass, serverConfig.programArguments);

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
/*
    public void multiStatelessBootstrap(List<NetworkEndConfiguration> producerConfigs, List<JvmInstanceConfiguration> serverConfigs, List<NetworkEndConfiguration> consumerConfigs) {

    	logger.info("multiStatelessBootstrap");
        final List<List<Path>> writersFileNames = new ArrayList<>(consumersOutputDirectory.size());

        final List<Path> readerFileNames = collectProducerPaths(producerInputDirectory);
        for(String x: consumersOutputDirectory ) {
            final Path outputDir = generateConsumerRootDir(x);
            final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);
            writersFileNames.add(writerFileNames);
        }

        Iterator<Path> readerIt = readerFileNames.iterator();
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
            List<Bootstrap> jvms = new ArrayList<>(jvmConfigurations.size());
            List<Thread> jvmThreads = new ArrayList<>(jvmConfigurations.size());
            for(JvmInstanceConfiguration x: jvmConfigurations) {
                Bootstrap bootstrap = new Bootstrap(done, x.classpath, x.jvmArguments, x.mainClass, x.programArguments);
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

    }*/
   /* public void statefullBootstrap() {

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
*/
  /*  public void remoteContiniusSend() {

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
*/
/*    public void remoteBootstrap() {
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
*/
    public void send(NetworkEndConfiguration producerConfig) {
        logger.info("send mode");
        final List<Path> readerFileNames = collectProducerPaths(producerConfig.directory);
        Iterator<Path> readerIt = readerFileNames.iterator();

        while(readerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            Producer producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerConfig.adress);
            Thread producerThread = new Thread(producer);

            producerThread.start();

            try {
                producerThread.join();
            } catch (InterruptedException e1) {
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

    private List<Path>collectProducerPaths (Path dirOrFilePath) {

        if (Files.isDirectory(dirOrFilePath, LinkOption.NOFOLLOW_LINKS)) {
            RecursiveFileCollector walk= new RecursiveFileCollector();
            try {
                Files.walkFileTree(dirOrFilePath, walk);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return walk.getResult();
        } else {
            List<Path> singleFile = new ArrayList<Path>();
            singleFile.add(dirOrFilePath);
            return singleFile;
        }
    }


    public int lastCommonElement(Path x, Path y){
    	int i = 1;
    	while( i < x.getNameCount() && i < y.getNameCount()) {
    		if(x.subpath(0, i).toAbsolutePath().equals(y.subpath(0, i).toAbsolutePath())) {
    			i ++;
    		} else {
    			return i-1;
    		}
    		
    	}
    	return 0;
    }
    
    private Path generateConsumerRootDir(Path dirName) {
        Path outputDir = Paths.get(dirName + "-" + (new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
        int i = 1;
        try {
            while (Files.exists(outputDir) && isDirNonEmpty(outputDir)) {
                String candidate = outputDir.toString();
                if (i>1){
                    outputDir = Paths.get(candidate.substring(0,candidate.length()-2)
                            + "-" + (i < 10 ? "0" + i : i));
                } else {
                    outputDir = Paths.get(candidate + "-" + (i < 10 ? "0" + i : i));
                }
                i++;
            }
            if (Files.notExists(outputDir)) {
                Files.createDirectory(outputDir);
            }
        } catch(IOException ex) {
            ex.printStackTrace();
        }
        return outputDir;
    }

    
    private Path generateConsumerRootDir2(final Path inputDir, final Path outputDir) {
    	  System.out.println("R-2 " + outputDir.toAbsolutePath());
    	int j = lastCommonElement(inputDir, outputDir);	
    	 System.out.println("R0 j=" + j);
    	 Path root = outputDir;
    	 while( root.getNameCount() > j +1 ){
    		 root = root.getParent();
    	 }
        //Path root = Paths.get(outputDir.subpath(0, j + 1 ) + "-" + (new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
        //Path root = outputDir.subpath(0, j-1).toAbsolutePath(); 
        System.out.println("R-1 " + root);
        root = root.resolve(/*outputDir.subpath(j, j+1).toString() + "-" +*/ (new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date())));
        System.out.println("R0 " + root);
        int i = 1;
        try {
            while (Files.exists(root) && isDirNonEmpty(root)) {
            	System.out.println("R1 " + root.toString());
                String candidate = root.toString();
                if (i>1){
                    root = Paths.get(candidate.substring(0,candidate.length()-2)
                            + "-" + (i < 10 ? "0" + i : i));
                } else {
                    root = Paths.get(candidate + "-" + (i < 10 ? "0" + i : i));
                }
                
                System.out.println("R2 " + root.toString());
                i++;
            }
            if (Files.notExists(root)) {
                Files.createDirectory(root);
            }
            System.out.println("R3 " + root);
            //remaining paths
            root = root.resolve( outputDir.subpath(j+1, outputDir.getNameCount()) );
            System.out.println("R4 " + root);
            if (Files.notExists(root)) {
                Files.createDirectories(root);
            }
            
        } catch(IOException ex) {
            ex.printStackTrace();
        }
        return root;
    }
    
    
    
    private Path generateConsumerRootDir3(final Path outputDir) {
    	
    	
    	final String n;
    	if(outputDir.toString().contains("/output/")) {
    		n = outputDir.toString().replace("/output/", "/output/"+(new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date()))+"/");   
    	} else {
    		n = outputDir.toString().replace("\\output\\", "\\output\\"+(new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date()))+"\\");  
    	}
    	Path root = Paths.get(n);
    	if (Files.notExists(root)) {
            try {
            	Files.createDirectories(root);
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	return root;
    }
    
    private List<Path> generateConsumerPaths2(Path inputDir, Path outputDir, List<Path> producerInputPath) {
    	 System.out.println("-2 "+inputDir);
         System.out.println("-1 "+outputDir);
    	int  i = lastCommonElement(inputDir, outputDir);
    	 System.out.println("0 "+i);
        final List<Path> readerFileNames = new ArrayList<Path>();
        for (Path path: producerInputPath) {
            Path outputSubPath = path.subpath(i+1, path.getNameCount());
            Path newOne = outputDir.resolve(outputSubPath);
            System.out.println("1 "+outputSubPath);
            System.out.println("2 "+outputDir);
            System.out.println("3 "+newOne);
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
    
    private List<Path> generateConsumerPaths(Path outputDir, List<Path> producerInputPath) {
        int x = outputDir.getNameCount();
        final List<Path> readerFileNames = new ArrayList<Path>();
        for (Path path: producerInputPath) {
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
    
    
    private List<Path> generateConsumerPaths3(Path outputDir, List<Path> producerInputPath, Path inputDir) {
        final List<Path> readerFileNames = new ArrayList<Path>();
        for (Path path: producerInputPath) {
            Path outputSubPath = path.subpath(inputDir.getNameCount(), path.getNameCount());
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