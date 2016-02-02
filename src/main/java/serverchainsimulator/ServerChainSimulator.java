package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serverchainsimulator.content.MessageGenerator;
import serverchainsimulator.content.MessageReceiver;
import serverchainsimulator.content.ShellScriptGenerator;
import serverchainsimulator.control.LayerControllerRecursive;
import serverchainsimulator.control.LayerControllerRecursiveStateful;
import serverchainsimulator.control.LayerControllerTerminal;
import serverchainsimulator.transport.*;
import serverchainsimulator.transport.Process;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public abstract class ServerChainSimulator {

    private static final Logger logger = LoggerFactory.getLogger(ServerChainSimulator.class);

    protected abstract MessageReceiver getMessageReceiver(String type);

    protected abstract MessageGenerator getMessageGenerator(String type);

    protected abstract ShellScriptGenerator getShellScriptGenerator(String... args);

    public void start(String[] args) {
        Optional<Properties> properties = Arrays.stream(args)
            .filter((String s) -> !s.startsWith("-"))
            .map((String s) -> {
                Path p = Paths.get(s);
                Path path;
                if (p.isAbsolute()) {
                    path = p;
                } else {
                    try {
                         Path root = Paths.get(ServerChainSimulator.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                         path = root.getParent().resolveSibling(p);
                    } catch (URISyntaxException e) {
                         System.err.println(e); //TODO log
                         path = p;
                        }
                    }
                    return path;
            })
            .map((Path p) -> {
                Properties prop = new Properties();
                try {
                    prop.load(Files.newBufferedReader(p, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    System.err.println(e);//TODO log
                }
                return prop;
            })
            .reduce((Properties p, Properties r) -> {
                p.putAll(r);
                return p;
            });
        Properties arguments = Arrays.stream(args)
                .filter((String s) -> s.startsWith("-") && s.contains("="))
                .map((String s) -> s.substring(1))
                .collect(() -> new Properties(), (Properties p, String s) -> p.put(s.substring(0, s.indexOf("=")), s.substring(s.indexOf("=") + 1)), (Properties p, Properties r) -> p.putAll(r));
        properties.get().putAll(arguments);

        // (new TreeMap(fileProperties.get())).forEach((k, v) -> System.out.println(k + "=" + v));//TreeMap to order elements, Properties is a hashmap
        //  Arrays.asList(fileProperties.get().getProperty("run").split("->|,|;")).forEach(s -> System.out.println(s.trim()));

        TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(properties.get().getProperty("run").split("->|,|;")));

        Map<String, String> mapOfProperties = properties.get().entrySet().stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue())));

        Map<String, String> selectedProperties = mapOfProperties.entrySet().stream()
                .filter(e -> {
                    if (e.getKey().contains(".")) {
                        return nodesToRun.contains(e.getKey().substring(0, e.getKey().indexOf(".")));
                    } else {
                        return nodesToRun.contains(e.getKey()) || "run".equals(e.getKey());
                    }
                })
                .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue()));

        String today = (new SimpleDateFormat("dd-MMM-yy")).format(Calendar.getInstance().getTime());

        selectedProperties = selectedProperties.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().replace("{dd-MMM-yy}",today)));
        //(new TreeMap(res)).forEach((k, v) -> System.out.println(k + "=" + v));

        Map<String, Map<String, String>> nodeToProperties = selectedProperties.entrySet().stream()
                    .filter(e -> !e.getKey().equals("run"))
                    .collect(Collectors
                        .groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf(".")),
                            Collectors.toMap(
                                e -> e.getKey().substring(e.getKey().indexOf(".") + 1),
                                e -> e.getValue())));

        for (String compoundStep:   Arrays.asList(properties.get().getProperty("run").split(";"))) {
            Map<String, Map<String, String>> producers = new TreeMap<>();
            Map<String, Map<String, String>> consumers = new TreeMap<>();
            Map<String, Map<String, String>> remoteScripts = new TreeMap<>();
            Map<String, Map<String, String>> processes = new TreeMap<>();
            Map<String, Map<String, String>> localScripts = new TreeMap<>();
            for (String key: Arrays.asList(compoundStep.split("->|,"))) {
                     Map<String, String> values = nodeToProperties.get(key);
                    if (values.containsKey("type") && values.containsKey("input") && values.containsKey("output")
                            && values.get("type").equals("sender")
                            && values.get("input").equals("files")
                            && values.get("output").equals("tcpserver")
                            ) {
                        producers.put(key, values);
                    } else if (values.containsKey("type") && values.containsKey("input") && values.containsKey("output")
                            && values.get("type").equals("receiver")
                            && values.get("input").equals("tcpclient")
                            && values.get("output").equals("files")
                            ) {
                        consumers.put(key, values);
                    } else if (values.containsKey("type") && values.containsKey("host") && values.containsKey("user") && values.containsKey("password")
                            && values.get("type").equals("remotescript")
                            ) {
                        remoteScripts.put(key, values);
                    } else if (values.containsKey("type") && values.get("type").equals("javaprocess")) {
                        processes.put(key, values);
                    } else if (values.containsKey("type") && values.get("type").equals("localscript")) {
                        localScripts.put(key, values);
                    } else {
                        logger.warn("Command " + key + " not recognized");
                }
            }
            logger.info("Dispaching " + producers.size() + " producers, "
            + consumers.size() + " consumers, "
            + remoteScripts.size() + " remote scripts, "
            + processes.size() + " processes, "
            + localScripts.size() + " local scripts ...");
            if (producers.size() == 1 && consumers.size() == 0 && remoteScripts.size() == 0) {
                try {
                     java.lang.Process process = Runtime.getRuntime().exec(localScripts.get(0).get("script"));
                     // exhaust input stream  http://dhruba.name/2012/10/16/java-pitfall-how-to-prevent-runtime-getruntime-exec-from-hanging/
                     BufferedInputStream in = new BufferedInputStream(process.getInputStream());
                     byte[] bytes = new byte[4096];
                     while (in.read(bytes) != -1) {}// wait for completion
                     try {
                         process.waitFor();
                     } catch (InterruptedException e) {
                         e.printStackTrace();
                     }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (producers.size() == 1 && consumers.size() == 0 && remoteScripts.size() == 0) {
                logger.info(" to command send");
                Map<String, String> values = producers.entrySet().iterator().next().getValue();
                send(values);
            } else if (producers.size() == 1 && consumers.size() > 0 && remoteScripts.size() == 0) {
                logger.info("... to command send receive process");
                Map<String, String> producer = producers.entrySet().iterator().next().getValue();
                Iterator<Map.Entry<String, Map<String, String>>> consumerIterator = consumers.entrySet().iterator();
                List<Map<String, String>> consumerConfigs = new ArrayList<>(2);
                consumerConfigs.add(consumerIterator.next().getValue());
                if(consumerIterator.hasNext()) {
                    consumerConfigs.add(consumerIterator.next().getValue());
                }
                List<Map<String, String>> producerConfigs = new ArrayList<>(1);
                producerConfigs.add(producer);
                List<Map<String, String>> processConfigs = new ArrayList<>(processes.size());
                for (Map<String, String> x : processes.values()) {
                     processConfigs.add(x);
                }
                sendReceiveInterpreter(producerConfigs, consumerConfigs, processConfigs);
            } else if (producers.size() == 1 && consumers.size() == 2 && remoteScripts.size() == 2) {
                logger.info("... to command send receive remote scripts");
                Map<String, String> producerConfigs = producers.entrySet().iterator().next().getValue();
                Iterator<Map.Entry<String, Map<String, String>>> consumerIterator = consumers.entrySet().iterator();
                List<Map<String, String>> consumerConfigs = new ArrayList<>(2);
                consumerConfigs.add(consumerIterator.next().getValue());
                consumerConfigs.add(consumerIterator.next().getValue());
                Iterator<Map.Entry<String, Map<String, String>>> remoteScriptIterator = remoteScripts.entrySet().iterator();
                List<Map<String, String>> scriptConfig = new ArrayList<>(2);
                scriptConfig.add(remoteScriptIterator.next().getValue());
                scriptConfig.add(remoteScriptIterator.next().getValue());
                sendReceiveThreeLayers(producerConfigs, consumerConfigs, scriptConfig);
            } else {
                logger.error("... not dispatched");
            }
        }
    }

    public void send(Map<String,String> values) {

        final List<Path> readerFileNames = collectProducerPaths(Paths.get(values.get("input.directory")), null);
        Iterator<Path> readerIt = readerFileNames.iterator();
        while (readerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            final Runnable producer;
            final int noOfClients = Integer.parseInt(values.get("output.clients.number"));
            if (noOfClients > 1) {
                List<MessageGenerator> msgProducers = new ArrayList<>(noOfClients);
                for (int i=0; i < noOfClients; i++) {
                    msgProducers.add(getMessageGenerator( values.get("output.format")));
                }
                producer = new DeprecatedMultiProducer(done,
                        readerIt.next(),
                        msgProducers,
                        new InetSocketAddress(values.get("output.ip"), Integer.parseInt(values.get("output.port"))),
                        noOfClients,
                        "true".equals(values.get("output.realtime")));
            } else {
                producer = new DepracetedProducer(done,
                        readerIt.next(),
                        getMessageGenerator( values.get("output.format")),
                        new InetSocketAddress(values.get("output.ip"), Integer.parseInt(values.get("output.port"))),
                        "true".equals(values.get("output.realtime")));
            }
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

    public void sendReceiveInterpreter(List<Map<String,String>> senderConfig, List<Map<String,String>> consumerConfig, List<Map<String,String>> processConfigs) {

        final List<Path> allReaderFileNames = collectProducerPaths(Paths.get(senderConfig.get(0).get("input.directory")), null);
        Iterator<Path> it = allReaderFileNames.iterator();
        Path a = it.next();
        while (it.hasNext()) {
            final List<Path> readerFileNames = new ArrayList<>(30);
            readerFileNames.add(a);
            while (it.hasNext()) {
                Path b = it.next();
                if (a.getParent().equals(b.getParent())
                        || processConfigs.isEmpty()
                        || (!processConfigs.isEmpty() && processConfigs.get(0).containsKey("restart")) && !processConfigs.get(0).get("restart").equals("on-new-folder")){
                    readerFileNames.add(b);
                    a = b;
                } else {
                    a = b;
                    break;
                }
            }
            List<PullConsumer> consumers = new ArrayList<>(consumerConfig.size());
            List<Thread> consumersThreads = new ArrayList<>(consumerConfig.size());

            CyclicBarrier consumerStartBarrier = new CyclicBarrier(consumerConfig.size() + 1);
            CyclicBarrier consumerStopBarrier = new CyclicBarrier(consumerConfig.size() + 1);
            CyclicBarrier producerStartBarrier = new CyclicBarrier(senderConfig.size() + 1);
            CyclicBarrier producerStopBarrier = new CyclicBarrier(senderConfig.size() + 1);

            for (Map<String, String> node : consumerConfig) {
                final Path outputDir = generateConsumerRootDir3(Paths.get(node.get("output.directory")));
                final List<Path> writerFileNames = generateConsumerPaths3(outputDir, readerFileNames, Paths.get(senderConfig.get(0).get("input.directory")));
                PullConsumer consumer = new PullConsumer(writerFileNames,
                        getMessageReceiver(node.get("input.format")),
                        new InetSocketAddress(node.get("input.ip"), Integer.parseInt(node.get("input.port"))),
                        consumerStartBarrier,
                        consumerStopBarrier);
                consumers.add(consumer);
                consumersThreads.add(new Thread(consumer));
            }

            List<PushProducer> producers = new ArrayList<>(senderConfig.size());
            List<Thread> producersThreads = new ArrayList<>(senderConfig.size());

            for (Map<String, String> node : senderConfig) {
                final int clientsNumber = Integer.parseInt(node.get("output.clients.number")) > 0 ? Integer.parseInt(node.get("output.clients.number")) : 1;
                List generators = new ArrayList<>(clientsNumber);
                for (int j = 0; j < clientsNumber; j++) {
                    generators.add(getMessageGenerator(node.get("output.format")));
                }
                String newAdress = node.get("output.ip");
                /*if("localhost".equalsIgnoreCase(node.get("output.ip"))) {
                    try {
                        newAdress = InetAddress.getLocalHost().getHostAddress();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }*/
                PushProducer producer = new PushProducer(
                        readerFileNames,
                        generators,
                        new InetSocketAddress(newAdress, Integer.parseInt(node.get("output.port"))),
                        Integer.parseInt(node.get("output.clients.number")),
                        "true".equals(node.get("output.realtime")),
                        producerStartBarrier,
                        producerStopBarrier);
                producers.add(producer);
                producersThreads.add(new Thread(producer));
            }
            //LayerController controller = new LayerController(consumerStartBarrier, consumerStopBarrier, consumers, producerStartBarrier, producerStopBarrier, producers);
            LayerControllerTerminal terminalLayer = new LayerControllerTerminal("input-simulators", producerStartBarrier, producerStopBarrier, producers);
            LayerControllerRecursive topLayer = new LayerControllerRecursive("output-captures", consumerStartBarrier, consumerStopBarrier, consumers, terminalLayer);
            final Thread controllerThread;
            if (processConfigs.size() > 0) {
                List<Process> bootstraps = new ArrayList<>(processConfigs.size());
                List<Thread> bootstrapThreads = new ArrayList<>(processConfigs.size());
                CyclicBarrier batchStart = new CyclicBarrier(processConfigs.size() + 1);
                CyclicBarrier batchEnd = new CyclicBarrier(processConfigs.size() + 1);
                for (Map<String, String> node : processConfigs) {
                    Process process = new Process(node.get("classpath"), node.get("jvmArguments").split(" "), node.get("mainClass"),
                            node.get("programArguments").split(" "), batchStart, batchEnd);
                    bootstraps.add(process);
                    bootstrapThreads.add(new Thread(process));
                }
                LayerControllerRecursiveStateful processesLayer = new LayerControllerRecursiveStateful("parsers " + bootstraps.size(), batchStart, batchEnd, bootstraps, topLayer);
                controllerThread = new Thread(processesLayer);
                controllerThread.start();
                bootstrapThreads.forEach(Thread::start);
            } else {
                controllerThread = new Thread(topLayer);
                controllerThread.start();
            }
            consumersThreads.forEach(Thread::start);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producersThreads.forEach(Thread::start);
            producersThreads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumersThreads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        logger.info("All done!");
    }

    //TODO company specific
    public void sendReceiveThreeLayers(Map<String,String> producerConfigs, List<Map<String,String>> consumerConfigs, List<Map<String,String>> remoteScriptConfigs) {

        final List<Path> readerFileNames = collectProducerPaths(Paths.get(producerConfigs.get("input.directory")), null);

        final Path outputDir1 = generateConsumerRootDir3(Paths.get(consumerConfigs.get(0).get("output.directory")));

        final List<Path> writerFileNames1 = generateConsumerPaths3(outputDir1, readerFileNames, Paths.get(producerConfigs.get("input.directory")));

        final Path outputDir2 = generateConsumerRootDir3(Paths.get(consumerConfigs.get(1).get("output.directory")));
        System.out.println("output " +outputDir2);
        final List<Path> writerFileNames2 = generateConsumerPaths3(outputDir2, readerFileNames, Paths.get(producerConfigs.get("input.directory")));


        final Path thirdLayerOutputDir1 = generateConsumerRootDir3(Paths.get(remoteScriptConfigs.get(0).get("output.directory")));
        System.out.println("output " +thirdLayerOutputDir1);
        final List<Path>  thirdLayerWriterFileNames1 = generateConsumerPaths3(thirdLayerOutputDir1, readerFileNames, Paths.get(producerConfigs.get("input.directory")));

        final Path thirdLayerOutputDir2 = generateConsumerRootDir3(Paths.get(remoteScriptConfigs.get(1).get("output.directory")));
        System.out.println("output " +thirdLayerOutputDir2);
        final List<Path>  thirdLayerWriterFileNames2 = generateConsumerPaths3(thirdLayerOutputDir2, readerFileNames, Paths.get(producerConfigs.get("input.directory")));

        CyclicBarrier barrier = new CyclicBarrier(6); //updated for SSH
        CountDownLatch done = new CountDownLatch(2);

        final List<RemoteShellScrip> thirdLayer = new ArrayList<>();

        final List<String> thirdLayerFileNames1 = new ArrayList<>();
        for (Iterator<Path> it = thirdLayerWriterFileNames1.iterator(); it.hasNext(); ){
            thirdLayerFileNames1.add(it.next().toString());
        }
        final List<String> thirdLayerFileNames2 = new ArrayList<>();
        for (Iterator<Path> it = thirdLayerWriterFileNames2.iterator(); it.hasNext(); ) {
            thirdLayerFileNames2.add(it.next().toString());
        }

        final List<String> fileNames = new ArrayList<>();
        for (Iterator<Path> it = writerFileNames1.iterator(); it.hasNext(); ){
            fileNames.add(it.next().toString());
        }
        Iterator<Map<String,String>> thirdLayerIt = remoteScriptConfigs.iterator();
        int i=0;
        while (thirdLayerIt.hasNext()) {
            Map<String,String> elem = thirdLayerIt.next();
            RemoteShellScrip thirdLayerNode = new RemoteShellScrip(
                    elem.get("user"),
                    elem.get("host"),
                    elem.get("password"),
                    elem.get("sudo_pass"),
                    (i % 2 == 0? thirdLayerFileNames1: thirdLayerFileNames2),
                    barrier,
                    getShellScriptGenerator(elem.get("commandPart1"), elem.get("commandPart2")));
            thirdLayer.add(thirdLayerNode);
            i++;
        }

        Iterator<RemoteShellScrip> thirdLayerNodesIt = thirdLayer.iterator();
        final List<Runnable> thirdLayerThreads = new ArrayList<>();
        while (thirdLayerNodesIt.hasNext()) {
            Thread t = new Thread(thirdLayerNodesIt.next());
            t.start();
            thirdLayerThreads.add(t);
        }

        //System.out.println(consumerConfigs);
        DeprecatedPullConsumer consumer1 = new DeprecatedPullConsumer(writerFileNames1,
                getMessageReceiver(consumerConfigs.get(0).get("input.format")),
                new InetSocketAddress(consumerConfigs.get(0).get("input.ip"), Integer.parseInt(consumerConfigs.get(0).get("input.port"))),
                barrier,
                thirdLayer.get(0));
        DeprecatedPullConsumer consumer2 = new DeprecatedPullConsumer(writerFileNames2,
                getMessageReceiver(consumerConfigs.get(1).get("input.format")),
                new InetSocketAddress(consumerConfigs.get(1).get("input.ip"), Integer.parseInt(consumerConfigs.get(1).get("input.port"))),
                barrier,
                thirdLayer.get(1));
        List<DeprecatedPullConsumer> consumers = new ArrayList<>(2);
        consumers.add(consumer1);
        consumers.add(consumer2);

        final int clientsNumber = Integer.parseInt(producerConfigs.get("output.clients.number")) > 0 ? Integer.parseInt(producerConfigs.get("output.clients.number")) : 1;
        List generators = new ArrayList<>(clientsNumber);
        for(int j=0; j < clientsNumber; j++) {
            generators.add(getMessageGenerator(producerConfigs.get("output.format")));
        }
        DeprecatedPushProducer producer =
                new DeprecatedPushProducer(done,
                        readerFileNames,
                        generators,
                        new InetSocketAddress(producerConfigs.get("output.ip"), Integer.parseInt(producerConfigs.get("output.port"))),
                        clientsNumber,
                        "true".equals(producerConfigs.get("output.realtime")),
                        barrier,
                        consumers);

        Thread producerThread = new Thread(producer);
        Thread consumerThread1 = new Thread(consumer1);
        Thread consumerThread2 = new Thread(consumer2);

        System.out.println("Consumer start!");
        consumerThread1.start();
        consumerThread2.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("DepracetedProducer start!");
        producerThread.start();
        try {
            producerThread.join();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //logger.info("Terminating consumer!");
        //consumer.terminate();
        System.out.println("Awaiting join consumer!");
        try {
            consumerThread1.join();
            consumerThread2.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        done.countDown();
        //}
        logger.info("All done!");
    }


    public List<Path>collectProducerPaths (Path dirOrFilePath, String ignore) {

        if(Files.notExists(dirOrFilePath)) {
            try {
                Files.createDirectories(dirOrFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (Files.isDirectory(dirOrFilePath, LinkOption.NOFOLLOW_LINKS)) {
            RecursiveFileCollector walk= new RecursiveFileCollector(ignore);
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

    public static  Path generateConsumerRootDir3(final Path outputDir) {


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

    public static List<Path> generateConsumerPaths3(Path outputDir, List<Path> producerInputPath, Path inputDir) {
        final List<Path> readerFileNames = new ArrayList<Path>();
        for (Path path: producerInputPath) {
            final Path newOne;
            if(inputDir.getNameCount() != path.getNameCount()) {
                Path outputSubPath = path.subpath(inputDir.getNameCount(), path.getNameCount());
                newOne = outputDir.resolve(outputSubPath);}
            else {
                continue;
            }
            readerFileNames.add(newOne);
            if (Files.notExists(newOne.getParent())) {
                try {
                    Files.createDirectories(newOne.getParent());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return readerFileNames;
    }

    class RecursiveFileCollector extends SimpleFileVisitor<Path> {
        private final List<Path> result = new ArrayList<Path>();
        private final String ignore;
        public RecursiveFileCollector(String ignore) {
            this.ignore = ignore;
        }
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (dir.getFileName().toString().equals(ignore)) {
                //System.out.println("skipping" + dir.getFileName().toString());
                return FileVisitResult.SKIP_SUBTREE;
            } else {
                return super.preVisitDirectory(dir, attrs);
            }
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