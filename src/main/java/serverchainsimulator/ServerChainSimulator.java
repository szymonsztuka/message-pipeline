package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

/**
 * Created by szsz on 16/11/15.
 */
public abstract class ServerChainSimulator {

    private static final Logger logger = LoggerFactory.getLogger(ServerChainSimulator.class);

    protected abstract MessageReceiver getMessageReceiver(String type);

    //protected abstract PullMessageReceiver getPullMessageReceiver(String type);

    protected abstract MessageGenerator getMessageGenerator(String type);

    protected abstract ShellScriptGenerator getShellScriptGenerator(String... args);

    public void start(String[] args) {
        Optional<Properties> fileProperties = Arrays.stream(args)
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


        Properties agruments = Arrays.stream(args)
                .filter((String s) -> s.startsWith("-") && s.contains("="))
                .map((String s) -> s.substring(1))
                .collect(() -> new Properties(), (Properties p, String s) -> p.put(s.substring(0, s.indexOf("=")), s.substring(s.indexOf("=") + 1)), (Properties p, Properties r) -> p.putAll(r));


        fileProperties.get().putAll(agruments);
        (new TreeMap(fileProperties.get())).forEach((k, v) -> System.out.println(k + "=" + v));//TreeMap to order elements, Properties is a hashmap


        Arrays.asList(fileProperties.get().getProperty("run").split("->|,|;")).forEach(s -> System.out.println(s.trim()));

        TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(fileProperties.get().getProperty("run").split("->|,|;")));


        Map<String, String> mapOfProperties = fileProperties.get().entrySet().stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue())));

        Map<String, String> res = mapOfProperties.entrySet().stream()
                .filter(e -> {
                    if (e.getKey().contains(".")) {
                        return nodesToRun.contains(e.getKey().substring(0, e.getKey().indexOf(".")));
                    } else {
                        return nodesToRun.contains(e.getKey()) || "run".equals(e.getKey());
                    }
                })
                .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue())
                );
        System.out.println("\nfiltered\n");
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yy");
        String today = sdf.format(cal.getTime());



         res = res.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().replace("{dd-MMM-yy}",today))
        );
        (new TreeMap(res)).forEach((k, v) -> System.out.println(k + "=" + v));

        Map<String, List<Map.Entry<String, String>>> result =
                //System.out.println(
                res.entrySet().stream()
                        .filter(e -> !e.getKey().equals("run"))
                        .collect(Collectors
                                .groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf("."))));


        Map<String, Map<String, String>> result2 =
                res.entrySet().stream()
                        .filter(e -> !e.getKey().equals("run"))
                        .collect(Collectors
                                .groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf(".")),

                                        Collectors.toMap(
                                                e -> e.getKey().substring(e.getKey().indexOf(".") + 1),
                                                e -> e.getValue())

                                ));
        System.out.println(result2);


        List<String> compoundSteps = Arrays.asList(fileProperties.get().getProperty("run").split(";"));
        Iterator<String> stepIt = compoundSteps.iterator();
        while (stepIt.hasNext()) {

            String compoundStep = stepIt.next();
            List<String> steps = Arrays.asList(compoundStep.split("->|,"));
            if (steps.size() == 1) {
                Map<String, String> values = result2.get(steps.get(0));
                if (values.containsKey("type") && values.get("type").equals("localscript")) {
                    System.out.println("-----------------Executing " + steps.get(0)+ " " + values.get("script"));
                    try {

                        Process process = Runtime.getRuntime().exec(values.get("script"));
                        // exhaust input stream  http://dhruba.name/2012/10/16/java-pitfall-how-to-prevent-runtime-getruntime-exec-from-hanging/
                        BufferedInputStream in = new BufferedInputStream(process.getInputStream());
                        byte[] bytes = new byte[4096];
                        while (in.read(bytes) != -1) {}
                        // wait for completion
                        try {
                            process.waitFor();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                Map<String, Map<String, String>> fileInputTcpServerSender = new TreeMap<>();
                Map<String, Map<String, String>> tcpClientReceiverFileOutput = new TreeMap<>();
                Map<String, Map<String, String>> remoteSrcipts = new TreeMap<>();

                for (Map.Entry<String, Map<String, String>> elem : result2.entrySet()) {
                    Map<String, String> values = elem.getValue();
                    if (values.containsKey("type") && values.containsKey("input") && values.containsKey("output")
                            && values.get("type").equals("sender")
                            && values.get("input").equals("files")
                            && values.get("output").equals("tcpserver")
                            ) {
                        fileInputTcpServerSender.put(elem.getKey(), elem.getValue());
                    } else if (values.containsKey("type") && values.containsKey("input") && values.containsKey("output")
                            && values.get("type").equals("receiver")
                            && values.get("input").equals("tcpclient")
                            && values.get("output").equals("files")
                            ) {
                        tcpClientReceiverFileOutput.put(elem.getKey(), elem.getValue());
                    } else if (values.containsKey("type") && values.containsKey("host") && values.containsKey("user") && values.containsKey("password")
                            && values.get("type").equals("remotescript")
                            ) {
                        remoteSrcipts.put(elem.getKey(), elem.getValue());
                    } else {
                        System.out.print("\n !!!!!!!!!! configuration skipped '" + values + "'\n");
                    }
                }
                    System.out.println("fileInputTcpServerSender");
                    System.out.println(fileInputTcpServerSender);

                    System.out.println("\n\ntcpClientReceiverFileOutput " + tcpClientReceiverFileOutput.size());
                    System.out.println(tcpClientReceiverFileOutput);

                    if (fileInputTcpServerSender.size() == 1 && tcpClientReceiverFileOutput.size() == 0 && remoteSrcipts.size() == 0) {
                        Map<String, String> values = fileInputTcpServerSender.entrySet().iterator().next().getValue();
                        send(values);
                    } else if (fileInputTcpServerSender.size() == 1 && tcpClientReceiverFileOutput.size() == 2 && remoteSrcipts.size() == 0) {
                        Map<String, String> senderValues = fileInputTcpServerSender.entrySet().iterator().next().getValue();
                        Iterator<Map.Entry<String, Map<String, String>>> receiverIt = tcpClientReceiverFileOutput.entrySet().iterator();
                        List<Map<String, String>> consumerConfigs = new ArrayList<>(2);
                        consumerConfigs.add(receiverIt.next().getValue());
                        consumerConfigs.add(receiverIt.next().getValue());
                        sendReceive(senderValues, consumerConfigs);
                    } else if (fileInputTcpServerSender.size() == 1 && tcpClientReceiverFileOutput.size() == 2 && remoteSrcipts.size() == 2) {
                        Map<String, String> senderValues = fileInputTcpServerSender.entrySet().iterator().next().getValue();
                        Iterator<Map.Entry<String, Map<String, String>>> receiverIt = tcpClientReceiverFileOutput.entrySet().iterator();
                        List<Map<String, String>> consumerConfigs = new ArrayList<>(2);
                        consumerConfigs.add(receiverIt.next().getValue());
                        consumerConfigs.add(receiverIt.next().getValue());
                        Iterator<Map.Entry<String, Map<String, String>>> thirdLayerIt = remoteSrcipts.entrySet().iterator();
                        List<Map<String, String>> thirdLayer = new ArrayList<>(2);
                        thirdLayer.add(thirdLayerIt.next().getValue());
                        thirdLayer.add(thirdLayerIt.next().getValue());
                        sendReceiveThreeLayers(senderValues, consumerConfigs, thirdLayer);
                    }
            }
        }
    }
    public void send(Map<String,String> values) {

        logger.info("send mode");
        Coordinator coordinator = new Coordinator();
        final List<Path> readerFileNames = coordinator.collectProducerPaths(Paths.get(values.get("input.directory")), null);
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
                producer = new MultiProducer(done,
                        readerIt.next(),
                        msgProducers,
                        new InetSocketAddress(values.get("output.ip"), Integer.parseInt(values.get("output.port"))),
                        noOfClients,
                        "true".equals(values.get("output.realtime")));

            } else {
                producer = new Producer(done,
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


    public void sendReceive(Map<String,String> senderValues, List<Map<String,String>> consumerConfig) {

        logger.info("1  sender 2 parallel receivers");
        Coordinator coordinator = new Coordinator();
        final List<Path> readerFileNames = coordinator.collectProducerPaths(Paths.get(senderValues.get("input.directory")), null);

        final Path outputDir1 = Coordinator.generateConsumerRootDir3(Paths.get(consumerConfig.get(0).get("output.directory")));
        System.out.println("output " +outputDir1);
        final List<Path> writerFileNames1 = Coordinator.generateConsumerPaths3(outputDir1, readerFileNames, Paths.get(senderValues.get("input.directory")));

        final Path outputDir2 = Coordinator.generateConsumerRootDir3(Paths.get(consumerConfig.get(1).get("output.directory")));
        System.out.println("output " +outputDir2);
        final List<Path> writerFileNames2 = Coordinator.generateConsumerPaths3(outputDir2, readerFileNames, Paths.get(senderValues.get("input.directory")));

        CyclicBarrier barrier = new CyclicBarrier(4);
        CountDownLatch done = new CountDownLatch(2);
        System.out.println(consumerConfig);
        NonBlockingConsumerEagerIn consumer1 = new NonBlockingConsumerEagerIn(writerFileNames1,
                getMessageReceiver(consumerConfig.get(0).get("input.format")),
                new InetSocketAddress(consumerConfig.get(0).get("input.ip"), Integer.parseInt(consumerConfig.get(0).get("input.port"))),
                barrier,
                null);
        NonBlockingConsumerEagerIn consumer2 = new NonBlockingConsumerEagerIn(writerFileNames2,
                getMessageReceiver(consumerConfig.get(1).get("input.format")),
                new InetSocketAddress(consumerConfig.get(1).get("input.ip"), Integer.parseInt(consumerConfig.get(1).get("input.port"))),
                barrier,
                null);
        List<NonBlockingConsumerEagerIn> consumers = new ArrayList<>(2);
        consumers.add(consumer1);
        consumers.add(consumer2);

        List generators = new ArrayList<>(2);
        generators.add(getMessageGenerator(senderValues.get("output.format")));
        generators.add(getMessageGenerator(senderValues.get("output.format")));
        PullPushMultiProducer producer =
                new PullPushMultiProducer(done,
                        readerFileNames,
                        generators,
                        new InetSocketAddress(senderValues.get("output.ip"), Integer.parseInt(senderValues.get("output.port"))),
                        Integer.parseInt(senderValues.get("output.clients.number")),
                        "true".equals(senderValues.get("output.realtime")),
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
        System.out.println("Producer start!");
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

    public void sendReceiveInterpreter(List<Map<String,String>> senderConfig, List<Map<String,String>> consumerConfig) {

        logger.info( senderConfig.size() + " sender(s) " + consumerConfig.size() + " parallel receivers");
        Coordinator coordinator = new Coordinator();
        final List<Path> readerFileNames = coordinator.collectProducerPaths(Paths.get(senderConfig.get(0).get("input.directory")), null);

        List<NonBlockingConsumerEagerInDecoupled> consumers = new ArrayList<>(consumerConfig.size());
        List<Thread> consumersThreads = new ArrayList<>(consumerConfig.size());


        CyclicBarrier consumerStartBarrier = new CyclicBarrier(consumerConfig.size() + 1);
        CyclicBarrier consumerStopBarrier = new CyclicBarrier(consumerConfig.size() + 1);
        CyclicBarrier producerStartBarrier = new CyclicBarrier(senderConfig.size() + 1);
        CyclicBarrier producerStopBarrier = new CyclicBarrier(senderConfig.size() + 1, () -> {});

        for (Map<String,String> node: consumerConfig) {
            final Path outputDir = Coordinator.generateConsumerRootDir3(Paths.get(node.get("output.directory")));
            System.out.println("output " + outputDir);
            final List<Path> writerFileNames = Coordinator.generateConsumerPaths3(outputDir, readerFileNames, Paths.get(senderConfig.get(0).get("input.directory")));
            NonBlockingConsumerEagerInDecoupled consumer = new NonBlockingConsumerEagerInDecoupled(writerFileNames,
                    getMessageReceiver(node.get("input.format")),
                    new InetSocketAddress(node.get("input.ip"), Integer.parseInt(node.get("input.port"))),
                    consumerStartBarrier,
                    consumerStopBarrier);
            consumers.add(consumer);
            consumersThreads.add(new Thread(consumer));
        }

        List<PullPushMultiProducerDecoupled> producers = new ArrayList<>(senderConfig.size());
        List<Thread> producersThreads = new ArrayList<>(senderConfig.size());

        for (Map<String,String> node: senderConfig) {
            List generators = new ArrayList<>(2);
            generators.add(getMessageGenerator(node.get("output.format")));
            generators.add(getMessageGenerator(node.get("output.format")));
            PullPushMultiProducerDecoupled producer = new PullPushMultiProducerDecoupled(
                            readerFileNames,
                            generators,
                            new InetSocketAddress(node.get("output.ip"), Integer.parseInt(node.get("output.port"))),
                            Integer.parseInt(node.get("output.clients.number")),
                            "true".equals(node.get("output.realtime")),
                            producerStartBarrier,
                            producerStopBarrier);
            producers.add(producer);
            producersThreads.add(new Thread(producer));
        }

        LayerController controller = new LayerController(consumerStartBarrier, consumerStopBarrier, consumers, producerStartBarrier, producerStopBarrier, producers);
        Thread controllerThread = new Thread(controller);
        controllerThread.start();

        System.out.println("Consumer start!");
        consumersThreads.forEach(Thread::start);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Producer start!");
        producersThreads.forEach(Thread::start);

        producersThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        try {
            Thread.sleep(1000 * 5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Awaiting join consumer!");
        consumersThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        //done.countDown();
        logger.info("All done!");
    }
    public void sendReceiveThreeLayers(Map<String,String> senderValues, List<Map<String,String>> consumerConfig, List<Map<String,String>> consumerConfigThirdLayer) {

        logger.info("1  sender 2 parallel  2 scripts");
        Coordinator coordinator = new Coordinator();
        final List<Path> readerFileNames = coordinator.collectProducerPaths(Paths.get(senderValues.get("input.directory")), null);

        final Path outputDir1 = Coordinator.generateConsumerRootDir3(Paths.get(consumerConfig.get(0).get("output.directory")));

        final List<Path> writerFileNames1 = Coordinator.generateConsumerPaths3(outputDir1, readerFileNames, Paths.get(senderValues.get("input.directory")));

        final Path outputDir2 = Coordinator.generateConsumerRootDir3(Paths.get(consumerConfig.get(1).get("output.directory")));
        System.out.println("output " +outputDir2);
        final List<Path> writerFileNames2 = Coordinator.generateConsumerPaths3(outputDir2, readerFileNames, Paths.get(senderValues.get("input.directory")));


        final Path thirdLayerOutputDir1 = Coordinator.generateConsumerRootDir3(Paths.get(consumerConfigThirdLayer.get(0).get("output.directory")));
        System.out.println("output " +thirdLayerOutputDir1);
        final List<Path>  thirdLayerWriterFileNames1 = Coordinator.generateConsumerPaths3(thirdLayerOutputDir1, readerFileNames, Paths.get(senderValues.get("input.directory")));

        final Path thirdLayerOutputDir2 = Coordinator.generateConsumerRootDir3(Paths.get(consumerConfigThirdLayer.get(1).get("output.directory")));
        System.out.println("output " +thirdLayerOutputDir2);
        final List<Path>  thirdLayerWriterFileNames2 = Coordinator.generateConsumerPaths3(thirdLayerOutputDir2, readerFileNames, Paths.get(senderValues.get("input.directory")));


        CyclicBarrier barrier = new CyclicBarrier(6); //updated for SSH
        CountDownLatch done = new CountDownLatch(2);


        final List<RemoteShellScripTask> thirdLayer = new ArrayList<>();


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
        Iterator<Map<String,String>> thirdLayerIt = consumerConfigThirdLayer.iterator();
        int i=0;
        while (thirdLayerIt.hasNext()) {
            Map<String,String> elem = thirdLayerIt.next();
            RemoteShellScripTask thirdLayerNode = new RemoteShellScripTask(
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

        Iterator<RemoteShellScripTask> thirdLayerNodesIt = thirdLayer.iterator();
        final List<Runnable> thirdLayerThreads = new ArrayList<>();
        while (thirdLayerNodesIt.hasNext()) {
            Thread t = new Thread(thirdLayerNodesIt.next());
            t.start();
            thirdLayerThreads.add(t);
        }

        System.out.println(consumerConfig);
        NonBlockingConsumerEagerIn consumer1 = new NonBlockingConsumerEagerIn(writerFileNames1,
                getMessageReceiver(consumerConfig.get(0).get("input.format")),
                new InetSocketAddress(consumerConfig.get(0).get("input.ip"), Integer.parseInt(consumerConfig.get(0).get("input.port"))),
                barrier,
                thirdLayer.get(0));
        NonBlockingConsumerEagerIn consumer2 = new NonBlockingConsumerEagerIn(writerFileNames2,
                getMessageReceiver(consumerConfig.get(1).get("input.format")),
                new InetSocketAddress(consumerConfig.get(1).get("input.ip"), Integer.parseInt(consumerConfig.get(1).get("input.port"))),
                barrier,
                thirdLayer.get(1));
        List<NonBlockingConsumerEagerIn> consumers = new ArrayList<>(2);
        consumers.add(consumer1);
        consumers.add(consumer2);

        List generators = new ArrayList<>(2);
        generators.add(getMessageGenerator(senderValues.get("output.format")));
        generators.add(getMessageGenerator(senderValues.get("output.format")));
        PullPushMultiProducer producer =
                new PullPushMultiProducer(done,
                        readerFileNames,
                        generators,
                        new InetSocketAddress(senderValues.get("output.ip"), Integer.parseInt(senderValues.get("output.port"))),
                        Integer.parseInt(senderValues.get("output.clients.number")),
                        "true".equals(senderValues.get("output.realtime")),
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
        System.out.println("Producer start!");
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
}