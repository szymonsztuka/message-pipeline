package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

/**
 * Created by szsz on 16/11/15.
 */
public class ServerChainSimulator {

    private static final Logger logger = LoggerFactory.getLogger(ServerChainSimulator.class);

    protected MessageReceiver getMessageReceiver() {
        return new SimpleMessageReceiver();
    }

    protected PullMessageReceiver getPullMessageReceiver() {
        return null;
    }

    protected MessageGenerator getMessageGenerator() {
        return new SimpleMessageGenerator();
    }

    public static void main(String[] args) {
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
                    return path; })
                .map((Path p) -> {
                    Properties prop = new Properties();
                    try {
                        prop.load(Files.newBufferedReader(p, StandardCharsets.UTF_8));
                    } catch(IOException e) {
                        System.err.println(e);//TODO log
                    }
                    return prop; })
                .reduce((Properties p, Properties r) -> {p.putAll(r); return p;});


        Properties agruments = Arrays.stream(args)
                .filter((String s ) -> s.startsWith("-") && s.contains("="))
                .map((String s) -> s.substring(1))
                .collect(()-> new Properties(), (Properties p, String s) -> p.put(s.substring(0, s.indexOf("=")), s.substring(s.indexOf("=")+1)), (Properties p, Properties r) -> p.putAll(r));


        fileProperties.get().putAll(agruments);
        (new TreeMap(fileProperties.get())).forEach((k, v) -> System.out.println(k +"="+ v));//TreeMap to order elements, Properties is a hashmap


        Arrays.asList(fileProperties.get().getProperty("run").split("->|,")).forEach(s -> System.out.println(s.trim()));

        TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(fileProperties.get().getProperty("run").split("->|,")));


        Map<String, String> mapOfProperties = fileProperties.get().entrySet().stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue())));

        Map<String,String> res = mapOfProperties.entrySet().stream()
                .filter(e -> { if(e.getKey().contains(".")) {
                    return nodesToRun.contains(e.getKey().substring(0,e.getKey().indexOf("."))) ;
                } else {
                    return nodesToRun.contains(e.getKey()) || "run".equals(e.getKey());
                }
                })
                .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue())
                );
        System.out.println("\nfiltered\n");
        (new TreeMap(res)).forEach((k,v) -> System.out.println(k +"="+ v));


        Map<String, List<Map.Entry<String,String>>> result =
                //System.out.println(
                res.entrySet().stream()
                        .filter( e -> !e.getKey().equals("run"))
                        .collect(Collectors
                                .groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf("."))));


        Map<String, Map<String,String>> result2 =
                //System.out.println(
                res.entrySet().stream()
                        .filter( e -> !e.getKey().equals("run"))
                        .collect(Collectors
                                .groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf(".")),

                                        Collectors.toMap(
                                                e -> e.getKey().substring(e.getKey().indexOf(".")+1),
                                                e -> e.getValue())

                                ));
        System.out.println(result2);

        Map<String, Map<String,String>>  fileInputTcpServerSender = new TreeMap<>();
        Map<String, Map<String,String>> tcpClientReceiverFileOutput = new TreeMap<>();

        for(Map.Entry<String, Map<String,String>> elem : result2.entrySet()) {
            Map<String,String> values = elem.getValue();
            if ( values.containsKey("type") && values.containsKey("input") && values.containsKey("output")
                    &&  values.get("type").equals("sender")
                    &&  values.get("input").equals("files")
                    &&  values.get("output").equals("tcpserver")
                    ) {
                fileInputTcpServerSender.put(elem.getKey(), elem.getValue());
            } else if(
                    values.containsKey("type") && values.containsKey("input") && values.containsKey("output")
                            &&  values.get("type").equals("receiver")
                            &&  values.get("input").equals("tcpclient")
                            &&  values.get("output").equals("files")
                    ) {
                tcpClientReceiverFileOutput.put(elem.getKey(), elem.getValue());
            } else{
                System.out.print("\n !!!!!!!!!! configuration skipped");
            }
        }
        System.out.println("fileInputTcpServerSender");
        System.out.println(fileInputTcpServerSender);

        System.out.println("\n\ntcpClientReceiverFileOutput " + tcpClientReceiverFileOutput.size());
        System.out.println(tcpClientReceiverFileOutput);

        if(fileInputTcpServerSender.size()==1 && tcpClientReceiverFileOutput.size()==0) {
            Map<String,String> values = fileInputTcpServerSender.entrySet().iterator().next().getValue();
            ServerChainSimulator simulator = new ServerChainSimulator();
            simulator.send(new NetworkEndConfiguration(values.get("output.ip")              ,
                    values.get("output.port")     ,
                    values.get("input.directory") ,
                    values.get("output.clients.number"),
                    "true".equals(values.get("output.realtime")),
                    null) );
            } else if(fileInputTcpServerSender.size()==1 && tcpClientReceiverFileOutput.size()==2) {

                Map<String,String> senderValues = fileInputTcpServerSender.entrySet().iterator().next().getValue();

                Iterator<Map.Entry<String,Map<String,String>>> reciverIt = tcpClientReceiverFileOutput.entrySet().iterator();
                Map<String,String> firstReceiverValues = reciverIt.next().getValue();

                Map<String,String> secondReceiverValues = reciverIt.next().getValue();

                ServerChainSimulator simulator = new ServerChainSimulator();

                List<NetworkEndConfiguration> consumerConfigs = new ArrayList<>(2);
                consumerConfigs.add(

            new NetworkEndConfiguration(firstReceiverValues.get("input.ip"),
                    firstReceiverValues.get("input.port"),
                    firstReceiverValues.get("output.directory"),
                    null,
                    false,
                    null)
            );

            consumerConfigs.add(

                    new NetworkEndConfiguration(secondReceiverValues.get("input.ip"),
                            secondReceiverValues.get("input.port"),
                            secondReceiverValues.get("output.directory"),
                            null,
                            false,
                            null)
            );

                simulator.sendReceive(new NetworkEndConfiguration(senderValues.get("output.ip")              ,
                        senderValues.get("output.port")     ,
                        senderValues.get("input.directory") ,
                        senderValues.get("output.clients.number"),
                        "true".equals(senderValues.get("output.realtime")),
                        null),  consumerConfigs);
        }
    }


    public void send(NetworkEndConfiguration producerConfig) {
        logger.info("send mode");
        Coordinator coordinator = new Coordinator();
        final List<Path> readerFileNames = coordinator.collectProducerPaths(producerConfig.directory, producerConfig.ignoreFolderName);
        Iterator<Path> readerIt = readerFileNames.iterator();

        while(readerIt.hasNext()) {
            CountDownLatch done = new CountDownLatch(1);
            final Runnable producer;
            if(producerConfig.noClients>1) {
                List<MessageGenerator> msgProducers = new ArrayList<>(producerConfig.noClients);
                for(int i=0; i < producerConfig.noClients; i++) {
                    msgProducers.add(getMessageGenerator());
                }
                producer = new MultiProducer(done,  readerIt.next(), msgProducers, producerConfig.adress, producerConfig.noClients, producerConfig.sendAtTimestamp);

            } else{
                producer = new Producer(done, readerIt.next(), getMessageGenerator(), producerConfig.adress, producerConfig.sendAtTimestamp);
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


    public void sendReceive(NetworkEndConfiguration producerConfig, List<NetworkEndConfiguration> consumerConfig) {
        logger.info("1  sender 2 parallel receivers");
        Coordinator coordinator = new Coordinator();
        final List<Path> readerFileNames = coordinator.collectProducerPaths(producerConfig.directory, producerConfig.ignoreFolderName);
        for(Path x: readerFileNames) {
            System.out.println("reader " + x);
        }
       /* final Path outputDir = generateConsumerRootDir(producerConfig.directory);
        System.out.println("writer root " + outputDir);
        final List<Path> writerFileNames = generateConsumerPaths(outputDir, readerFileNames);
        for(Path threads: writerFileNames) {
        	System.out.println("writer " + threads);
        }*/
        /*final Path outputDir = generateConsumerRootDir2(producerConfig.directory, consumerConfig.directory);
        System.out.println("writer root " + outputDir);
        final List<Path> writerFileNames = generateConsumerPaths2(producerConfig.directory, outputDir, readerFileNames);
        for(Path threads: writerFileNames) {
        	System.out.println("writer " + threads);
        }*/
        final Path outputDir1 = Coordinator.generateConsumerRootDir3(consumerConfig.get(0).directory);
        System.out.println("output " +outputDir1);
        final List<Path> writerFileNames1 = Coordinator.generateConsumerPaths3(outputDir1, readerFileNames, producerConfig.directory);


        final Path outputDir2 = Coordinator.generateConsumerRootDir3(consumerConfig.get(1).directory);
        System.out.println("output " +outputDir2);
        final List<Path> writerFileNames2 = Coordinator.generateConsumerPaths3(outputDir2, readerFileNames, producerConfig.directory);

        //Iterator<Path> readerIt = readerFil for (Path path: producerInputPath) {eNames.iterator();
        //Iterator<Path> writerIt = writerFileNames.iterator();
        CyclicBarrier barrier = new CyclicBarrier(4);
        //while (readerIt.hasNext() && writerIt.hasNext()) {
        CountDownLatch done = new CountDownLatch(2);
        NonBlockingConsumerEagerIn consumer1 = new NonBlockingConsumerEagerIn(writerFileNames1, getMessageReceiver(), consumerConfig.get(0).adress, barrier,null);
        NonBlockingConsumerEagerIn consumer2 = new NonBlockingConsumerEagerIn(writerFileNames2, getMessageReceiver(), consumerConfig.get(1).adress, barrier,null);
        List<NonBlockingConsumerEagerIn> consumers = new ArrayList<>(2);
        consumers.add(consumer1);
        consumers.add(consumer2);

        //ProducerPullInPushOut producer = new ProducerPullInPushOut(done, readerFileNames, getMessageGenerator(), producerConfig.adress, barrier, consumers);
        ;
        List generators = new ArrayList<>(2);
        generators.add(getMessageGenerator());
        generators.add(getMessageGenerator());
        PullPushMultiProducer producer = new PullPushMultiProducer(done, readerFileNames,generators  , producerConfig.adress, producerConfig.noClients, producerConfig.sendAtTimestamp, barrier, consumers);

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