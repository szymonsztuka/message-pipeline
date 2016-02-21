package messagepipeline;

import messagepipeline.experimental.*;
import messagepipeline.pipeline.node.Node;
import messagepipeline.pipeline.topology.Layer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.message.MessageGenerator;
import messagepipeline.message.MessageReceiver;
import messagepipeline.message.ShellScriptGenerator;
import messagepipeline.pipeline.topology.NestedLayer;
import messagepipeline.pipeline.topology.StatefulLayer;
import messagepipeline.pipeline.topology.LeafLayer;
import messagepipeline.pipeline.node.*;
import messagepipeline.pipeline.node.JvmProcess;

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
import java.util.stream.Collector;
import java.util.stream.Collectors;

public abstract class MessagePipeline {

    private static final Logger logger = LoggerFactory.getLogger(MessagePipeline.class);

    protected abstract MessageReceiver getMessageReceiver(String type);

    protected abstract MessageGenerator getMessageGenerator(String type);

    protected abstract ShellScriptGenerator getShellScriptGenerator(String... args);

    public static Map<String,String> mergeProperties(String[] args) {
        Optional<Properties> properties = Arrays.stream(args)
                .filter((String s) -> !s.startsWith("-"))
                .map((String s) -> {
                    Path p = Paths.get(s);
                    Path path;
                    if (p.isAbsolute()) {
                        path = p;
                    } else {
                        try {
                            Path root = Paths.get(MessagePipeline.class.getProtectionDomain().getCodeSource().getLocation().toURI());
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
                .collect(() -> new Properties(),
                        (Properties p, String s) -> p.put(s.substring(0, s.indexOf("=")),
                                s.substring(s.indexOf("=") + 1)),
                        (Properties p, Properties r) -> p.putAll(r));
        properties.get().putAll(arguments);

        return properties.get().entrySet().stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue())));
        // (new TreeMap(fileProperties.get())).forEach((k, v) -> System.out.println(k + "=" + v));//TreeMap to order elements, Properties is a hashmap
        //  Arrays.asList(fileProperties.get().getProperty("run").split("->|,|;")).forEach(s -> System.out.println(s.trim()));
    }

    public static Map<String, String> filterProperties(Map<String, String> properties, Set<String> selectedProperites) {
        Map<String, String> selectedProperties = properties.entrySet().stream()
                .filter(e -> {
                    if (e.getKey().contains(".")) {
                        return selectedProperites.contains(e.getKey().substring(0, e.getKey().indexOf(".")));
                    } else {
                        return selectedProperites.contains(e.getKey()) || "run".equals(e.getKey());
                    }})
                .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue()));
            return selectedProperties;
    }

    public static Map<String, String> getVariables(Map<String,String> properties, String variablePrefix) {
        Map<String,String> variables = properties.entrySet().stream().filter(e -> e.getKey().toString().startsWith(variablePrefix)).
                collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey().toString().substring(e.getKey().toString().indexOf('.')+1)),
                        e -> String.valueOf(e.getValue())));
        return variables;
    }

    public static Map<String, String> replaceVariables(Map<String, String> properties, Map<String,String> variables, List<String> dateFormats ) {

        Map<String, String> alteredProperties = new TreeMap<>();
        alteredProperties.putAll(properties);
        for(String date : dateFormats) {
            final String today = (new SimpleDateFormat(date)).format(Calendar.getInstance().getTime());
            alteredProperties = alteredProperties.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(),
                            e -> e.getValue().replace("{"+date+"}",today)));
        }
        alteredProperties = alteredProperties.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(replace(e.getValue().toString(),variables))));
        return alteredProperties;
    }

    public static Map<String, Map<String, String>> wrapProperties(Map<String,String> properties) {
        return properties.entrySet().stream().filter(e -> e.getKey().contains("."))
                .collect(Collectors.groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf(".")),
                        Collectors.toMap(e -> e.getKey().substring(e.getKey().indexOf(".") + 1),
                                e -> e.getValue())));
    }

    public void start(String[] args) {

        final Map<String, String> rawProperties = mergeProperties(args);
        final Map<String, String> variables = getVariables(rawProperties, "path.");
        final Map<String, String> properties = replaceVariables(rawProperties, variables, Arrays.asList(new String[]{"dd-MMM-yy","yyyy-MMM-dd"}));
        final TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(properties.get("run").split("->|,|;")));
        final Map<String, String> selectedProperties = filterProperties(properties, nodesToRun);
        final Map<String, Map<String, String>> nodeToProperties = wrapProperties(selectedProperties);

        for (String compoundStep: Arrays.asList(properties.get("run").split(";"))) {
            final Map<String, Map<String, String>> producers = new TreeMap<>();
            final Map<String, Map<String, String>> consumers = new TreeMap<>();
            final Map<String, Map<String, String>> remoteScripts = new TreeMap<>();
            final Map<String, Map<String, String>> processes = new TreeMap<>();
            final Map<String, Map<String, String>> localScripts = new TreeMap<>();
            for (String key: Arrays.asList(compoundStep.split("->|,"))) {
                Map<String, String> values = nodeToProperties.get(key);
                if ("sender".equals(values.get("type"))
                         && "files".equals(values.get("input"))
                         && "tcpserver".equals(values.get("output"))) {
                    producers.put(key, values);
                } else if ("receiver".equals(values.get("type"))
                        && "tcpclient".equals(values.get("input"))
                        && "files".equals(values.get("output"))) {
                    consumers.put(key, values);
                } else if ("remotescript".equals(values.get("type"))
                        && values.containsKey("host")
                        && values.containsKey("user")
                        && values.containsKey("password")) {
                    remoteScripts.put(key, values);
                } else if ("javaprocess".equals(values.get("type"))) {
                    processes.put(key, values);
                } else if ("localscript".equals(values.get("type"))) {
                    localScripts.put(key, values);
                } else {
                    logger.warn("Command " + key + " not recognized");
                }
            }


            /*logger.info("Dispatching " + producers.size() + " producers, "
                + consumers.size() + " consumers, "
                + remoteScripts.size() + " remote scripts, "
                + processes.size() + " processes, "
                + localScripts.size() + " local scripts ...");*/
            if (localScripts.size() == 1 && producers.size() == 0 && consumers.size() == 0 && remoteScripts.size() == 0) {
                try {
                     String script = localScripts.values().iterator().next().get("script");
                     if(script!=null) {
                         String info = "Running " + script;
                         logger.info("... to command localscript '" + script + "'");
                         System.out.print( info );
                         final Process process = Runtime.getRuntime().exec(script);
                         // exhaust input stream  http://dhruba.name/2012/10/16/java-pitfall-how-to-prevent-runtime-getruntime-exec-from-hanging/
                         final BufferedInputStream in = new BufferedInputStream(process.getInputStream());
                         final byte[] bytes = new byte[4096];
                         while (in.read(bytes) != -1) {
                         }// wait for completion
                         try {
                             process.waitFor();
                         } catch (InterruptedException e) {
                             e.printStackTrace();
                         }
                         for(int i=0; i < info.length(); i++) {
                             System.out.print("\b");
                         }
                         System.out.println("Done    " + script);
                     }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (producers.size() == 1 && consumers.size() == 0 && remoteScripts.size() == 0) {
                logger.info("... to command send");
                send(producers.entrySet().iterator().next().getValue());
            } else if (producers.size() == 1 && consumers.size() > 0 && remoteScripts.size() == 0) {
                logger.info("... to command run send receive");
                //final List<Map<String, String>> consumerConfigs = consumers.values().stream().collect(Collectors.toList()); //map to list
                sendReceiveInterpreter(producers, consumers, processes);
            } else if (producers.size() == 1 && consumers.size() == 2 && remoteScripts.size() == 3) {
                logger.info("... to command send receive remote scripts");
                Map<String, String> producerConfigs = producers.entrySet().iterator().next().getValue();
                Iterator<Map.Entry<String, Map<String, String>>> consumerIterator = consumers.entrySet().iterator();
                List<Map<String, String>> consumerConfigs = new ArrayList<>(2);
                consumerConfigs.add(consumerIterator.next().getValue());
                consumerConfigs.add(consumerIterator.next().getValue());
                Iterator<Map.Entry<String, Map<String, String>>> remoteScriptIterator = remoteScripts.entrySet().iterator();
                List<Map<String, String>> scriptConfig = new ArrayList<>(3);
                scriptConfig.add(remoteScriptIterator.next().getValue());
                scriptConfig.add(remoteScriptIterator.next().getValue());
                scriptConfig.add(remoteScriptIterator.next().getValue());
                sendReceiveThreeLayers(producerConfigs, consumerConfigs, scriptConfig);
            } else {
                logger.error("... not dispatched");
            }
        }
    }


    @Deprecated
    public void send(Map<String,String> values) {
        final List<Path> readerFileNames = collectPaths(Paths.get(values.get("input.directory")), null);
        Iterator<Path> readerIt = readerFileNames.iterator();
        while(readerIt.hasNext()){
            CountDownLatch done = new CountDownLatch(1);
            final Runnable producer;
            final int noOfClients = Integer.parseInt(values.get("output.clients.number"));
            if(noOfClients > 1) {
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
                producer = new DeprecatedProducer(done,
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
        logger.info("done");
    }

    public void sendReceiveInterpreter(Map<String, Map<String,String>> senderConfig, Map<String, Map<String,String>> consumerConfig, Map<String, Map<String,String>> processConfigs) {

        final Path basePath = Paths.get(senderConfig.values().iterator().next().get("input.directory"));

        final List<Path> allReaderFileNames = collectPaths(basePath, null);
        final Iterator<Path> it = allReaderFileNames.iterator();
        Path a = it.next();
        while(it.hasNext()) {
            final List<Path> readerFileNames = new ArrayList<>(30);
            readerFileNames.add(a);
            while (it.hasNext()) {
                final Path b = it.next();
                if (a.getParent().equals(b.getParent())
                        || processConfigs.isEmpty()
                        || (!processConfigs.isEmpty()
                            && processConfigs.values().iterator().next().containsKey("restart"))
                            && !processConfigs.values().iterator().next().get("restart").equals("on-new-folder")){
                    readerFileNames.add(b);
                    a = b;
                } else {
                    a = b;
                    break;
                }
            }

            final List<String> fileNames = readerFileNames.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());

            final List<PullConsumer> consumers = new ArrayList<>(consumerConfig.size());
            final List<Thread> consumersThreads = new ArrayList<>(consumerConfig.size());

            final CyclicBarrier consumerStartBarrier = new CyclicBarrier(consumerConfig.size() + 1);
            final CyclicBarrier consumerStopBarrier = new CyclicBarrier(consumerConfig.size() + 1);
            final CyclicBarrier producerStartBarrier = new CyclicBarrier(senderConfig.size() + 1);
            final CyclicBarrier producerStopBarrier = new CyclicBarrier(senderConfig.size() + 1);

            String consumerLayerName = null;
            for (Map.Entry<String, Map<String, String>> node : consumerConfig.entrySet()) {
                 final PullConsumer consumer = new PullConsumer(getNextAvailablePath(node.getValue().get("output.directory")),
                        fileNames,
                        getMessageReceiver(node.getValue().get("input.format")),
                        new InetSocketAddress(node.getValue().get("input.ip"), Integer.parseInt(node.getValue().get("input.port"))),
                        consumerStartBarrier,
                        consumerStopBarrier);
                consumers.add(consumer);
                consumersThreads.add(new Thread(consumer,node.getKey()));
                if (consumerLayerName == null){ consumerLayerName = node.getKey();} else { consumerLayerName = consumerLayerName+", "+node.getKey();}
            }

            final List<PushProducer> producers = new ArrayList<>(senderConfig.size());
            final List<Thread> producersThreads = new ArrayList<>(senderConfig.size());

            String producerLayerName = null;
            for (Map.Entry<String, Map<String, String>> node : senderConfig.entrySet()) {
                final int clientsNumber;
                if(Integer.parseInt(node.getValue().get("output.clients.number")) > 0) {
                    clientsNumber = Integer.parseInt(node.getValue().get("output.clients.number"));
                } else {
                    clientsNumber = 1;
                }
                final List generators = new ArrayList<>(clientsNumber);
                for (int j = 0; j < clientsNumber; j++) {
                    generators.add(getMessageGenerator(node.getValue().get("output.format")));
                }
                final PushProducer producer = new PushProducer(
                        node.getKey(),
                        node.getValue().get("input.directory"),
                        fileNames,
                        generators,
                        new InetSocketAddress(node.getValue().get("output.ip"), Integer.parseInt(node.getValue().get("output.port"))),
                        "true".equals(node.getValue().get("output.realtime")),
                        producerStartBarrier,
                        producerStopBarrier);
                producers.add(producer);
                producersThreads.add(new Thread(producer,node.getKey()));
                if (producerLayerName == null){ producerLayerName = node.getKey();} else { producerLayerName = producerLayerName+", "+node.getKey();}
            }
            final LeafLayer terminalLayer = new LeafLayer(producerLayerName, producerStartBarrier, producerStopBarrier, producers);
            final NestedLayer topLayer = new NestedLayer(consumerLayerName, fileNames, consumerStartBarrier, consumerStopBarrier, consumers, terminalLayer);
            final Thread controllerThread;
            if (processConfigs.size() > 0) {
                String processLayerName = null;
                final List<JvmProcess> bootstraps = new ArrayList<>(processConfigs.size());
                final List<Thread> bootstrapThreads = new ArrayList<>(processConfigs.size());
                final CyclicBarrier batchStart = new CyclicBarrier(processConfigs.size() + 1);
                final CyclicBarrier batchEnd = new CyclicBarrier(processConfigs.size() + 1);
                for (Map.Entry<String, Map<String, String>> node : processConfigs.entrySet()) {
                    final JvmProcess process = new JvmProcess(node.getValue().get("classpath"),
                            node.getValue().get("jvmArguments").split(" "),
                            node.getValue().get("mainClass"),
                            node.getValue().get("programArguments").split(" "),
                            batchStart,
                            batchEnd);
                    bootstraps.add(process);
                    bootstrapThreads.add(new Thread(process, node.getKey()));
                    if (processLayerName == null){ processLayerName = node.getKey();} else { processLayerName = processLayerName + ", " + node.getKey();}
                }
                final StatefulLayer processesLayer = new StatefulLayer(processLayerName, fileNames, batchStart, batchEnd, bootstraps, topLayer);
                controllerThread = new Thread(processesLayer, "ctrl");
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
        logger.info("done");
    }

    public LeafLayer createLeafLayer(Map<String, Map<String, String>> command, List<String> names) {
        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<LeafNode> nodes = new ArrayList<>(command.size());
        for(Map.Entry<String,Map<String,String>> e : command.entrySet()) {
            if ("sender".equals(e.getValue().get("type"))
                    && "files".equals(e.getValue().get("input"))
                    && "tcpserver".equals(e.getValue().get("output"))) {
                int clientsNumber;
                if(Integer.parseInt(e.getValue().get("output.clients.number")) > 0) {
                    clientsNumber = Integer.parseInt(e.getValue().get("output.clients.number"));
                } else {
                    clientsNumber = 1;
                }
                List generators = new ArrayList<>(clientsNumber);
                for (int j = 0; j < clientsNumber; j++) {
                    generators.add(getMessageGenerator(e.getValue().get("output.format")));
                }
                PushProducer producer = new PushProducer(
                        e.getKey(),
                        e.getValue().get("input.directory"),
                        names,
                        generators,
                        new InetSocketAddress(e.getValue().get("output.ip"), Integer.parseInt(e.getValue().get("output.port"))),
                        "true".equals(e.getValue().get("output.realtime")),
                        startBarrier,
                        stopBarrier);
                nodes.add(producer);
          } else if ("receiver".equals(e.getValue().get("type"))
                    && "tcpclient".equals(e.getValue().get("input"))
                    && "files".equals(e.getValue().get("output"))) {
              //
            } else if ("remotescript".equals(e.getValue().get("type"))
                    && e.getValue().containsKey("host")
                    && e.getValue().containsKey("user")
                    && e.getValue().containsKey("password")) {
                //remoteScripts.put(e.getKey(), e.getValue());
            } else if ("javaprocess".equals(e.getValue().get("type"))) {
                //processes.put(e.getKey(), e.getValue());
            } else if ("localscript".equals(e.getValue().get("type"))) {
                //localScripts.put(e.getKey(), e.getValue());
            }
        }
       LeafLayer layer = new LeafLayer("", startBarrier, stopBarrier, nodes);
        return layer;
    }

    public NestedLayer createLayer(Map<String, Map<String, String>> command, List<String> names, Layer next) {
        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<Node> nodes = new ArrayList<>(command.size());
        for(Map.Entry<String,Map<String,String>> e : command.entrySet()) {
            if ("sender".equals(e.getValue().get("type"))
                    && "files".equals(e.getValue().get("input"))
                    && "tcpserver".equals(e.getValue().get("output"))) {
                 //
            } else if ("receiver".equals(e.getValue().get("type"))
                    && "tcpclient".equals(e.getValue().get("input"))
                    && "files".equals(e.getValue().get("output"))) {
                final PullConsumer consumer = new PullConsumer(getNextAvailablePath(e.getValue().get("output.directory")),
                        names,
                        getMessageReceiver(e.getValue().get("input.format")),
                        new InetSocketAddress(e.getValue().get("input.ip"), Integer.parseInt(e.getValue().get("input.port"))),
                        startBarrier,
                        stopBarrier);
                nodes.add(consumer);
            } else if ("remotescript".equals(e.getValue().get("type"))
                    && e.getValue().containsKey("host")
                    && e.getValue().containsKey("user")
                    && e.getValue().containsKey("password")) {
                //remoteScripts.put(e.getKey(), e.getValue());
            } else if ("javaprocess".equals(e.getValue().get("type"))) {
                final JvmProcess process = new JvmProcess(e.getValue().get("classpath"),
                        e.getValue().get("jvmArguments").split(" "),
                        e.getValue().get("mainClass"),
                        e.getValue().get("programArguments").split(" "),
                        startBarrier,
                        stopBarrier);
            } else if ("localscript".equals(e.getValue().get("type"))) {
                //localScripts.put(e.getKey(), e.getValue());
            }
        }
        NestedLayer layer = new NestedLayer("", names, startBarrier, stopBarrier, nodes, next);
        return layer;
    }

    public Layer walk(messagepipeline.experimental.Node n, Map<String, Map<String, String>> allCommands, List<String> names ){



        Map<String, Map<String, String>> leyarCommands =
            allCommands.entrySet().stream().filter(a->n.layer.contains(a.getKey())).collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue()));
        if(n.children.size()==0) {
            return createLeafLayer( leyarCommands,  names);
        } else {
            Layer l = walk(n.children.get(0), allCommands, names);
            return createLayer(leyarCommands, names, l);
        }
    }

    public void start2(String[] args) {

        Map<String, String> rawProperties = mergeProperties(args);
        Map<String, String> variables = getVariables(rawProperties, "path.");
        Map<String, String> properties = replaceVariables(rawProperties, variables, Arrays.asList(new String[]{"dd-MMM-yy", "yyyy-MMM-dd"}));
        TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(properties.get("run").split("->|,|;")));
        Map<String, String> selectedProperties = filterProperties(properties, nodesToRun);
        Map<String, Map<String, String>> nodeToProperties = wrapProperties(selectedProperties);
        Set<String> dirs = nodeToProperties.entrySet().stream().filter(a->a.getValue().containsKey("input.directory")).map(a->a.getValue().get("input.directory")).collect(Collectors.toSet());
        Path basePath = Paths.get(dirs.iterator().next());
        List<Path> allReaderFileNames = collectPaths(basePath, null);
        List<String> fileNames = allReaderFileNames.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());

        for (String compoundStep: Arrays.asList(properties.get("run").split(";"))) {
            messagepipeline.experimental.Node meta = new messagepipeline.experimental.Node("run", false);
            TestCommand.parse(meta, false, false,false, TestCommand.tokenize(compoundStep).iterator());
            Layer top = walk(meta, nodeToProperties, fileNames);
            top.start();
        }

    }

        @Deprecated
    public void sendReceiveThreeLayers(Map<String,String> producerConfigs, List<Map<String,String>> consumerConfigs, List<Map<String,String>> remoteScriptConfigs) {

        final List<Path> readerFileNames = collectPaths(Paths.get(producerConfigs.get("input.directory")), null);

        final Path outputDir1 = generateConsumerRootDir(Paths.get(consumerConfigs.get(0).get("output.directory")));

        final List<Path> writerFileNames1 = generateConsumerPaths(outputDir1, readerFileNames, Paths.get(producerConfigs.get("input.directory")));

        final Path outputDir2 = generateConsumerRootDir(Paths.get(consumerConfigs.get(1).get("output.directory")));
        //System.out.println("output " + outputDir2);
        final List<Path> writerFileNames2 = generateConsumerPaths(outputDir2, readerFileNames, Paths.get(producerConfigs.get("input.directory")));


        final Path thirdLayerOutputDir1 = generateConsumerRootDir(Paths.get(remoteScriptConfigs.get(0).get("output.directory")));
        //System.out.println("output " + thirdLayerOutputDir1);
        final List<Path>  thirdLayerWriterFileNames1 = generateConsumerPaths(thirdLayerOutputDir1, readerFileNames, Paths.get(producerConfigs.get("input.directory")));

        final Path thirdLayerOutputDir2 = generateConsumerRootDir(Paths.get(remoteScriptConfigs.get(1).get("output.directory")));
        //System.out.println("output " + thirdLayerOutputDir2);
        final List<Path>  thirdLayerWriterFileNames2 = generateConsumerPaths(thirdLayerOutputDir2, readerFileNames, Paths.get(producerConfigs.get("input.directory")));

        final Path thirdLayerOutputDir3 = generateConsumerRootDir(Paths.get(remoteScriptConfigs.get(2).get("output.directory")));
        //System.out.println("output " + thirdLayerOutputDir3);
        final List<Path>  thirdLayerWriterFileNames3 = generateConsumerPaths(thirdLayerOutputDir3, readerFileNames, Paths.get(producerConfigs.get("input.directory")));


        CyclicBarrier barrier = new CyclicBarrier(7); //updated for SSH (1 producer, 2 consumer, 3 ssh))
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
        final List<String> thirdLayerFileNames3 = new ArrayList<>();
        for (Iterator<Path> it = thirdLayerWriterFileNames3.iterator(); it.hasNext(); ) {
            thirdLayerFileNames3.add(it.next().toString());
        }

        final List<String> fileNames = new ArrayList<>();
        for (Iterator<Path> it = writerFileNames1.iterator(); it.hasNext(); ){
            fileNames.add(it.next().toString());
        }
        Iterator<Map<String,String>> thirdLayerIt = remoteScriptConfigs.iterator();
        int i=1;
        while (thirdLayerIt.hasNext()) {
            Map<String,String> elem = thirdLayerIt.next();
            RemoteShellScrip thirdLayerNode = new RemoteShellScrip(
                    elem.get("user"),
                    elem.get("host"),
                    elem.get("password"),
                    elem.get("sudo_pass"),
                    (i ==1 ? thirdLayerFileNames1: ( i==2? thirdLayerFileNames2: thirdLayerFileNames3)),
                    barrier,
                    getShellScriptGenerator(elem.get("command")));
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

        ArrayList x = new ArrayList<>(1);
        x.add(thirdLayer.get(0));
        DeprecatedPullConsumer consumer1 = new DeprecatedPullConsumer(writerFileNames1,
                getMessageReceiver(consumerConfigs.get(0).get("input.format")),
                new InetSocketAddress(consumerConfigs.get(0).get("input.ip"), Integer.parseInt(consumerConfigs.get(0).get("input.port"))),
                barrier,
                x);
        ArrayList y = new ArrayList<>(2);
        y.add(thirdLayer.get(1));
        y.add(thirdLayer.get(2));
        DeprecatedPullConsumer consumer2 = new DeprecatedPullConsumer(writerFileNames2,
                getMessageReceiver(consumerConfigs.get(1).get("input.format")),
                new InetSocketAddress(consumerConfigs.get(1).get("input.ip"), Integer.parseInt(consumerConfigs.get(1).get("input.port"))),
                barrier,
                y);
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

        //System.out.println("Consumer start!");
        consumerThread1.start();
        consumerThread2.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println("Producer start!");
        producerThread.start();
        try {
            producerThread.join();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //logger.info("Terminating consumer!");
        //consumer.terminate();
        //System.out.println("Awaiting join consumer!");
        try {
            consumerThread1.join();
            consumerThread2.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        done.countDown();
        //}
        logger.info("done");
        System.out.print(String.format("Done [100 %%] %60s\n", "                                                        "));
    }

    public static String replace(String value, Map<String,String> variables) {
        for(Map.Entry<String,String> var: variables.entrySet()) {
            if(value.contains("{"+var.getKey()+"}")){
                value = value.replace("{"+var.getKey()+"}", var.getValue());
            }
        }
        return value;
    }

    private static String getNextAvailablePath(String name){
        if (Files.exists(Paths.get(name))) {
            int i = 1;
            while (Files.exists(Paths.get(name + "-" + i))) {
                i++;
            }
            name = name + "-" + i;
        }
        return name;
    }

    private List<Path> collectPaths(Path dirOrFilePath, String ignore) {
        if(Files.notExists(dirOrFilePath)) {
            try {
                Files.createDirectories(dirOrFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (Files.isDirectory(dirOrFilePath, LinkOption.NOFOLLOW_LINKS)) {
            RecursiveFileCollector walk = new RecursiveFileCollector(ignore);
            try {
                Files.walkFileTree(dirOrFilePath, walk);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return walk.getResult();
        } else {
            List<Path> singleFile = new ArrayList<>();
            singleFile.add(dirOrFilePath);
            return singleFile;
        }
    }
    @Deprecated
    private static Path generateConsumerRootDir(final Path outputDir) {
        if (Files.notExists(outputDir)) {
            try {
                Files.createDirectories(outputDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return outputDir;
    }
    @Deprecated
    private static List<Path> generateConsumerPaths(Path outputDir, List<Path> producerInputPath, Path inputDir) {
        final List<Path> readerFileNames = new ArrayList<>();
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
        private final List<Path> result = new ArrayList<>();
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