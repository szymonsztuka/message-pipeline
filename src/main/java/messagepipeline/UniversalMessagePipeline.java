package messagepipeline;

import messagepipeline.experimental.TestCommand;
import messagepipeline.message.MessageGenerator;
import messagepipeline.message.MessageReceiver;
import messagepipeline.message.ShellScriptGenerator;
import messagepipeline.pipeline.node.*;
import messagepipeline.pipeline.topology.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public abstract class UniversalMessagePipeline {

    private static final Logger logger = LoggerFactory.getLogger(UniversalMessagePipeline.class);

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
                            Path root = Paths.get(UniversalMessagePipeline.class.getProtectionDomain().getCodeSource().getLocation().toURI());
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

    public UniversalLayer createLayer(Map<String, Map<String, String>> command, List<String> names, UniversalLayer next) {
        logger.info("NestedLayer "+command.keySet()+ " "+ (next!=null?next.getName():"") );
        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<UniversalNode> nodes = new ArrayList<>(command.size());
        boolean steps = false;
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
                ServerRunner producer = new ServerRunner(
                        e.getKey(),
                        e.getValue().get("input.directory"),
                        generators,
                        new InetSocketAddress(e.getValue().get("output.ip"), Integer.parseInt(e.getValue().get("output.port"))),
                        //"true".equals(e.getValue().get("output.realtime")),
                        startBarrier,
                        stopBarrier);
                nodes.add(producer);
            } else if ("receiver".equals(e.getValue().get("type"))
                    && "tcpclient".equals(e.getValue().get("input"))
                    && "files".equals(e.getValue().get("output"))) {
                final ClientRunner consumer = new ClientRunner(
                        e.getKey(),
                        getNextAvailablePath(e.getValue().get("output.directory")),
                        getMessageReceiver(e.getValue().get("input.format")),
                        new InetSocketAddress(e.getValue().get("input.ip"), Integer.parseInt(e.getValue().get("input.port"))),
                        startBarrier,
                        stopBarrier);
                nodes.add(consumer);
                steps = true;
            } else if ("remotescript".equals(e.getValue().get("type"))
                    && e.getValue().containsKey("host")
                    && e.getValue().containsKey("user")
                    && e.getValue().containsKey("password")) {
                //remoteScripts.put(e.getKey(), e.getValue());
            } else if ("javaprocess".equals(e.getValue().get("type"))) {
                final UniversalJvmProcessRunner process = new UniversalJvmProcessRunner(
                        e.getKey(),
                        e.getValue().get("classpath"),
                        e.getValue().get("jvmArguments").split(" "),
                        e.getValue().get("mainClass"),
                        e.getValue().get("programArguments").split(" "),
                        e.getValue().get("processLogFile"),
                        startBarrier,
                        stopBarrier
                        );
                nodes.add(process);
                steps = true;
            } else if ("localscript".equals(e.getValue().get("type"))) {
                LocalScript ls = new LocalScript(e.getValue().get("script"),startBarrier,
                        stopBarrier);
//                nodes.add(ls);
            }
        }
        List<String> stepNames;
        if(steps) {
            stepNames = names;
        } else{
            stepNames = new ArrayList<>();
            stepNames.add("1");
        }
        UniversalLayer layer = new UniversalLayer(command.keySet().toString(), stepNames, startBarrier, stopBarrier, nodes, next);
        return layer;
    }

    public UniversalLayer walk(messagepipeline.experimental.Node n, Map<String, Map<String, String>> allCommands, List<String> names ){

        Map<String, Map<String, String>> leyarCommands =
            allCommands.entrySet().stream().filter(a->n.layer.contains(a.getKey())).collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue()));
        if(n.children.size()==0) {
            return createLayer( leyarCommands,  names, null);
        } else {
            UniversalLayer l = walk(n.children.get(0), allCommands, names);
            return createLayer(leyarCommands, names, l);
        }
    }

    public void start(String[] args) {

        Map<String, String> rawProperties = mergeProperties(args);
        Map<String, String> variables = getVariables(rawProperties, "path.");
        Map<String, String> properties = replaceVariables(rawProperties, variables, Arrays.asList(new String[]{"dd-MMM-yy", "yyyy-MMM-dd"}));
        TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(properties.get("run").split(",|;|\\(|\\)")));
        Map<String, String> selectedProperties = filterProperties(properties, nodesToRun);
        Map<String, Map<String, String>> nodeToProperties = wrapProperties(selectedProperties);
        Set<String> dirs = nodeToProperties.entrySet().stream().filter(a->a.getValue().containsKey("input.directory")).map(a->a.getValue().get("input.directory")).collect(Collectors.toSet());
        Path basePath = Paths.get(dirs.iterator().next());
        List<Path> allReaderFileNames = collectPaths(basePath, null);
        List<String> fileNames = allReaderFileNames.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());

        for (String compoundStep: Arrays.asList(properties.get("run").split(";"))) {
            logger.info("---------------compoundStep "+compoundStep);
            messagepipeline.experimental.Node meta = new messagepipeline.experimental.Node("run", false);
            TestCommand.parse(meta, false, false,false, TestCommand.tokenize(compoundStep).iterator());
            UniversalLayer top = walk(meta.children.get(0), nodeToProperties, fileNames);
            Thread th = new Thread(top, top.getName());
            th.start();
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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