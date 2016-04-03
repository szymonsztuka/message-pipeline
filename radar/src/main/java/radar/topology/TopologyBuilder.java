package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radar.conf.Command;
import radar.conf.PropertiesParser;
import radar.message.CodecFactoryMethod;
import radar.node.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class TopologyBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TopologyBuilder.class);

    public final List<Sequence> sequences = new ArrayList<>(1);

    public TopologyBuilder(Command command, Map<String, Map<String, String>> nodeToProperties, CodecFactoryMethod codecFactoryMethod) {
        for (Command child : command.children) {
            Map<String, String> dataStreamPath = PropertiesParser.getParentKeyToChildProperty(nodeToProperties, child.getAllNames(), "input");
            logger.info("Interpreting " + child.layer + " with " + dataStreamPath + " " + child.getAllNames());
            List<String> fileNames = Collections.EMPTY_LIST;
            if (dataStreamPath.size() > 0) {
                String firstKey = dataStreamPath.keySet().iterator().next();
                Path basePath = Paths.get(dataStreamPath.get(firstKey));
                if (Files.isDirectory(basePath, LinkOption.NOFOLLOW_LINKS)) {
                    String ignore = null;
                    RecursiveFileCollector walk = new RecursiveFileCollector(ignore);
                    try {
                        Files.walkFileTree(basePath, walk);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    fileNames = walk.result.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());
                    logger.info("Interpreting:\n" + child + " with " + fileNames.size() + " steps from " + basePath + " brought by " + firstKey + ".input");
                } else {
                    logger.warn("Not found " + basePath);
                }
            } else {
                logger.warn("No dataStreamPath found ");
            }
            sequences.add(parseSteps(child, nodeToProperties, fileNames, codecFactoryMethod));
        }
    }

    public static Sequence parseSteps(Command command, Map<String, Map<String, String>> allCommands, List<String> steps, CodecFactoryMethod codecFactoryMethod) {

        Map<String, Map<String, String>> sequenceCommand = allCommands.entrySet().stream()
                .filter(a -> command.layer.contains(a.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        List<Sequence> childSequences = command.children.stream()
                .map(e -> parseSteps(e, allCommands, steps, codecFactoryMethod))
                .collect(Collectors.toList());
        logger.info("create sequence " + sequenceCommand.keySet() + " " + childSequences.size());
        return createSequence(sequenceCommand, steps, childSequences, codecFactoryMethod);
    }

    public static Sequence createSequence(Map<String, Map<String, String>> command, List<String> names, List<Sequence> childSequences, CodecFactoryMethod codecFactoryMethod) {

        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<Runner> runners = new ArrayList<>(command.size());
        for (Map.Entry<String, Map<String, String>> e : command.entrySet()) {
            Node node = createNode(e, codecFactoryMethod);
            if (node != null) {
                runners.add(new Runner(e.getKey(), node, startBarrier, stopBarrier));
            }
        }
        if (names.size() == 0) {
            names = new ArrayList(1);
            names.add("1"); //TODO once off sequence
        }
        Sequence layer = new Sequence(names, startBarrier, stopBarrier, runners, childSequences, 1000);
        return layer;
    }

    public static Node createNode(Map.Entry<String, Map<String, String>> e, CodecFactoryMethod codecFactoryMethod) {

        if ("sender".equals(e.getValue().get("type"))) {
            int clientsNumber;
            if (Integer.parseInt(e.getValue().get("connections.number")) > 0) {
                clientsNumber = Integer.parseInt(e.getValue().get("connections.number"));
            } else {
                clientsNumber = 1;
            }
            List generators = new ArrayList<>(clientsNumber);
            for (int j = 0; j < clientsNumber; j++) {
                generators.add(codecFactoryMethod.getMessageEncoder(e.getValue().get("encoder")));
            }
            return new SocketWriter(Paths.get(e.getValue().get("input")),
                    new InetSocketAddress(e.getValue().get("ip"), Integer.parseInt(e.getValue().get("port"))),
                    generators);
        } else if ("receiver".equals(e.getValue().get("type"))) {
            return new SocketReader(
                    new InetSocketAddress(e.getValue().get("ip"), Integer.parseInt(e.getValue().get("port"))),
                    Paths.get(e.getValue().get("output")),
                    codecFactoryMethod.getMessageDecoder(e.getValue().get("decoder")));
        } else if ("localscript".equals(e.getValue().get("type"))) {
            return new LocalScript(e.getValue().get("script"));
        } else if ("remotescript".equals(e.getValue().get("type"))) {
            return new SshScript(
                    Paths.get(e.getValue().get("output")),
                    e.getValue().get("user"),
                    e.getValue().get("host"),
                    e.getValue().get("password"),
                    codecFactoryMethod.getScriptGenerator(e.getValue().get("oldCommand")));
        } else if ("filedownload".equals(e.getValue().get("type"))) {
            return new SshScript(
                    Paths.get(e.getValue().get("output")),
                    e.getValue().get("user"),
                    e.getValue().get("host"),
                    e.getValue().get("password"),
                    codecFactoryMethod.getScriptGenerator(e.getValue().get("oldCommand")));
        } else if ("jvmprocess".equals(e.getValue().get("type"))) {
            return new LongJvmProcess(
                    e.getValue().get("classpath"),
                    e.getValue().get("jvmArguments").split(" "),
                    e.getValue().get("mainClass"),
                    e.getValue().get("programArguments").split(" "),
                    e.getValue().get("processLogFile"));
        } else if ("jvmshortprocess".equals(e.getValue().get("type"))) {
            return new ShortJvmProcess(
                    e.getValue().get("classpath"),
                    e.getValue().get("jvmArguments").split(" "),
                    e.getValue().get("mainClass"),
                    e.getValue().get("programArguments").split(" "),
                    e.getValue().get("processLogFile"));
        } else {
            return null;
        }
    }

    private class RecursiveFileCollector extends SimpleFileVisitor<Path> {
        public final List<Path> result = new ArrayList<>();
        private final String ignore;
        public RecursiveFileCollector(String ignore) {
            this.ignore = ignore;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (dir.getFileName().toString().equals(ignore)) {
                return FileVisitResult.SKIP_SUBTREE;
            } else {
                return super.preVisitDirectory(dir, attrs);
            }
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            result.add(file);
            return FileVisitResult.CONTINUE;
        }
    }
}
