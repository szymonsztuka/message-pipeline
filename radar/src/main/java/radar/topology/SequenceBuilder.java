package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radar.conf.Command;
import radar.conf.PropertiesParser;
import radar.node.*;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class SequenceBuilder {

    private static final Logger logger = LoggerFactory.getLogger(SequenceBuilder.class);

    private final NodeFactory nodeFactory;

    public final List<Sequence> sequences = new ArrayList<>(1);

    public SequenceBuilder(NodeFactory nodeFactory, Command command, Map<String, Map<String, String>> nodeToProperties) {
        this.nodeFactory = nodeFactory;
        for (Command child: command.children) {
            Map<String, String> dataStreamPath = PropertiesParser.getParentKeyToChildProperty(nodeToProperties, child.getAllNames(), "input");
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
                }
                logger.info(String.format("Interpreting topology with %s steps from %s.input=%s:\n%s", fileNames.size() , firstKey, basePath, child));
            } else {
                logger.info("Interpreting topology without steps (no property '.input' provided within the scope):\n" + child );
            }
            sequences.add(parseSteps(child, nodeToProperties, fileNames));
        }
    }

    private Sequence parseSteps(Command command, Map<String, Map<String, String>> allCommands, List<String> steps) {

        Map<String, Map<String, String>> sequenceCommand = allCommands.entrySet().stream()
                .filter(a -> command.layer.contains(a.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        List<Sequence> childSequences = command.children.stream()
                .map(e -> parseSteps(e, allCommands, steps))
                .collect(Collectors.toList());
        return createSequence(sequenceCommand, steps, childSequences);
    }

    private Sequence createSequence(Map<String, Map<String, String>> command, List<String> names, List<Sequence> childSequences) {

        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<Runner> runners = new ArrayList<>(command.size());
        for (Map.Entry<String, Map<String, String>> e : command.entrySet()) {
            Node node = nodeFactory.createNode(e);
            if (node != null) { //TODO nodes number should equal command.size() otherwise Sequence will block on a barrier
                runners.add(new Runner(e.getKey(), node, startBarrier, stopBarrier));
            }
        }
        if (names.size() == 0) {
            names = new ArrayList(1);
            names.add("1"); //TODO once off sequence
        }
        return new Sequence(names, startBarrier, stopBarrier, runners, childSequences, 1000);
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
