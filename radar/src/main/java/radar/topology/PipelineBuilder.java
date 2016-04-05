package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radar.conf.Command;
import radar.conf.PropertiesParser;
import radar.processor.*;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class PipelineBuilder {

    private static final Logger logger = LoggerFactory.getLogger(PipelineBuilder.class);

    private final NodeFactory nodeFactory;

    public final List<Pipeline> pipelines = new ArrayList<>(1);

    public PipelineBuilder(NodeFactory nodeFactory, Command command, Map<String, Map<String, String>> nodeToProperties) {

        this.nodeFactory = nodeFactory;
        for (Command child: command.childCommands) {
            Map<String, String> dataStreamPath = PropertiesParser.getParentKeyToChildProperty(nodeToProperties, child.getAllNames(), "input");
            List<String> fileNames = Collections.EMPTY_LIST;
            if (dataStreamPath.size() > 0) {
                String firstKey = dataStreamPath.keySet().iterator().next();
                Path basePath = Paths.get(dataStreamPath.get(firstKey));
                if (Files.isDirectory(basePath, LinkOption.NOFOLLOW_LINKS)) {
                    RecursiveFileCollector walk = new RecursiveFileCollector();
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
            pipelines.add(parseSteps(child, nodeToProperties, fileNames));
        }
    }

    private Pipeline parseSteps(Command command, Map<String, Map<String, String>> allCommands, List<String> steps) {

        Map<String, Map<String, String>> sequenceCommand = allCommands.entrySet().stream()
                .filter(a -> command.names.contains(a.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        List<Pipeline> childPipelines = command.childCommands.stream()
                .map(e -> parseSteps(e, allCommands, steps))
                .collect(Collectors.toList());
        return createSequence(sequenceCommand, steps, childPipelines);
    }

    private Pipeline createSequence(Map<String, Map<String, String>> command, List<String> names, List<Pipeline> childPipelines) {

        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<Node> nodes = new ArrayList<>(command.size());
        for (Map.Entry<String, Map<String, String>> entry : command.entrySet()) {
            Processor processor = nodeFactory.createNode(entry);
            if (processor != null) { //TODO nodes number should equal command.size() otherwise Pipeline will block on a barrier
                int stepEndDelay = 0;
                try {
                    stepEndDelay = Integer.parseInt(entry.getValue().get("stepEndDelay"));
                } catch (NumberFormatException e) {
                    logger.warn(e.getMessage());
                }
                nodes.add(new Node(entry.getKey(), processor, startBarrier, stopBarrier, stepEndDelay));
            }
        }
        if (names.size() == 0) {
            names = new ArrayList(1);
            names.add("1"); //TODO once off sequence
        }
        return new Pipeline(names, startBarrier, stopBarrier, nodes, childPipelines);
    }

    private class RecursiveFileCollector extends SimpleFileVisitor<Path> {

        public final List<Path> result = new ArrayList<>();

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            result.add(file);
            return FileVisitResult.CONTINUE;
        }
    }
}
