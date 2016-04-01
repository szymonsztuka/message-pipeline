package messagepipeline;

import messagepipeline.lang.CommandBuilder;
import messagepipeline.lang.PropertiesParser;
import messagepipeline.message.CodecFactoryMethod;
import messagepipeline.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private final CodecFactoryMethod codecFactoryMethod;

    public Bootstrap(CodecFactoryMethod codecFactoryMethod) {
        this.codecFactoryMethod = codecFactoryMethod;
    }

    public void run(String[] args) {

        PropertiesParser propertiesParser = new PropertiesParser(args);

        for (String compoundStep : propertiesParser.nodeSequences) { //TODO move to sequence?

            Set<String> parentKeys = new HashSet<>();//TODO move to PropertiesParser
            Collections.addAll(parentKeys, compoundStep.split(",|;|\\(|\\)"));
            Map<String, String> dataStreamPath = PropertiesParser.getParentKeyToChildProperty(propertiesParser.nodeToProperties, parentKeys, "input");
            logger.info("Interpreting " + compoundStep + " with " + dataStreamPath);
            List<String> fileNames = Collections.EMPTY_LIST;
            if (dataStreamPath.size() > 0) {
                String firstKey = dataStreamPath.keySet().iterator().next();
                Path basePath = Paths.get(dataStreamPath.get(firstKey));
                if (Files.isDirectory(basePath, LinkOption.NOFOLLOW_LINKS)) {  //TODO move to CommandBuilder or TopologyBuilder ?
                    String ignore = null;
                    RecursiveFileCollector walk = new RecursiveFileCollector(ignore);
                    try {
                        Files.walkFileTree(basePath, walk);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    fileNames = walk.result.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());
                    logger.info("Interpreting " + compoundStep + " with " + fileNames.size() + " steps from " + basePath + " brought by " + firstKey + ".input");
                } else {
                    logger.warn("Not found " + basePath);
                }
            } else {
                logger.warn("No dataStreamPath found ");
            }

            CommandBuilder commandBuilder = new CommandBuilder(compoundStep);
            TopologyBuilder topologyBuilder = new TopologyBuilder(commandBuilder.command,
                    propertiesParser.nodeToProperties,
                    fileNames,
                    codecFactoryMethod);
            Thread th = new Thread(topologyBuilder.sequence, topologyBuilder.sequence.getName());
            th.start();
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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