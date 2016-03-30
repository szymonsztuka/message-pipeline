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

public class Bootstarp {

    private static final Logger logger = LoggerFactory.getLogger(Bootstarp.class);

    private final CodecFactoryMethod codecFactoryMethod;

    public Bootstarp(CodecFactoryMethod codecFactoryMethod) {
        this.codecFactoryMethod = codecFactoryMethod;
    }

    public void run(String[] args) {

        PropertiesParser propertiesParser = new PropertiesParser(args);

        Path basePath = Paths.get(propertiesParser.dirWithSteps); //TODO move to concrete node, callback from within sequencer
        List<Path> inputDataPaths = Collections.EMPTY_LIST;
        if (Files.isDirectory(basePath, LinkOption.NOFOLLOW_LINKS)) {
            String ignore = null;
            RecursiveFileCollector walk = new RecursiveFileCollector(ignore);
            try {
                Files.walkFileTree(basePath, walk);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            inputDataPaths = walk.result;
        } else {
            logger.warn("Not found " + basePath );
        }
        List<String> fileNames = inputDataPaths.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());

        for (String compoundStep : propertiesParser.nodeSequences) { //TODO move to sequence
            logger.info("Interpreting " + compoundStep);
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