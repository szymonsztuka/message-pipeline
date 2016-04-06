package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

public class Tap implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Tap.class);

    private final Pipeline pipeline;
    private final Map<String, String> dataStreamPaths;

    public Tap(Map<String, String> dataStreamPaths, Pipeline pipeline) {
        this.dataStreamPaths = dataStreamPaths;
        this.pipeline = pipeline;
    }

    @Override
    public void run() {

        List<String> steps = Collections.EMPTY_LIST;
        if (dataStreamPaths.size() > 0) {
            String firstKey = dataStreamPaths.keySet().iterator().next();
            Path basePath = Paths.get(dataStreamPaths.get(firstKey));
            if (Files.isDirectory(basePath, LinkOption.NOFOLLOW_LINKS)) {
                RecursiveFileCollector walk = new RecursiveFileCollector();
                try {
                    Files.walkFileTree(basePath, walk);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                steps = walk.result.stream().map(p -> basePath.relativize(p)).map(Path::toString).collect(Collectors.toList());
            }
            logger.info(String.format("Resolving tap with %s steps from %s.input=%s with pipeline %s", steps.size(), firstKey, basePath, pipeline));
         }

        pipeline.startNodes();
        if (steps.size() == 0) {
            logger.info(String.format("Running tap with default once off step with pipeline %s", pipeline));
            pipeline.step("", true);
        } else {
            logger.info(String.format("Running tap with %s steps with pipeline %s", steps.size() , pipeline));
            Iterator<String> it = steps.iterator();
            while (it.hasNext()) {
                String elem = it.next();
                pipeline.step(elem, !it.hasNext());
            }
        }
        logger.info("End" + pipeline);
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
