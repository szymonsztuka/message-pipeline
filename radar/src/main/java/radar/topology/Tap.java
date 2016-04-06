package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class Tap implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Tap.class);

    private final Pipeline pipeline;
    private final List<String> steps;

    public Tap(List<String> steps, Pipeline pipeline) {
        this.steps = steps;
        this.pipeline = pipeline;
    }

    @Override
    public void run() {
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
        logger.info("end");
    }
}
