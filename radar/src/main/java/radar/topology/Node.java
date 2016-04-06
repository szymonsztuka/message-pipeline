package radar.topology;

import radar.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class Node implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    private final Processor processor;
    private final CyclicBarrier stepStart;
    private final CyclicBarrier stepEnd;
    private final String name;
    private Path path;
    private final int millisecondsStepEndDelay;
    private volatile boolean process = true;

    public Node(String name, Processor processor, CyclicBarrier stepStart, CyclicBarrier stepEnd, int millisecondsStepEndDelay) {
        this.name = name;
        this.processor = processor;
        this.stepStart = stepStart;
        this.stepEnd = stepEnd;
        this.millisecondsStepEndDelay = millisecondsStepEndDelay;
    }

    public void finish() {
        process = false;
    }

    public final String getName() {
        return name;
    }

    public final void addStep(String step) {
        if (step != null && !"".equals(step)) {
            path = Paths.get(step);
        } else {
            path = null;
        }
    }

    public void signalStepEnd() {
        processor.signalStepEnd();
    }

    public void run() {
        processor.start();
        while (process) {
            try {
                logger.trace(name + " start " + stepStart.getParties() + " " + stepStart.getNumberWaiting() + " " + processor.getClass().getName().toString() + " " + process);
                stepStart.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                logger.error(e.getMessage(), e);
            }
            processor.step(path);
            try {
                logger.trace(name + " end " + stepStart.getParties() + " " + stepStart.getNumberWaiting());
                if( millisecondsStepEndDelay > 0){
                    logger.trace(name + " sleep " + millisecondsStepEndDelay);
                    Thread.sleep(millisecondsStepEndDelay);
                }
                stepEnd.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                logger.error(e.getMessage(), e);
            }
        }
        processor.end();
    }
    @Override
    public String toString() {
        return processor.getClass().getSimpleName().toString();
    }
}
