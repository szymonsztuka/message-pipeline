package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class Pipeline implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final CyclicBarrier stepStart;
    private final CyclicBarrier stepEnd;
    private final List<Node> nodes;
    private final List<Pipeline> childPipelines;
    private final List<String> steps;
    private final List<Thread> threads;

    public Pipeline(List<String> steps, CyclicBarrier stepStart, CyclicBarrier stepEnd, List<Node> nodes, List<Pipeline> childPipelines) {
        this.stepStart = stepStart;
        this.stepEnd = stepEnd;
        this.nodes = nodes;
        this.threads = new ArrayList<>(this.nodes.size());
        this.nodes.stream().forEach(e -> this.threads.add(new Thread(e, e.getName())));
        this.childPipelines = childPipelines;
        this.steps = steps;
    }

    /** Recursive call */
    public void startRunners() {
        threads.forEach(Thread::start);
        if (childPipelines != null) {//TODO !=
            childPipelines.stream().forEach(Pipeline::startRunners);
        }
    }

    /** Recursive call */
    public void step(String stepName, boolean last) {
        nodes.forEach(e -> e.addStep(stepName));
        try {
            logger.trace(stepName + " start " + stepStart.getParties() + " " + stepStart.getNumberWaiting() );
            stepStart.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
        if (childPipelines != null) { //TODO !=
            childPipelines.stream().forEach(e -> e.step(stepName, last));
        }
        nodes.forEach(Node::signalStepEnd);
        if (last) {
            nodes.forEach(Node::finish);
        }
        try {
            logger.trace(stepName + " end " + stepEnd.getParties() + " " + stepEnd.getNumberWaiting());
            stepEnd.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        startRunners();
        if (steps.size() == 0) {
            logger.debug("once off run (number of steps to process:" + steps.size() + ")");
            step("", false);
        } else {
            logger.debug("number of steps to process:" + steps.size());
            Iterator<String> it = steps.iterator();
            while (it.hasNext()) {
                String elem = it.next();
                step(elem, !it.hasNext());
            }
        }
        try {
            for (Thread th : threads)
                th.join();
        } catch (InterruptedException e) {
           logger.error(e.getMessage(),e);
        }
        logger.info("end");
    }

    public String toString() {
        String res = "{" + nodes.toString() + "}";
        if (childPipelines != null) {//TODO !=
            for (Pipeline e : childPipelines) {
                res = res + e.toString();
            }
        }
        return res;
    }
}
