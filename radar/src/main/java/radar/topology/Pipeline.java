package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class Pipeline {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final CyclicBarrier stepStart;
    private final CyclicBarrier stepEnd;
    private final List<Node> nodes;
    private final List<Pipeline> childPipelines;
    private final List<Thread> threads;

    public Pipeline(CyclicBarrier stepStart, CyclicBarrier stepEnd, List<Node> nodes, List<Pipeline> childPipelines) {
        this.stepStart = stepStart;
        this.stepEnd = stepEnd;
        this.nodes = nodes;
        this.threads = new ArrayList<>(this.nodes.size());
        this.nodes.stream().forEach(e -> this.threads.add(new Thread(e, e.getName())));
        this.childPipelines = childPipelines != null ? childPipelines : Collections.EMPTY_LIST;
    }

    /** Recursive call */
    public void startNodes() {
        threads.forEach(Thread::start);
        childPipelines.stream().forEach(Pipeline::startNodes);
    }

    /** Recursive call */
    public void step(String name, boolean last) {
        nodes.forEach(e -> e.addStep(name));
        try {
            logger.trace(name + " start " + stepStart.getParties() + " " + stepStart.getNumberWaiting() );
            stepStart.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
        childPipelines.stream().forEach(e -> e.step(name, last));
        nodes.forEach(Node::signalStepEnd);
        if (last) {
            nodes.forEach(Node::finish);
        }
        try {
            logger.trace(name + " end " + stepEnd.getParties() + " " + stepEnd.getNumberWaiting());
            stepEnd.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error(e.getMessage(), e);
        }
    }
    @Override
    public String toString() {
        String res = "({" + nodes.toString().substring(1,nodes.toString().length()-1)+"}" ;
        for (Pipeline e : childPipelines) {
              res = res + e.toString();
        }
        return res + ")";
    }
}
