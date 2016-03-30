package messagepipeline.topology;

import messagepipeline.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class Runner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private final Node node;
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final String name;
    private Path path;
    private volatile boolean process = true;

    public Runner(String name, Node node, CyclicBarrier start, CyclicBarrier end) {
        this.node = node;
        this.batchStart = start;
        this.batchEnd = end;
        this.process = true;
        this.name = name;
    }

    public void finish() {
        process = false;
    }

    public final String getName() {
        return name;
    }

    public final void addStep(String p) {
        if (p != null && !"".equals(p)) {
            path = Paths.get(p);
        } else {
            path = null;
        }
    }

    public void signalStepEnd() {
        node.signalStepEnd();
    }

    public void run() {
        node.start();
        while (process) {
            try {
                //logger.debug(name + " s " + batchStart.getParties() + " " + batchStart.getNumberWaiting() + " " + node.getClass().getName().toString() + " " + process);
                batchStart.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            node.step(path);

            try {
                //logger.debug(name + " e " + batchStart.getParties() + " " + batchStart.getNumberWaiting());
                batchEnd.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
        node.end();
    }
}
