package messagepipeline.pipeline.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.pipeline.node.LeafNode;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LeafLayer implements Layer {

    private static final Logger logger = LoggerFactory.getLogger(LeafLayer.class);
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends LeafNode> nodes;
    private final String name;

    public LeafLayer(String name, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends LeafNode> nodes) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.name = name;
    }

    public void start(){
        List<Thread> threads = new ArrayList<>(nodes.size());
        threads.add(new Thread((Runnable)nodes,nodes.getClass().getName()));
        threads.forEach(Thread::start);
    }

    public boolean step(String stepName){
        logger.info(name + " awaiting " + stepName + " ...");
        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.debug(name + " finishing...");
        try {
            batchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        boolean result = !allProducersDone();
        logger.info(name + " ...finished" + ((!result)?", result=false":""));
        return result;
    }

    private boolean allProducersDone() {
        boolean reread;
        boolean result = true;
        do {
            reread = false;
            try {
                for (LeafNode prod : nodes) {
                    if (!prod.isDone()) {
                        result = false;
                    }
                }
            } catch (ConcurrentModificationException e) {
                reread = true;
            }
        } while (reread);
        return result;
    }
}
