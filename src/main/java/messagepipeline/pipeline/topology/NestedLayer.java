package messagepipeline.pipeline.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.pipeline.node.Node;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class NestedLayer implements Runnable, Layer {

    private static final Logger logger = LoggerFactory.getLogger(NestedLayer.class);
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends Node> nodes;
    final private Layer next;
    private final String name;
    private final List<String> fileNames;

    public NestedLayer(String name, List<String> names, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends Node> nodes, Layer next) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.next = next;
        this.name = name;
        this.fileNames = names;
    }

    public boolean step(String stepName){
        logger.info(name + " awaiting " + stepName + " ...");
        boolean result =false;
        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.debug(name + " ...starting");
        if(next!=null) {
            result = next.step(stepName);
        }
        nodes.forEach(Node::signalBatchEnd);
        logger.debug(name + " finishing...");
        try {
            batchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " ...finished");
        return result;
    }

    @Override
    public void run() {
        boolean run = true;
        Iterator<String> nameIterator = fileNames.iterator();
        while(run) {
            run = step(nameIterator.next());
        }
        logger.info("done");
    }
}
