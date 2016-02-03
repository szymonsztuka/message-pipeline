package serverchainsimulator.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serverchainsimulator.transport.Node;

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

    public NestedLayer(String name, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends Node> nodes, Layer next) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.next = next;
        this.name = name;
    }

    public boolean step(){
        logger.info(name + " awaiting...");
        boolean result =false;
        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " ...starting");
        if(next!=null) {
            result = next.step();
        }
        nodes.forEach(Node::signalBatchEnd);
        logger.info(name + " finishing...");
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
        while(run) {
            run = step();
        }
        logger.info("done");
    }
}
