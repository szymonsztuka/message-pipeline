package serverchainsimulator.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LayerControllerTerminal implements LayerControllerDecorator {

    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends SelfStoppable> nodes;
    private final String name;
    private static final Logger logger = LoggerFactory.getLogger(LayerControllerTerminal.class);

    public LayerControllerTerminal(String name, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends SelfStoppable> nodes) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.name = name;
    }
    
    public boolean step(){
        logger.info(name + " awaiting...");
        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " finishing...");
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
                for (SelfStoppable prod : nodes) {
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
