package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LayerControllerRecursiveHead implements LayerControllerDecorator {

    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends SelfStopable> nodes;

    private static final Logger logger = LoggerFactory.getLogger(LayerControllerRecursiveHead.class);

    public LayerControllerRecursiveHead(CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends SelfStopable> nodes) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
    }
    
    public boolean step(){
        logger.info("batch start");
        boolean result =false;
        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        try {
            batchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        result = !allProducersDone();
        logger.info("terminal node result " + result);
        return result;
    }

    private boolean allProducersDone() {
        boolean reread;
        boolean result = true;
        do {
            reread = false;
            try {
                for (SelfStopable prod : nodes) {
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
