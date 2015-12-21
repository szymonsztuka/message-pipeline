package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LayerController implements Runnable {

    private final CyclicBarrier layer1batchStart;
    private final CyclicBarrier layer1batchEnd;
    private final List<NonBlockingConsumerEagerInDecoupled> consumers;
    private final CyclicBarrier layer2batchStart;
    private final CyclicBarrier layer2batchEnd;
    private final List<PullPushMultiProducerDecoupled> producers;

    private static final Logger logger = LoggerFactory.getLogger(LayerController.class);

    public LayerController(CyclicBarrier layer1batchStart, CyclicBarrier layer1batchEnd, List<NonBlockingConsumerEagerInDecoupled> consumers, CyclicBarrier layer2batchStart, CyclicBarrier layer2batchEnd, List<PullPushMultiProducerDecoupled> producers) {
        this.layer1batchStart = layer1batchStart;
        this.layer1batchEnd = layer1batchEnd;
        this.consumers = consumers;
        this.layer2batchStart = layer2batchStart;
        this.layer2batchEnd = layer2batchEnd;
        this.producers = producers;
    }

    @Override
    public void run() {
        while (!allProducersDone()) {
            logger.info("batch start");
            try {
                layer1batchStart.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            try {
                layer2batchStart.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            try {
                layer2batchEnd.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            consumers.forEach(NonBlockingConsumerEagerInDecoupled::signalBatchEnd);
            try {
                layer1batchEnd.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }

        }
        logger.info("done");
    }

    private boolean allProducersDone() {
        boolean reread;
        boolean result = true;
        do {
            reread = false;
            try {
                for (PullPushMultiProducerDecoupled prod : producers) {
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
