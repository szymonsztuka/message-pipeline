package serverchainsimulator.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LayerControllerRecursiveStateful implements Runnable, LayerControllerDecorator {

    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends Stoppable> nodes;


    final private LayerControllerDecorator next;
    private static final Logger logger = LoggerFactory.getLogger(LayerControllerRecursiveStateful.class);
    private final String name;

    public LayerControllerRecursiveStateful(String name, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends Stoppable> nodes, LayerControllerDecorator next) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.next = next;
        this.name = name;
    }

    public boolean step(){
        boolean result =false;
        if(next!=null) {
            result = next.step();
        }
        return result;
    }

    @Override
    public void run() {

        logger.info(name + " awaiting...");

        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " ...starts");

        boolean run = true;
        //while (!allProducersDone()) {


        //}
        while(run) {
            run = step();
        }
        nodes.forEach(Stoppable::signalBatchEnd);
        logger.info(name + " finishing...");
        try {
            batchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " ...finished");
    }

}
