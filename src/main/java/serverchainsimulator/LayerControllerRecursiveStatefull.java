package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LayerControllerRecursiveStatefull implements Runnable, LayerControllerDecorator {

    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends Stopable> nodes;


    final private LayerControllerDecorator next;
    private static final Logger logger = LoggerFactory.getLogger(LayerControllerRecursiveStatefull.class);
    private final String name;

    public LayerControllerRecursiveStatefull(String name, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends Stopable> nodes, LayerControllerDecorator next) {
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

        logger.info(name + " batch start awaits !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " batch start got througth awaits !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        boolean run = true;
        //while (!allProducersDone()) {


        //}
        while(run) {
            run = step();
        }
        nodes.forEach(Stopable::signalBatchEnd);
        logger.info(name + " batch end awaits !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        try {
            batchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        logger.info(name + " batch end got througth awaits !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        logger.info("done");
    }

}
