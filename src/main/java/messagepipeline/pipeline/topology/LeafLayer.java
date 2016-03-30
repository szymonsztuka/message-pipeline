package messagepipeline.pipeline.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.pipeline.node.LeafNode;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class LeafLayer implements Layer, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LeafLayer.class);
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends LeafNode> nodes;
    private final String name;
    private final List<String> fileNames;

    public LeafLayer(String name, List<String> names, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends LeafNode> nodes) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.name = name;
        this.fileNames = names;
    }

    public void nodesStart(){
        logger.trace("startRunners " + name + ", " +nodes.size() + " nodes");
        List<Thread> threads = new ArrayList<>(nodes.size());
        for( LeafNode n : nodes){
            threads.add(new Thread((Runnable)n,n.getName()));
        }
        threads.forEach(Thread::start);
    }

    public boolean step(String stepName){
        logger.trace(name + " awaiting " + stepName + " ..." +batchStart.getParties()+" "+batchStart.getNumberWaiting());
        try {
            batchStart.await(); logger.info(name + "s " + stepName + " e "+ batchStart.getParties() + " "+ batchStart.getNumberWaiting());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        try {

            batchEnd.await();logger.info(name + " e " + stepName + " e "+ batchEnd.getParties() + " "+ batchEnd.getNumberWaiting());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        boolean result = !allProducersDone();
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

    @Override
    public void run() {
        nodesStart();
        //logger.trace("run");
        boolean run = true;
        long i = 0;
        Iterator<String> nameIterator = fileNames.iterator();
        while(run) {  //logger.info("xxxxxxxxxxxxxxxxxxxxxxxxx");
            String fileName = nameIterator.next();
            run = step(fileName);
            //System.out.print(String.format("Running [%2d %%] %30s %20s\r", ((i * 100) / fileNames.size()), fileName, "                  "));
            i++;
        }
        //System.out.print("whose done");
        //logger.info("whose done");
    }
    public String getName(){
        return name;
    }
}
