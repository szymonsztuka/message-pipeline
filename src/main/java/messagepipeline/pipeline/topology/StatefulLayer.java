package messagepipeline.pipeline.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import messagepipeline.pipeline.node.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class StatefulLayer implements Runnable, Layer {

    private static final Logger logger = LoggerFactory.getLogger(StatefulLayer.class);
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends Node> nodes;
    final private Layer next;
    private final String name;
    private final List<String> fileNames;

    public StatefulLayer(String name, List<String> fileNames, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends Node> nodes, Layer next) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.next = next;
        this.name = name;
        this.fileNames = fileNames;
    }

    public void start(){
        List<Thread> threads = new ArrayList<>(nodes.size());
        threads.add(new Thread((Runnable)nodes,nodes.getClass().getName()));
        threads.forEach(Thread::start);
        if(next!=null){
            next.start();
        }
    }

    public boolean step(String stepName){
        boolean result = false;
        if (next != null) {
            result = next.step(stepName);
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
        Iterator<String> nameIterator = fileNames.iterator();
        while(run && nameIterator.hasNext()) {
            run = step(nameIterator.next());
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
    }
}
