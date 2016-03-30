package messagepipeline.pipeline.topology;

import messagepipeline.pipeline.node.UniversalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class UniversalLayer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(UniversalLayer.class);
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<? extends UniversalNode> nodes;
    final private List<UniversalLayer> next;
    private final String name;
    private final List<String> fileNames;
    private List<Thread> threads;

    public UniversalLayer(String name, List<String> names, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<? extends UniversalNode> nodes, List<UniversalLayer> next) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.nodes = nodes;
        this.next = next;
        this.name = name;
        this.fileNames = names;
    }

    public void nodesStart(){
        threads = new ArrayList<>(nodes.size());
        nodes.stream().forEach(e -> threads.add(new Thread((Runnable)e, e.getName())));
        threads.forEach(Thread::start);
        if (next != null){
            next.stream().forEach(UniversalLayer::nodesStart);
        }
    }

    public void step(String stepName, boolean last){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        nodes.forEach(e -> e.addStep(stepName));
        try { logger.debug(name + " " + stepName + " start "+ batchStart.getParties() + " "+ batchStart.getNumberWaiting() + " for " +nodes.toString());
            batchStart.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        if (next != null) {
            next.stream().forEach(e -> e.step(stepName, last));
        }
        nodes.forEach(UniversalNode::signalStepEnd);
        if(last) {
            nodes.forEach(UniversalNode::finish);
        } try { logger.debug(name + " " + stepName + " end "+ batchEnd.getParties() + " "+ batchEnd.getNumberWaiting());
            batchEnd.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        nodesStart();
        logger.debug("nb dirWithSteps to run:" + fileNames.size());
        Iterator<String> it = fileNames.iterator();
        while(it.hasNext()) {
            String elem = it.next();
            step(elem, !it.hasNext());
        }
        try {
            for(Thread th: threads)
                th.join();
        } catch(InterruptedException e){
            e.printStackTrace();
        }
        logger.info("done");
    }

    public String getName(){
        return name;
    }

    public String toString() {
        String res = "{"+nodes.toString()+"}";
        if(next!=null)
        for(UniversalLayer e : next ) {
            res = res + e.toString();
        }
        return res;
    }
}
