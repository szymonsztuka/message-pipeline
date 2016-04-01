package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class Sequence implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Sequence.class);

    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    private final List<Runner> runners;
    private final List<Sequence> next;
    private final String name;
    private final List<String> fileNames;
    private List<Thread> threads;

    public Sequence(String name, List<String> names, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<Runner> nodes, List<Sequence> next) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.runners = nodes;
        this.next = next;
        this.name = name;
        this.fileNames = names;
    }

    public void startRunners() {
        threads = new ArrayList<>(runners.size());
        runners.stream().forEach(e -> threads.add(new Thread(e, e.getName())));
        threads.forEach(Thread::start);
        if (next != null) {
            next.stream().forEach(Sequence::startRunners);
        }
    }

    public void step(String stepName, boolean last) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        runners.forEach(e -> e.addStep(stepName));
        try {
            //logger.debug(name + " " + stepName + " start " + batchStart.getParties() + " " + batchStart.getNumberWaiting() );
            batchStart.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        if (next != null) {
            next.stream().forEach(e -> e.step(stepName, last));
        }
        runners.forEach(Runner::signalStepEnd);
        if (last) {
            runners.forEach(Runner::finish);
        }
        try {
            //logger.debug(name + " " + stepName + " end " + batchEnd.getParties() + " " + batchEnd.getNumberWaiting());
            batchEnd.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        startRunners();
        logger.debug("nb dirWithSteps to process:" + fileNames.size());
        Iterator<String> it = fileNames.iterator();
        while (it.hasNext()) {
            String elem = it.next();
            step(elem, !it.hasNext());
        }
        try {
            for (Thread th : threads)
                th.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("End");
    }

    public String getName() {
        return name;
    }

    public String toString() {
        String res = "{" + runners.toString() + "}";
        if (next != null)
            for (Sequence e : next) {
                res = res + e.toString();
            }
        return res;
    }
}
