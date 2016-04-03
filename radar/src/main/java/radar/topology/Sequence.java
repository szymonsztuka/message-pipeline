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
    private final List<String> steps;
    private final List<Thread> threads;
    public final int millisecondsDelay;

    public Sequence(List<String> steps, CyclicBarrier batchStart, CyclicBarrier batchEnd, List<Runner> nodes, List<Sequence> next, int millisecondsDelay) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.runners = nodes;
        this.threads = new ArrayList<>(this.runners.size());
        this.runners.stream().forEach(e -> this.threads.add(new Thread(e, e.getName())));
        this.next = next;
        this.steps = steps;
        this.millisecondsDelay = millisecondsDelay;
    }

    public void startRunners() {
        threads.forEach(Thread::start);
        if (next != null) {
            next.stream().forEach(Sequence::startRunners);
        }
    }

    public void step(String stepName, boolean last) {
        try {
            Thread.sleep(millisecondsDelay);
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
        if (steps.size() == 0) {
            logger.debug("once off run (number of steps to process:" + steps.size());
            step("", false);
        } else {
            logger.debug("number of steps to process:" + steps.size());
            Iterator<String> it = steps.iterator();
            while (it.hasNext()) {
                String elem = it.next();
                step(elem, !it.hasNext());
            }
        }
        try {
            for (Thread th : threads)
                th.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("end");
    }

    public String toString() {
        String res = "{" + runners.toString() + "}";
        if (next != null)
            for (Sequence e: next) {
                res = res + e.toString();
            }
        return res;
    }
}
