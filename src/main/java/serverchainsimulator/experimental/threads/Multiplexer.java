package serverchainsimulator.experimental.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by simon on 17/10/15.
 */
public class Multiplexer implements Runnable{

    final private Producer producer;
    final private List<Consumer> consumers;

    final private CountDownLatch start;
    final CyclicBarrier turn;
    final private Object turnDone;


    final CyclicBarrier subtaskTurn;

    /** 1:n
     * Start latch controled outside
     * Self stopping multiplexer. */
    public Multiplexer(final Producer producer, final List<Consumer> consumers,
                       final CountDownLatch start, final CyclicBarrier startTurn, final Object myTurnDone){
        this.producer = producer;
        this.consumers = consumers;
        this.start = start;
        this.turn = startTurn;
        this.turnDone = myTurnDone;
        this.subtaskTurn = new CyclicBarrier(1 + consumers.size());
    }
    @Override
    public void run() {
        Thread producerThread = new Thread(producer);
        List<Thread> consumerThreads = new ArrayList<>(consumers.size());
        boolean interrupted = false;
        do {
            try {
                start.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                interrupted = true;
            }
        } while (interrupted);
        for (Consumer consumer: consumers) {
            consumerThreads.add(new Thread(consumer));
        }
        producerThread.start();
        for (Thread consumerThread: consumerThreads) {
            consumerThread.start();
        }
        int failedPolls = 0;
        int t = 0;
        while (failedPolls < 5) {
            t++;
            System.out.println("!!! Turn "+t+" starts !!!\n");
            Pack elem = null;
            try {
                elem = producer.getOutput().poll(50L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                if(elem == null) {
                    continue;
                }
            }
            if(elem == null) {
                failedPolls ++;
                continue;
            } else {
                failedPolls = 0;
            }
            for(Consumer consumer :consumers) {
                boolean added = false;
                while (!added) { //TODO failed additions
                    added = consumer.offer(elem, 50L, TimeUnit.MILLISECONDS);
                }
                System.out.print("Element hand over\n");
            }
            int noDoneConsumers;
            do {
                noDoneConsumers = 0;
                for (Consumer consumer : consumers) {
                    if (consumer.turnDone()) {
                        noDoneConsumers++;
                    }
                }
            } while (noDoneConsumers >= consumers.size()) ;
            synchronized(turnDone) {
                turnDone.notifyAll();
            }
            interrupted = false;
            do {
                try {
                    System.out.println("!!! Turn "+t+" await !!!\n");
                    turn.await();
                    interrupted = false;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            } while (interrupted);
        }
        for (Consumer consumer: consumers) {
            consumer.stop();
        }
        System.out.print("End\n");
    }
}
