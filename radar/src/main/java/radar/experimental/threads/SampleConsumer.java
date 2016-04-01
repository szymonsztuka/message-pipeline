package radar.experimental.threads;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by simon on 17/10/15.
 */
public class SampleConsumer implements Consumer, Runnable{

    private final BlockingQueue<Pack> input = new LinkedBlockingDeque<Pack>();
    private final SampleTransmitter sender;
    private volatile boolean run = true;
    private Object turnDoneLock = new Object();
    private boolean turnDone = false;
    public SampleConsumer(SampleTransmitter mediumTransmitter) {
        this.sender = mediumTransmitter;
    }

    @Override
    public boolean offer(Pack elem, long timeout, TimeUnit unit ) {
        boolean interrupted;
        boolean added = false;
        do {
            interrupted = false;
            try {
                synchronized (turnDoneLock) {
                    added = input.offer(elem, timeout, unit);
                    turnDone = false;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                interrupted = true;
            }
        } while(interrupted);
        return added;
    }

    public boolean turnDone(){
        return turnDone;
    }

    public void stop(){
        run = false;
    }

    @Override
    public void run() {
        while(run) {
            Pack elem = null;
            try {
                elem = input.poll(50L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                if (elem == null) {
                    continue;
                }
            }
            if(elem!=null) {
                sender.transmit(elem);//blocking
                turnDone = Boolean.TRUE;
            }
        }
        System.out.print("Consumer end\n");
    }
}
