package serverchainsimulator.experimental.threads;

import java.util.concurrent.BlockingQueue;

/**
 * Created by simon on 17/10/15.
 */
public interface Producer extends Runnable {
    public BlockingQueue<Pack> getOutput();
}
