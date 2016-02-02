package serverchainsimulator.experimental.threads;

import java.util.concurrent.TimeUnit;

/**
 * Created by simon on 17/10/15.
 */
public interface Consumer extends Runnable{
    public boolean turnDone();
    public boolean offer(Pack elem, long timeout, TimeUnit unit );
    public void stop();
}
