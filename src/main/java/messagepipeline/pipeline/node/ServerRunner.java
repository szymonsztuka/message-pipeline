package messagepipeline.pipeline.node;

import messagepipeline.message.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


public class ServerRunner extends UniversalNode implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ServerRunner.class);

    private Server server;

    public ServerRunner(String name, String directory, List<Encoder> encoders, InetSocketAddress address, CyclicBarrier start, CyclicBarrier end) {
        super(name, directory, start, end);
        this.server = new Server(encoders, address);
    }

    @Override
    public void signalStepEnd() {

    }

    public void run () {
        logger.debug("connecting " + process);
        server.connect();
        logger.debug("connected " + process);
        while(process) {
            try {logger.debug(name + " x  s " + batchStart.getParties() + " " + batchStart.getNumberWaiting());
                batchStart.await();
                logger.debug(name + " x s -passed" + batchStart.getParties() + " " + batchStart.getNumberWaiting());
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
            if(path == null) {
                //server.close();
                //server.connect();
            } else {
                if (Files.notExists(path.getParent())) {
                    try {
                        Files.createDirectories(path.getParent());
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error(e.getMessage(), e);
                    }
                }
                server.write(path);
            }
            try {logger.debug(name + " e " + batchStart.getParties() + " " + batchStart.getNumberWaiting());
                batchEnd.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
        }
        server.close();
    }

    public String toString(){
        return "ServerRunner";
    }
}
