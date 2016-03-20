package messagepipeline.pipeline.node;

import messagepipeline.message.MessageGenerator;
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

    public ServerRunner(String name, String directory, List<MessageGenerator> messageGenerators, InetSocketAddress address, CyclicBarrier start, CyclicBarrier end) {
        super(name, directory, address, start, end);
        this.server = new Server(messageGenerators, address ,false);
    }

    @Override
    public void signalStepEnd() {

    }

    public void run () {
        server.connect();
        while(process) {
            try {logger.debug(name + " s " + batchStart.getParties() + " " + batchStart.getNumberWaiting());
                batchStart.await();
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
}
