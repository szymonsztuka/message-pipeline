package messagepipeline.pipeline.node;

import messagepipeline.message.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


public class ClientRunner extends UniversalNode implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(ClientRunner.class);

    private Client client;

    public ClientRunner(String name, String directory, Decoder decoder, InetSocketAddress address, CyclicBarrier start, CyclicBarrier end) {
        super(name, directory, start, end);
        this.client = new Client(address, decoder);
    }

    @Override
    public void signalStepEnd() {
        client.finishStep();
    }

    public void run () {
        client.connect();
        while (process) {
            try {logger.debug(name + " s " + batchStart.getParties() + " " + batchStart.getNumberWaiting());
                batchStart.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
           // if (path == null) {
                //client.close();
                //client.connect();
            //} else {
                if (Files.notExists(path.getParent())) {
                    try {
                        Files.createDirectories(path.getParent());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                client.read(path);
           // }
            try {logger.debug(name + " e " + batchStart.getParties() + " " + batchStart.getNumberWaiting());
                batchEnd.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
        client.close();
    }

    public String toString(){
        return "ClientRunner";
    }
}
