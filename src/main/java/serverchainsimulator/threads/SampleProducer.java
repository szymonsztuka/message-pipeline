package serverchainsimulator.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by simon on 18/10/15.
 */
public class SampleProducer implements Producer {
    BlockingQueue<Pack> output = new ArrayBlockingQueue<>(2);

    @Override
    public BlockingQueue<Pack> getOutput() {
        return output;
    }

    @Override
    public void run() {
        for(int i =0 ; i < 10; i++){
            SamplePack elem = new SamplePack();
            List<String> payload = new ArrayList<>();
            for(int j=0; j < i; j++) {
                payload.add("Pack " + i + j);
                //System.out.print(payload.get(payload.size()-1)+"\n");
            }
            elem.pyaload = payload;
            try {
                output.put(elem);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Producer End\n");
    }
}
