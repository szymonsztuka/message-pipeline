package messagepipeline.experimental.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * Created by simon on 18/10/15.
 */
public class Sample {
        public static void main(String[] args) {
            Producer producer = new SampleProducer();
            SampleTransmitter transmitter = new SampleTransmitter();
            Consumer consumer = new SampleConsumer(transmitter);
            List<Consumer> consumers = new ArrayList<>(1);
            consumers.add(consumer);
            CountDownLatch start = new CountDownLatch(0);
            CyclicBarrier startTurn = new CyclicBarrier(1);
            Object myTurnDone = new Object();
            Multiplexer multiplexer = new Multiplexer( producer, consumers, start, startTurn, myTurnDone);
            Thread multiplexerThread = new Thread(multiplexer);
            multiplexerThread.start();
        }
}
