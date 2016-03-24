package messagepipeline.pipeline.node;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


public class UniversalLocalScript extends UniversalNode implements Runnable{

    String script;
    private final CyclicBarrier batchStart;
    private final CyclicBarrier batchEnd;
    public UniversalLocalScript(String script, CyclicBarrier batchStart, CyclicBarrier batchEnd){
        super(script,".",batchStart, batchEnd);
        this.script = script;
        this.batchStart =batchStart;
        this.batchEnd =batchEnd;

    }
    public void run(){
        try {
            batchStart.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        try {
            if(script!=null) {
                String info = "Running " + script;
                final Process process = Runtime.getRuntime().exec(script);
                // exhaust input stream  http://dhruba.name/2012/10/16/java-pitfall-how-to-prevent-runtime-getruntime-exec-from-hanging/
                final BufferedInputStream in = new BufferedInputStream(process.getInputStream());
                final byte[] bytes = new byte[4096];
                while (in.read(bytes) != -1) {
                }// wait for completion
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for(int i=0; i < info.length(); i++) {
                    System.out.print("\b");
                }
                System.out.println("Done    " + script);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            batchEnd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void signalStepEnd() {

    }

    public String toString(){
        return "UniversalRemoteShellScrip";
    }
}
