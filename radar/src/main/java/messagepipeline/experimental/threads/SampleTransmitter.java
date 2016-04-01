package messagepipeline.experimental.threads;

/**
 * Created by simon on 18/10/15.
 */
public class SampleTransmitter {
    public boolean transmit(Pack elem) {
        if(elem instanceof SamplePack) {
            for(String line: ((SamplePack)elem).pyaload){
                //System.out.print("Out transmitter:"+line+"\n");
            }
        }
        return true;
    }
}
