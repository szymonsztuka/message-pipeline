package radar;

import radar.message.DummyScriptFactory;
import radar.message.TxtDecoderFactory;
import radar.message.TxtEncoderFactory;

public class TxtBootstrap {

    public static void main(String[] args) {
        Bootstrap me = new Bootstrap(new TxtEncoderFactory(), new TxtDecoderFactory(), new DummyScriptFactory());
        String[] files = new String[]{"../../radar-samples/build/resources/main/sample.properties","../../radar-samples/build/resources/main/my-env.properties"};
        me.run(files);
    }
}
