package radar;

import radar.message.DummyScriptFactory;
import radar.message.FileReaderFactory;
import radar.message.TxtByteConverterFactory;
import radar.message.TxtStringConverterFactory;

public class TxtBootstrap {

    public static void main(String[] args) {
        String[] configurationFiles = new String[]{"../../radar-samples/build/resources/main/sample.properties","../../radar-samples/build/resources/main/my-env.properties"};
        Radar me = new Radar(
                new FileReaderFactory(),
                new TxtStringConverterFactory(),
                new TxtByteConverterFactory(),
                new DummyScriptFactory(),
                configurationFiles);
         me.run();
    }
}
