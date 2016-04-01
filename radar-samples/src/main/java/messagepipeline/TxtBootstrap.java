package messagepipeline;

import messagepipeline.message.DummyCodecFactoryMethod;

public class TxtBootstrap {

    public static void main(String[] args) {
        Bootstrap me = new Bootstrap(new DummyCodecFactoryMethod());
        String[] files = new String[]{"../../radar-samples/build/resources/main/sample.properties","../../radar-samples/build/resources/main/my-env.properties"};
        me.run(files);
    }
}
