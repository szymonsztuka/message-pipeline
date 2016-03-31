package messagepipeline.example;

import messagepipeline.Bootstarp;

public class TxtBootstrap {

    public static void main(String[] args) {

        Bootstarp me = new Bootstarp(new DummyCodecFactoryMethod());
        String[] files = new String[]{"resources/main/example/properties.txt resources/main/example/env-properties.txt"};
        me.run(files);
    }
}
