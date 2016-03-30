package messagepipeline.example;

import messagepipeline.message.Encoder;
import messagepipeline.message.Decoder;
import messagepipeline.message.ScriptGenerator;
import messagepipeline.message.CodecFactoryMethod;

public class DummyCodecFactoryMethod implements CodecFactoryMethod {
    @Override
    public Decoder getMessageDecoder(String type) {
        return new DummyDecoder();
    }

    @Override
    public Encoder getMessageEncoder(String type) {
        return new DummyEncoder();
    }

    @Override
    public ScriptGenerator getScriptGenerator(String... args) {
        return new DummyScriptGenerator();
    }
}
