package messagepipeline.message;

public class DummyCodecFactoryMethod implements CodecFactoryMethod {
    @Override
    public Decoder getMessageDecoder(String type) {
        return new TxtDecoder();
    }

    @Override
    public Encoder getMessageEncoder(String type) {
        return new TxtEncoder();
    }

    @Override
    public ScriptGenerator getScriptGenerator(String... args) {
        return new DummyScriptGenerator();
    }
}
