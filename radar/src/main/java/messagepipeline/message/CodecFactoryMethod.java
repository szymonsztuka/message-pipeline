package messagepipeline.message;

public interface CodecFactoryMethod {
    Decoder getMessageDecoder(String type);

    Encoder getMessageEncoder(String type);

    ScriptGenerator getScriptGenerator(String... args);
}
