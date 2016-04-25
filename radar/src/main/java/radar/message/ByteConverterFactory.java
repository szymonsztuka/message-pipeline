package radar.message;

public interface ByteConverterFactory {

    ByteConverter getByteConverter(String type);
}
