package radar.message;

public class TxtByteConverterFactory implements ByteConverterFactory {
    @Override
    public ByteConverter getByteConverter(String type) {
        return new TxtByteConverter();
    }
}
