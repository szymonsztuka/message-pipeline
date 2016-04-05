package radar.message;

public interface EncoderFactory<T> {

    Encoder<T> getMessageEncoder(String type);
}
