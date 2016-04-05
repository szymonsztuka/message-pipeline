package radar.message;

public interface EncoderFactory {

    Encoder getMessageEncoder(String type);
}
