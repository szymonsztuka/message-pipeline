package radar.message;

public interface DecoderFactory {

    Decoder getMessageDecoder(String type);
}
