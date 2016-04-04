package radar.message;

public class TxtDecoderFactory implements DecoderFactory {
    @Override
    public Decoder getMessageDecoder(String type) {
        return new TxtDecoder();
    }
}
