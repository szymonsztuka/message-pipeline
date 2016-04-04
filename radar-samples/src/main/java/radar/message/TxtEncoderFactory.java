package radar.message;

public class TxtEncoderFactory implements EncoderFactory {

    @Override
    public Encoder getMessageEncoder(String type) {
        return new TxtEncoder();
    }
}
