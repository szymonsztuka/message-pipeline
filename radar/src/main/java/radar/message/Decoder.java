package radar.message;

import java.nio.ByteBuffer;

public interface Decoder {

    String read(final ByteBuffer input);
}
