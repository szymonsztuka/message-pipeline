package radar.message;

import java.nio.ByteBuffer;

public interface ByteConverter {

    String convert(final ByteBuffer input);
}
