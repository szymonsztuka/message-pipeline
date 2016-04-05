package radar.message;

import java.nio.ByteBuffer;

public interface Encoder {

    boolean write(final String src, final ByteBuffer dst);
}
