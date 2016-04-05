package radar.message;

import java.nio.ByteBuffer;

public interface Encoder<T> {

    boolean write(final T src, final ByteBuffer dst);
}
