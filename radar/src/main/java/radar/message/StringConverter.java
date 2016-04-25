package radar.message;

import java.nio.ByteBuffer;

public interface StringConverter {

    boolean convert(final String src, final ByteBuffer dst);
}
