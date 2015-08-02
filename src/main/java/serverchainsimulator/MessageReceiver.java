package serverchainsimulator;

import java.nio.ByteBuffer;

public interface MessageReceiver {
    public String read(final ByteBuffer input);
}
