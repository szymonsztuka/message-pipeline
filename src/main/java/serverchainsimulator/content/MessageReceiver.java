package serverchainsimulator.content;

import java.nio.ByteBuffer;

public interface MessageReceiver {
    String read(final ByteBuffer input);
}
