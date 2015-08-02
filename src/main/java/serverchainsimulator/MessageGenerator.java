package serverchainsimulator;

import java.nio.ByteBuffer;

public interface MessageGenerator {
    public void write(final String input, final ByteBuffer output);
}
