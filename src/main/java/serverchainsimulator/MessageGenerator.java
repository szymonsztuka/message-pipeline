package serverchainsimulator;

import java.nio.ByteBuffer;

public interface MessageGenerator {
    public boolean write(final String input, final ByteBuffer output, boolean sendAtTimestamps);
    default public void resetSequencNumber() {
    }
}
