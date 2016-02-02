package serverchainsimulator.content;

import java.nio.ByteBuffer;

public interface MessageGenerator {
    boolean write(final String input, final ByteBuffer output, boolean sendAtTimestamps);
    default void resetSequencNumber() {
    }
}
