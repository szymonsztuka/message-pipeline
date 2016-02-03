package messagepipeline.content;

import java.nio.ByteBuffer;

//TODO class name should be message encoder
public interface MessageGenerator {
    boolean write(final String input, final ByteBuffer output, boolean sendAtTimestamps);
    default void resetSequencNumber() {
    }
}
