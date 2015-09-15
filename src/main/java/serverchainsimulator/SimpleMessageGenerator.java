package serverchainsimulator;

import java.nio.ByteBuffer;

public class SimpleMessageGenerator implements MessageGenerator {
    public boolean write(final String input, final ByteBuffer output, boolean sendAtTimestamps) {
        byte[] body = input.getBytes();
        output.put(body);
        return true;
    }
}
