package serverchainsimulator;

import java.nio.ByteBuffer;

public class SimpleMessageGenerator implements MessageGenerator {
    public void write(final String input, final ByteBuffer output) {
        byte[] body = input.getBytes();
        output.put(body);
    }
}
