package messagepipeline.message;

import java.nio.ByteBuffer;

//TODO class name should be simple message encoder
public class SimpleMessageGenerator implements MessageGenerator {
    public boolean write(final String input, final ByteBuffer output, boolean sendAtTimestamps) {
        byte[] body = input.getBytes();
        output.put(body);
        return true;
    }
}