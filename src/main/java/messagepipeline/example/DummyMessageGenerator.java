package messagepipeline.example;

import messagepipeline.message.MessageGenerator;

import java.nio.ByteBuffer;

public class DummyMessageGenerator implements MessageGenerator {
    @Override
    public boolean write(String input, ByteBuffer output, boolean sendAtTimestamps) {
        byte[] x = input.getBytes();
        output.put(x);
        return true;
    }
}
