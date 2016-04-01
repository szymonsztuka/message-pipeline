package messagepipeline.message;

import messagepipeline.message.Encoder;

import java.nio.ByteBuffer;

public class TxtEncoder implements Encoder {
    @Override
    public boolean write(String input, ByteBuffer output) {
        byte[] x = input.getBytes();
        output.put(x);
        return true;
    }
}
