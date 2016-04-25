package radar.message;

import java.nio.ByteBuffer;

public class TxtEncoder implements StringConverter {
    @Override
    public boolean convert(String input, ByteBuffer output) {
        byte[] x = input.getBytes();
        output.put(x);
        return true;
    }
}
