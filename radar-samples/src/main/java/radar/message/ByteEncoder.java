package radar.message;

import java.nio.ByteBuffer;

public class ByteEncoder implements StringConverter {

    private byte[] buffer = new byte[1000];


    public boolean convert(ByteBuffer src, ByteBuffer dest) {
        src.get(buffer, 0, src.limit());
        dest.put(buffer);
        return src.hasRemaining();
    }

    @Override
    public boolean convert(String src, ByteBuffer dst) {
        return false;
    }
}
