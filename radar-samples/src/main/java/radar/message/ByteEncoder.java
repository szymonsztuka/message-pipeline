package radar.message;

import java.nio.ByteBuffer;

public class ByteEncoder implements Encoder {

    private byte[] buffer = new byte[1000];


    public boolean write(ByteBuffer src, ByteBuffer dest) {
        src.get(buffer, 0, src.limit());
        dest.put(buffer);
        return src.hasRemaining();
    }

    @Override
    public boolean write(String src, ByteBuffer dst) {
        return false;
    }
}
