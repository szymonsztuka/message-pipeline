package radar.message;

import java.nio.ByteBuffer;

public class ByteEncoder implements Encoder<ByteBuffer> {

    private byte[] buffer = new byte[1000];

    @Override
    public boolean write(ByteBuffer src, ByteBuffer dest) {
        src.get(buffer, 0, src.limit());
        dest.put(buffer);
        return src.hasRemaining();
    }
}
