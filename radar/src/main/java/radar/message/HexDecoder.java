package radar.message;

import java.nio.ByteBuffer;

public class HexDecoder implements Decoder {
    @Override
    public String read(ByteBuffer input) {
        //http://stackoverflow.com/questions/8890174/in-java-how-do-i-convert-a-hex-string-to-a-byte
        StringBuilder dest = new StringBuilder(input.limit() * 2);
        for (int i = 0; i < input.limit(); i++) {
            int x = (int) input.get();
            if ((x & 0xff) < 0x10) {
                dest.append("0");
            }
            dest.append(Long.toString(x & 0xff, 16));
        }
        return dest.toString();
    }
}
