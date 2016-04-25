package radar.message;

import java.nio.ByteBuffer;

public class HexEncoder implements StringConverter {
    @Override
    public boolean convert(String src, ByteBuffer dst) {
        //http://stackoverflow.com/questions/8890174/in-java-how-do-i-convert-a-hex-string-to-a-byte
        int len = src.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(src.charAt(i), 16) << 4)
                    + Character.digit(src.charAt(i+1), 16));
        }
        dst.put(data);
        return true;
    }
}
