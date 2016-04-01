package radar.message;

import java.nio.ByteBuffer;

public class TxtDecoder implements Decoder {
    @Override
    public String read(ByteBuffer input) {
        String result = new String();
        while(input.hasRemaining()){
            result += (char) input.get(); // read 1 byte at a time
        }
        return result;
    }
}