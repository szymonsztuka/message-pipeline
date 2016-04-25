package radar.message;

import java.nio.ByteBuffer;

public class TxtByteConverter implements ByteConverter {
    @Override
    public String convert(ByteBuffer input) {
        String result = new String();
        while(input.hasRemaining()){
            result += (char) input.get(); // convert 1 byte at a time
        }
        return result;
    }
}
