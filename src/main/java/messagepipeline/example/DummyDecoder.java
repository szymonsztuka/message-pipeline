package messagepipeline.example;

import messagepipeline.message.Decoder;

import java.nio.ByteBuffer;

public class DummyDecoder implements Decoder {
    @Override
    public String read(ByteBuffer input) {
        String result = new String();
        while(input.hasRemaining()){
            result += (char) input.get(); // read 1 byte at a time
        }
        return result;
    }
}
