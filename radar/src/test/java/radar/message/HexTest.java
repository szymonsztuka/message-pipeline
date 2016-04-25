package radar.message;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HexTest {
    public static void main(String[] str) {

        ByteBuffer buffer = ByteBuffer.allocateDirect(4048);
        HexEncoder encoder = new HexEncoder();
        String hexInput = "416c61206d61206b6f74612e";
        byte[] byteExpectedOutput = new byte[]{65, 108, 97, 32, 109, 97, 32, 107, 111, 116, 97, 46};
        encoder.convert(hexInput, buffer);
        buffer.flip();
        byte[] byteOutput = new byte[buffer.limit()];
        buffer.get(byteOutput);
        boolean match = true;
        if (byteExpectedOutput.length != byteOutput.length) {
            match = false;
        } else {
            for (int i = 0; i < byteOutput.length; i++) {
                if (byteExpectedOutput[i] != byteOutput[i]) {
                    match = false;
                }
            }
        }
        System.out.println(Arrays.toString(byteOutput));
        System.out.println(match);

        /*HexByteConverter decoder = new HexByteConverter();
        String hexOutput = decoder.convert(buffer);
        buffer.rewind();
        System.out.println(hexOutput);
        System.out.println(hexInput.equals(hexOutput));
        */
    }

}
