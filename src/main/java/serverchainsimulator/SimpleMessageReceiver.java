package serverchainsimulator;

import java.nio.ByteBuffer;

public class SimpleMessageReceiver implements MessageReceiver {

    public String read(final ByteBuffer input) {
        byte[] bytes = new byte[input.limit()];
        input.get(bytes);
        return new String(bytes);
    }
}
