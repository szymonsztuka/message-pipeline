package messagepipeline.content;

import java.nio.ByteBuffer;

//TODO class name should be message decoder
public interface MessageReceiver {
    String read(final ByteBuffer input);
}
