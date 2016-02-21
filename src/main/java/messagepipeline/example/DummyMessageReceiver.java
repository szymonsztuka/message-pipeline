package messagepipeline.example;

import messagepipeline.message.MessageReceiver;

import java.nio.ByteBuffer;

public class DummyMessageReceiver implements MessageReceiver {
    @Override
    public String read(ByteBuffer input) {
        return "";
    }
}
