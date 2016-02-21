package messagepipeline.example;

import messagepipeline.MessagePipeline;
import messagepipeline.message.MessageGenerator;
import messagepipeline.message.MessageReceiver;
import messagepipeline.message.ShellScriptGenerator;

public class DummyMessagePipeline extends MessagePipeline {
    @Override
    protected MessageReceiver getMessageReceiver(String type) {
        return new DummyMessageReceiver();
    }

    @Override
    protected MessageGenerator getMessageGenerator(String type) {
        return new DummyMessageGenerator();
    }

    @Override
    protected ShellScriptGenerator getShellScriptGenerator(String... args) {
        return new DummyShellScriptGenerator();
    }

    public static void main(String[] args) {
        DummyMessagePipeline me = new DummyMessagePipeline();
        String[] files = new String[]{"dummyproperties.txt"};
        me.start2(files);
    }
}
